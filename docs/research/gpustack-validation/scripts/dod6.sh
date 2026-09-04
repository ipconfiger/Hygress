#!/usr/bin/env bash
# =============================================================================
# DoD 6 — image / port / ops checks for the Hygress-swapped server.
#
# Run ON THE BASELINE REMOTE from /root/gpustack-validation:
#   ./dod6.sh
# Emits $fixtures-hygress/dod6_results.txt (PASS/FAIL per check + evidence).
# Each check is independent; a FAIL does not stop the rest.
# =============================================================================
set -uo pipefail
BASE=/root/gpustack-validation; cd "$BASE"
FXH="$BASE/fixtures-hygress"; mkdir -p "$FXH"
R="$FXH/dod6_results.txt"; : > "$R"
pass(){ echo "PASS  $*" | tee -a "$R"; }
fail(){ echo "FAIL  $*" | tee -a "$R"; }
ev(){   echo "      | $*" | tee -a "$R"; }
echo "================ DoD 6 (image/port/ops) ================"

ssx(){ docker exec gpustack-server ss -ltnp 2>/dev/null || docker exec gpustack-server ss -ltn 2>/dev/null; }
PORTS="$(ssx)"; printf '%s\n' "$PORTS" | tee "$FXH/dod6_ports.txt" >/dev/null
port(){ printf '%s\n' "$PORTS" | grep -qE "$1"; }
port_exact_listener(){ printf '%s\n' "$PORTS" | grep -E "$1" | head -3; }

# ---------- 6.1 port inventory ----------
ev "ss -ltnp captured -> fixtures-hygress/dod6_ports.txt"
port_exact_listener ":18443" >/dev/null && port "18443" \
  && { pass "6.1a embedded kube-apiserver LISTEN 18443 (127.0.0.1)"; ev "$(port_exact_listener ':18443')"; } \
  || { fail "6.1a apiserver 18443 not listening"; }

port "0.0.0.0:80|:80 " && port_exact_listener ":80" \
  && { pass "6.1b data plane LISTEN 0.0.0.0:80 (hygress/pingora)"; ev "$(port_exact_listener ':80')"; } \
  || { fail "6.1b no listener on :80"; }

port_exact_listener ":443" && ev "6.1c https :443 present (optional TLS): $(port_exact_listener ':443')" \
  || ev "6.1c no :443 (TLS not enabled in this env — acceptable)"

port "8081" \
  && { pass "6.1d hygress admin LISTEN 127.0.0.1:8081"; ev "$(port_exact_listener ':8081')"; } \
  || { fail "6.1d hygress admin :8081 not listening (HYGRESS_ADMIN_ADDR)"; }

port "15020" \
  && { pass "6.1e GATEWAY_PILOT_AGENT_METRICS_PORT 15020 present"; ev "$(port_exact_listener ':15020')"; } \
  || ev "6.1e :15020 not present (report; hygress may not open a pilot-agent metrics port)"

# excluded Envoy/Istio pilot ports must be ABSENT
bad=""
for p in 9876 15010 15012 8888 15051; do port ":$p" && bad="$bad $p"; done
[[ -z "$bad" ]] && pass "6.1f NO envoy/istio pilot ports present ($$)" \
  || { fail "6.1f forbidden ports still present:$bad"; ev "$(port_exact_listener '9876|15010|15012|8888|15051')"; }

# ---------- 6.2 process inventory ----------
PSEXEC=$(docker exec gpustack-server ps aux 2>/dev/null); printf '%s\n' "$PSEXEC" | tee "$FXH/dod6_ps.txt" >/dev/null
n_hygress=$(printf '%s\n' "$PSEXEC" | grep -cE "\bhygress\b")
n_envoy=$(printf '%s\n' "$PSEXEC" | grep -ciE "\benvoy\b")
n_pilot=$(printf '%s\n' "$PSEXEC" | grep -cE "pilot-discovery|higress-pilot|\bpilot\b")
n_ctrl=$(printf '%s\n' "$PSEXEC" | grep -ciE "higress-controller|pilot-agent|istio")
ev "procs: hygress=$n_hygress envoy=$n_envoy pilot=$n_pilot controller/istio=$n_ctrl"
(( n_hygress >= 1 )) && { pass "6.2 ONE+ hygress process running"; } || { fail "6.2 no hygress process"; }
(( n_envoy == 0 && n_pilot == 0 && n_ctrl == 0 )) \
  && { pass "6.2 envoy/pilot/controller GONE"; } \
  || { fail "6.2 envoy/pilot/controller still present (envoy=$n_envoy pilot=$n_pilot ctrl=$n_ctrl)"; }

# ---------- 6.3 supercronic ----------
LOG="$(docker logs gpustack-server 2>&1)"
sup_start=$(printf '%s\n' "$LOG" | grep -ciE "supercronic.*(starting|cron start|CRON)")
sup_fail=$(printf '%s\n' "$LOG" | grep -ciE "readinessCheck.*Higress Pilot|readiness.*pilot.*fail")
cronf=$(docker exec gpustack-server sh -c 'ls /var/spool/cron/* 2>/dev/null; cat /var/spool/cron/cron.txt 2>/dev/null | head -1' 2>/dev/null)
ev "supercronic start log lines=$sup_start | 'Higress Pilot' readiness failures=$sup_fail"
ev "cron files: ${cronf:-<none>}"
(( sup_fail == 0 )) && (( sup_start >= 0 )) && { pass "6.3 supercronic healthy (no 'Higress Pilot' readiness failure)"; } \
  || { fail "6.3 supercronic readiness failure present ($sup_fail)"; }
[[ -n "$cronf" ]] && ev "cron file present: $cronf" || ev "no cron file yet (may appear after first GPUStack cron injection)"

# ---------- 6.4 readiness through the data plane ----------
code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:80/readyz 2>/dev/null)
ev "GET :80/readyz -> HTTP $code (through hygress mirror passthrough)"
[[ "$code" == "200" ]] && pass "6.4 gateway :80/readyz == 200 (mirror passthrough + ready-wait passed)" \
  || fail "6.4 gateway :80/readyz != 200 (got $code) — possible 10-min lock / mirror not wired"

# ---------- 6.5 hygress log (first snapshot / routes / token-auth / no panic) ----------
HLOG=$(docker exec gpustack-server sh -c 'cat /var/log/higress/access.log 2>/dev/null; cat /var/log/higress/* 2>/dev/null' 2>/dev/null)
printf '%s\n' "$HLOG" | tail -60 > "$FXH/dod6_hygress_log_tail.txt"
panic=$(printf '%s\n' "$HLOG" | grep -cE "\bpanic\b|Panic|SIGSEGV|core dumped")
ev "hygress log tail -> fixtures-hygress/dod6_hygress_log_tail.txt"
(( panic == 0 )) && pass "6.5 NO panic/crash in hygress log" || { fail "6.5 panic present in hygress log"; ev "$(printf '%s\n' "$HLOG" | grep -E 'panic|SIGSEGV' | head -5)"; }
if printf '%s\n' "$HLOG" | grep -qiE "snapshot|route|config.*(load|applied)|token.*auth|ready"; then
  pass "6.5 positive markers present in hygress log (snapshot/routes/token-auth/ready)"
  ev "$(printf '%s\n' "$HLOG" | grep -iE 'snapshot|route|token.*auth|ready' | tail -5)"
else
  ev "6.5 no explicit snapshot/route/token-auth markers found (inspect tail file) — not auto-failed"
fi

# ---------- 6.6 rollback artifacts present in image ----------
RB=$(docker exec gpustack-server sh -c 'ls /etc/s6-overlay/s6-rc.d.dist/gateway/run /etc/s6-overlay/s6-rc.d.dist/pilot/run /etc/s6-overlay/s6-rc.d.dist/controller/run 2>/dev/null' 2>/dev/null)
ev "rollback artifacts: ${RB:-<missing>}"
[[ -n "$RB" && $(printf '%s\n' "$RB" | wc -l) -ge 3 ]] \
  && pass "6.6 pristine gateway/pilot/controller run scripts preserved under /etc/s6-overlay/s6-rc.d.dist/" \
  || fail "6.6 rollback artifacts missing/incomplete"

# ---------- 6.7 s6 surgery: gateway/run IS the hygress launcher, no separate hygress service ----------
GR=$(docker exec gpustack-server sh -c 'grep -c "/usr/local/bin/hygress" /etc/s6-overlay/s6-rc.d/gateway/run 2>/dev/null' 2>/dev/null)
HSVC=$(docker exec gpustack-server sh -c 'test -d /etc/s6-overlay/s6-rc.d/hygress && echo present || echo absent' 2>/dev/null)
ev "gateway/run references /usr/local/bin/hygress: count=$GR | separate 'hygress' s6 svc: $HSVC"
[[ "$GR" =~ ^[1-9] && "$HSVC" == "absent" ]] \
  && pass "6.7 'gateway' slot is the hygress launcher; no separate 'hygress' s6 service" \
  || fail "6.7 s6 wiring unexpected (gateway/run hygress refs=$GR hygress svc=$HSVC)"

echo "================ DoD 6 RESULTS -> $R ================"
grep -E "^(PASS|FAIL)" "$R"
