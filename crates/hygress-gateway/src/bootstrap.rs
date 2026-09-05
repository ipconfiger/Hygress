//! Process bootstrap: configuration wiring + readiness + server launch (design §11).
//!
//! ## Always-available (pure / CPU-only) wiring
//!
//! [`DataState`] builds the control-plane snapshot holder ([`SharedConfig`]), the central
//! [`Metrics`], and the TLS SNI [`SniStore`]. The real (non-egress)
//! [`AdminState`] / [`StatsState`] for the admin and 15020 listeners are built from it. This is
//! all pure and is assembled under the default feature so the crate's unit tests cover the real
//! wiring. [`readiness_wait`] (the `GPUSTACK_API_PORT` TCP probe) and [`build_server`] (the admin
//! + 15020 `ServeHttp` listeners) are also always available and unit-tested here.
//!
//! ## `integrations`-gated data plane (design §11.2 — this replaces the old stub)
//!
//! Under `integrations` ([`run`]) performs the **real container launch sequence**:
//! 1. `GPUSTACK_API_PORT` **readiness probe** (500 ms poll, ~30 s bounded) — fail-fast on timeout.
//! 2. `jwt_secret_key` resolution (env → `{data_dir}/jwt_secret_key` → **fail-fast**), derive the
//!    gateway token (design §9), build the [`forward_auth::Client`] + [`GpustackSink`] (loopback).
//! 3. Control-plane [`hygress_adapter::Controller`]: build, optional topology-B IngressClass
//!    seed, spawn the poll loop, and **await `ready()`** (first snapshot — bind-ready).
//! 4. Attach the terminate-mode [`HygressProxy`] data plane (+ TLS when the snapshot has material),
//!    **only after** `ready()`. admin/stats listeners ride the same Pingora [`Server`].
//!
//! The Pingora [`Server`] is returned by [`run`] and driven by [`main`] on the main thread
//! (`run_forever()`, which installs the SIGTERM/SIGINT handler and exits 0 on a graceful stop).
//! The read-only `Controller` keeps polling on the process runtime until the process stops.
//!
//! ## Port discipline (design §11)
//!
//! Data plane `GATEWAY_HTTP_PORT`/`GATEWAY_TLS_PORT`; admin `HYGRESS_ADMIN_ADDR` (8081);
//! stats `GATEWAY_PILOT_AGENT_METRICS_PORT` (15020). The data plane binds **only after** the
//! adapter `ready()` (bind-ready) and the `GPUSTACK_API_PORT` readiness probe.
//! NEVER 9876 / 15010 / 15012 / 8888 / 15051.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hygress_core::prelude::{ConfigData, SharedConfig};
use pingora_core::server::configuration::{Opt, ServerConf};
use pingora_core::server::Server;
use pingora_core::services::listening::Service;
use pingora_core::Result as PingoraResult;
use tracing::{debug, error, info, warn};

use crate::admin::{AdminService, AdminState};
use crate::config::GatewayConfig;
use crate::context::SharedConfigHandle;
use crate::error::GatewayError;
use crate::metrics::Metrics;
use crate::stats::{StatsService, StatsState};
use crate::tls_store::SniStore;

/// The always-available runtime state (control-plane holder + metrics + TLS +
/// policy). The egress/adapter-wired [`crate::context::GatewayState`] is layered on top of
/// this at P5 under `integrations`.
pub struct DataState {
    pub config: GatewayConfig,
    pub shared: SharedConfigHandle,
    pub metrics: Arc<Metrics>,
    pub tls: SniStore,
    /// The policy handle (design §2.1 / D-7): `ArcSwap<PolicyConfig>` + mtime
    /// poll + admin `/reload`. Built from `config.policy_path`; a missing
    /// file is the all-pass default, a malformed file starts all-pass + warn.
    pub policy: Arc<crate::policy_loader::PolicyHandle>,
}

impl DataState {
    /// Build the state from a config + initial control-plane snapshot.
    ///
    /// `data` may be empty on first boot (the adapter's first LIST fills it, and the data plane
    /// binds only after `ready()`); a structurally-invalid snapshot is rejected here.
    pub fn new(config: GatewayConfig, data: ConfigData) -> Result<Self, GatewayError> {
        let shared = SharedConfig::new(data)
            .map_err(|issues| GatewayError::Other(format!("initial snapshot rejected: {issues:?}")))?;
        let policy = Arc::new(crate::policy_loader::PolicyHandle::new(
            config.policy_path.clone(),
        ));
        Ok(Self {
            config,
            shared: SharedConfigHandle::new(shared),
            metrics: Arc::new(Metrics::new()),
            tls: SniStore::new(),
            policy,
        })
    }

    /// Real admin service state (`/metrics` `/healthz` `/reload` `/stats/usage`).
    ///
    /// The reload hook is wired to the **policy** reload (design §2.1 / D-7):
    /// the closure captures the [`PolicyHandle`] and forces a reload from the
    /// configured path. Returns `true` on success (new policy swapped),
    /// `false` on failure (last-known-good retained; the admin endpoint
    /// reports 500 so operators can distinguish).
    pub fn admin_state(&self) -> Arc<AdminState> {
        let policy = self.policy.clone();
        let reloader: Arc<dyn Fn() -> bool + Send + Sync> =
            Arc::new(move || policy.reload());
        Arc::new(AdminState::new(
            self.metrics.clone(),
            self.config.admin_token.clone(),
            Some(reloader),
        ))
    }

    /// Real 15020 stats service state (`/stats/prometheus` `/stats`).
    pub fn stats_state(&self) -> Arc<StatsState> {
        Arc::new(StatsState::new(self.metrics.clone()))
    }
}

// ---------------------------------------------------------------------------
// Pure, always-available helpers
// ---------------------------------------------------------------------------

/// Poll a TCP `connect` to `addr` every `interval` until it succeeds or `timeout` elapses
/// (design §11.2 `GPUSTACK_API_PORT` readiness, aligned with the original `gateway/run`
/// readiness). Bounded: it **fails** (returns `Err`) rather than hanging when the target is
/// never reachable. Logs the start, each retry (debug), and the outcome.
pub async fn readiness_wait(
    addr: &str,
    interval: Duration,
    timeout: Duration,
) -> Result<(), GatewayError> {
    info!(addr, timeout = timeout.as_millis(), "readiness: waiting for target");
    let deadline = Instant::now() + timeout;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let connected = tokio::net::TcpStream::connect(addr).await.is_ok();
        if connected {
            info!(addr, attempt, "readiness: target reachable");
            return Ok(());
        }
        if Instant::now() >= deadline {
            error!(
                addr,
                attempt,
                timeout = timeout.as_millis(),
                "readiness: target not reachable within timeout"
            );
            return Err(GatewayError::Other(format!(
                "readiness: {addr} not reachable within {timeout:?} after {attempt} attempts (fail-fast)"
            )));
        }
        debug!(addr, attempt, "readiness: not up yet; retrying in {interval:?}");
        tokio::time::sleep(interval).await;
    }
}

/// Resolve the `jwt_secret_key` (design §9 precedence) and derive the gateway auth token in one
/// step. This is the fail-fast path: a missing/empty key yields `Err` — the launcher turns it
/// into a logged, non-zero exit (never a silent degrade, which would 401 every usage report).
///
/// Compiled under `integrations` or `test` (it calls the frozen `hygress-egress::token`
/// contract); excluded from the pure default build so that build stays free of egress.
#[cfg(any(test, feature = "integrations"))]
pub fn resolve_gateway_credentials(jwt_env: Option<&str>, data_dir: &std::path::Path) -> Result<String, String> {
    let key = hygress_egress::token::resolve_jwt_key(jwt_env, data_dir)
        .map_err(|e| format!("fail-fast: jwt_secret_key unresolved: {e}"))?;
    Ok(hygress_egress::token::derive_gateway_token(&key))
}

/// Build the Pingora [`Server`] hosting the **always-on control-plane listeners**: the admin
/// `ServeHttp` on `HYGRESS_ADMIN_ADDR` and the 15020 stats shallow-compat `ServeHttp`. The
/// terminate-mode data plane (and TLS) are added by the `integrations` launcher.
///
/// This only records the endpoints here — the sockets are actually bound when the server is
/// started (by [`main`] → `run_forever`), i.e. **after** the adapter's `ready()`.
pub fn build_server(
    config: &GatewayConfig,
    admin: Arc<AdminState>,
    stats: Arc<StatsState>,
) -> PingoraResult<Server> {
    // P2: run the data plane on as many worker threads as the host has vCPUs.
    // Pingora 0.8.1 defaults `ServerConf::threads` to **1**, which serializes
    // every downstream connection onto a single core (the whole gateway's
    // throughput is then bounded by one CPU). Use the host's vCPU count,
    // clamped to a sane [2, 32] for small / very large hosts.
    //
    // Future item: honour a cgroup CPU-quota (e.g. a container limited to 1
    // vCPU) instead of the host's physical vCPUs — that is not visible via
    // `available_parallelism()`, so it is left out of scope here.
    let n = std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(8)
        .clamp(2, 32);
    let conf = ServerConf { threads: n, ..Default::default() };
    let mut server = Server::new_with_opt_and_conf(Some(Opt::default()), conf);
    server.bootstrap();

    let mut admin_svc = Service::new("hygress-admin".to_string(), AdminService::new(admin));
    admin_svc.add_tcp(&config.admin_addr);
    server.add_service(admin_svc);

    let mut stats_svc = Service::new("hygress-stats".to_string(), StatsService::new(stats));
    stats_svc.add_tcp(&format!("0.0.0.0:{}", config.pilot_agent_metrics_port));
    server.add_service(stats_svc);

    Ok(server)
}

// ---------------------------------------------------------------------------
// `integrations`-gated data-plane launch (design §11.2)
// ---------------------------------------------------------------------------

/// Attach the terminate-mode [`HygressProxy`] data plane to `server` (plain HTTP on
/// `GATEWAY_HTTP_PORT`; a TLS listener on `GATEWAY_TLS_PORT` when the snapshot carries TLS
/// material, plain HTTP otherwise). Called **only after** the adapter `ready()`.
#[cfg(feature = "integrations")]
fn attach_data_plane(
    server: &mut Server,
    state: Arc<crate::context::GatewayState>,
    config: &GatewayConfig,
) -> Result<(), GatewayError> {
    use crate::pipe::HygressProxy;

    let proxy = HygressProxy::new(state.clone());
    let mut data_svc = pingora_proxy::http_proxy_service(&server.configuration, proxy);
    data_svc.add_tcp(&config.http_bind());

    let data = state.config.load();
    if data.tls.hosts.is_empty() {
        debug!("data plane: no TLS material in snapshot; plain HTTP only");
    } else {
        let tls_addr = format!("0.0.0.0:{}", config.tls_port);
        match write_default_tls_pem(&data.tls) {
            // `write_default_tls_pem` returns the PEM file **paths as `String`s**;
            // `&String` derefs to the `&str` file paths Pingora 0.8's `add_tls` wants.
            Ok((cert, key)) => data_svc
                .add_tls(&tls_addr, &cert, &key)
                .map_err(|e| GatewayError::Other(format!("TLS listener on {tls_addr}: {e}")))?,
            Err(e) => warn!(error = %e, "TLS material present but not exportable; plain HTTP only on {tls_addr}"),
        }
        info!(tls_port = config.tls_port, "data plane TLS listener bound (default cert)");
    }

    server.add_service(data_svc);
    Ok(())
}

/// Export the `gpustack-tls-default` (or first) host's PEM pair to a temp file pair so Pingora's
/// file-based `add_tls` can terminate TLS with it. Pingora 0.8's public listener API only accepts
/// file paths (single cert), so per-host SNI multi-cert selection is served with the default cert
/// (a documented limitation); the plain-HTTP / port-discipline behaviour is unaffected.
#[cfg(feature = "integrations")]
fn write_default_tls_pem(tls: &hygress_core::prelude::TlsConfig) -> std::io::Result<(String, String)> {
    let host = tls
        .hosts
        .iter()
        .find(|h| h.is_default)
        .or_else(|| tls.hosts.first())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no TLS host configured"))?;
    let dir = std::env::temp_dir().join(format!("hygress-gateway-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    std::fs::write(&cert, &host.cert_pem)?;
    std::fs::write(&key, &host.key_pem)?;
    Ok((cert.to_string_lossy().into_owned(), key.to_string_lossy().into_owned()))
}

/// Kubeconfig for the control-plane `Controller` (design §9): the `KUBECONFIG` env, else
/// `{data_dir}/higress/kubeconfig` if it exists, else `None` (in-cluster).
#[cfg(feature = "integrations")]
fn resolve_kubeconfig(config: &GatewayConfig) -> Option<std::path::PathBuf> {
    if let Ok(kc) = std::env::var("KUBECONFIG") {
        let kc = kc.trim();
        if !kc.is_empty() {
            return Some(std::path::PathBuf::from(kc));
        }
    }
    let fallback = std::path::Path::new(&config.data_dir).join("higress").join("kubeconfig");
    fallback.is_file().then_some(fallback)
}

/// Process shutdown signal (SIGTERM/SIGINT). The `Controller`'s poll loop awaits this so a stop
/// is clean; Pingora's own handler drives the data-plane graceful stop.
#[cfg(feature = "integrations")]
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(h) => h,
            Err(e) => {
                error!("install SIGTERM handler: {e}");
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("SIGINT received; shutting down"),
            _ = term.recv() => info!("SIGTERM received; shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("SIGINT received; shutting down");
    }
}

// ---------------------------------------------------------------------------
// Process entry
// ---------------------------------------------------------------------------

/// Process entry: parse config, drive [`run`], then block the main thread on the Pingora server
/// (which handles SIGTERM/SIGINT and exits 0 on a graceful stop). A startup failure (readiness
/// timeout, jwt fail-fast, control-plane init) is a logged **non-zero** exit.
pub fn main() {
    init_tracing();
    let config = GatewayConfig::from_env();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let server = match runtime.block_on(run(&config)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("hygress-gateway: startup failed: {e}");
            std::process::exit(1);
        }
    };

    info!("hygress-gateway: all pre-conditions met; starting server (blocking until SIGTERM/SIGINT)");
    // Blocks until a graceful stop (SIGTERM/SIGINT); never returns on the normal path.
    server.run_forever();
}

/// Build the pure state, run the readiness probe, and (under `integrations`) the full
/// control-plane + data-plane pre-launch, returning the bound Pingora [`Server`].
///
/// `None` is never returned on the failure paths — a failed pre-condition is an `Err` that
/// [`main`] turns into a non-zero exit (fail-fast, design §11.2).
async fn run(config: &GatewayConfig) -> Result<Server, Box<dyn std::error::Error + Send + Sync>> {
    let ds = DataState::new(config.clone(), ConfigData::default())?;
    let admin = ds.admin_state();
    let stats = ds.stats_state();
    info!(
        http_port = config.http_port,
        tls_port = config.tls_port,
        admin = config.admin_addr.as_str(),
        stats_port = config.pilot_agent_metrics_port,
        "hygress-gateway bootstrap: state built (admin + stats listeners)"
    );

    // 1. Readiness probe (design §11.2): GPUSTACK_API_PORT must be up before the data plane
    //    listens, so the gateway fails fast instead of eating GPUStack's readiness window.
    //    Returns an error (main() → non-zero exit) on timeout — never a silent degrade.
    let api_addr = format!("127.0.0.1:{}", config.gpustack_api_port);
    readiness_wait(&api_addr, Duration::from_millis(500), config.api_ready_timeout)
        .await?;

    #[cfg(feature = "integrations")]
    {
        use hygress_adapter::Controller;
        use hygress_egress::forward_auth;
        use hygress_egress::provider::ProviderClient;
        use hygress_egress::usage_sink::GpustackSink;
        use std::path::Path;

        use crate::context::GatewayState;

        // 2. jwt_secret_key fail-fast (design §9) + derived gateway token.
        let token = resolve_gateway_credentials(config.jwt_secret_key.as_deref(), Path::new(&config.data_dir))
            .map_err(|e| {
                error!("{e}");
                e
            })?;
        debug!("gateway token derived");

        // 3. Egress clients (all loopback to GPUSTACK_API_PORT) + the data-plane state.
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let base_url = format!("http://127.0.0.1:{}", config.gpustack_api_port);
        let auth = Some(Arc::new(
            forward_auth::Client::new(&base_url, http.clone()).with_auth_token(token.clone()),
        ));
        let sink = Some(Arc::new(GpustackSink::new(
            &format!("{base_url}/v2/usage/gateway-metrics"),
            http.clone(),
            token.clone(),
        )));
        let gateway_state = Arc::new(GatewayState {
            config: Arc::new(ds.shared.clone()),
            auth,
            sink,
            upstream: Arc::new(ProviderClient),
            metrics: ds.metrics.clone(),
            // Design §2.1: the policy handle (hot-reloadable `ArcSwap` + mtime
            // poll + admin `/reload`).
            policy: Some(ds.policy.clone()),
            // Design §4.1: per-key rate-limit token buckets (ip:/consumer: keys).
            ratelimit_buckets: Arc::new(dashmap::DashMap::new()),
            // Design §4.2: the in-memory token-quota engine.
            quota: Arc::new(hygress_core::prelude::QuotaEngine::new()),
            quota_k: config.quota_k,
            // Design §4.4 B4b: the shared egress client + LLM guardrail
            // endpoint (a `None` URL = LLM guardrail not configured, D-14).
            http: http.clone(),
            guardrail_url: config.guardrail_url.clone(),
            guardrail_clients: Arc::new(dashmap::DashMap::new()),
        });

        // 3b. Design §2.1 / D-7: the mtime 1s poll task (the same cadence as
        //     the adapter poll). A changed `policy.yaml` swaps the live
        //     `ArcSwap` on the next tick; the admin `POST /reload` forces it.
        //     Also performs periodic eviction of idle quota counters and
        //     rate-limit buckets (BLOCK-2: leak prevention).
        let policy_poll = ds.policy.clone();
        let poll_interval = config.poll_interval;
        let quota_evict = gateway_state.quota.clone();
        let ratelimit_evict = gateway_state.ratelimit_buckets.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.tick().await; // the first tick is immediate
            // Idle threshold: 5 minutes (hardcoded; not currently
            // configurable via env).
            let idle_ms: u64 = 300_000;
            loop {
                interval.tick().await;
                // L10: the policy poll does synchronous fs metadata + (on a
                // change) a YAML parse — run the sync work on the blocking
                // pool so the 1s tick never blocks a runtime worker.
                let owner = policy_poll.clone();
                if let Err(e) =
                    tokio::task::spawn_blocking(move || owner.poll()).await
                {
                    debug!(error = %e, "policy poll task failed");
                }
                // Evict idle quota counters (BLOCK-2: spec-agnostic leak
                // prevention; complements the window-based `gc_stale`).
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let evicted = quota_evict.evict_idle(now_ms, idle_ms);
                if evicted > 0 {
                    debug!(evicted, "quota: evicted idle counters");
                }
                // Evict idle rate-limit buckets (same idle threshold).
                let cutoff = now_ms.saturating_sub(idle_ms);
                let mut removed = 0usize;
                ratelimit_evict.retain(|_, e| {
                    if e.last_active_ms >= cutoff {
                        true
                    } else {
                        removed += 1;
                        false
                    }
                });
                if removed > 0 {
                    debug!(removed, "ratelimit: evicted idle buckets");
                }
            }
        });

        // 4. Control plane: build the Controller, optionally seed the topology-B IngressClass,
        //    and run the 1s poll loop as a task (stopped on shutdown_signal()).
        let controller = Controller::new(
            ds.shared.inner.clone(),
            resolve_kubeconfig(config),
            config.gateway_namespace.clone(),
            "higress".to_string(),
            config.poll_interval,
        )
        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("control plane init: {e}")))?;
        if config.topology_b {
            info!("topology B enabled: seeding the higress IngressClass (best-effort)");
            controller.seed_ingress_class();
        }
        let ready = controller.ready();
        tokio::spawn(controller.run(shutdown_signal()));

        // 5. Await the first snapshot (bind-ready, design §11.2) before binding the data plane.
        //    Bounded: a control-plane connect failure ends `Controller::run` without firing
        //    `ready()`, so without a cap the process would hang here and s6 would loop health
        //    checks. Cap it to fail fast rather than hang (design §11.2: 快速失败而非 10 分钟挂起).
        let bound = config.snapshot_timeout;
        if tokio::time::timeout(bound, ready.notified()).await.is_err() {
            return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                "control plane produced no first snapshot within {bound:?}; refusing to bind the data plane (fail-fast)"
            )));
        }
        info!("controller ready: first snapshot stored; binding data plane");

        // 6. Build the server (admin + stats) and attach the terminate-mode data plane (+ TLS) —
        //    all bound only now, after ready().
        let mut server = build_server(config, admin, stats)?;
        attach_data_plane(&mut server, gateway_state, config)?;
        Ok(server)
    }

    #[cfg(not(feature = "integrations"))]
    {
        warn!(
            "the `integrations` feature is disabled: the Pingora data plane, the egress \
             (forward-auth / usage / provider) clients, and the control-plane Controller are not \
             compiled (P5-pending). The admin + 15020 listeners are served; the data plane is not."
        );
        let server = build_server(config, admin, stats)?;
        Ok(server)
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

// ---------------------------------------------------------------------------
// Tests — pure helpers + real admin/stats listeners (no egress/adapter, no port 80/443).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static STATIC_TMP: AtomicUsize = AtomicUsize::new(0);

    /// A throwaway empty dir (unique per call — safe under parallel test threads).
    fn tempdir() -> std::path::PathBuf {
        let n = STATIC_TMP.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("hygress-gateway-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn ephemeral_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    async fn wait_tcp(port: u16) {
        let addr = format!("127.0.0.1:{port}");
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("listener on {port} did not become ready");
    }

    // ----- readiness-wait loop behaviour -----

    #[tokio::test]
    async fn readiness_wait_succeeds_when_target_is_up() {
        let port = ephemeral_port().await;
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        // Accept nothing (the readiness probe only needs a successful connect).
        tokio::spawn(async move {
            let _ = listener;
            // Keep the listener open for the duration of the test.
            std::future::pending::<()>().await;
        });

        let res = readiness_wait(
            &format!("127.0.0.1:{port}"),
            Duration::from_millis(10),
            Duration::from_secs(5),
        )
        .await;
        assert!(res.is_ok(), "a live target must pass the readiness probe: {res:?}");
    }

    #[tokio::test]
    async fn readiness_wait_fails_fast_when_target_is_down() {
        // A port nothing is listening on (ephemeral, dropped) → connect refused → timeout.
        let port = ephemeral_port().await;
        let start = std::time::Instant::now();
        let res = readiness_wait(
            &format!("127.0.0.1:{port}"),
            Duration::from_millis(20),
            Duration::from_millis(150),
        )
        .await;
        assert!(res.is_err(), "a dead target must fail the readiness probe: {res:?}");
        // Bounded: returns promptly, well under an unbounded hang.
        assert!(start.elapsed() < Duration::from_secs(5));
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("readiness"), "error should mention readiness: {msg}");
    }

    #[tokio::test]
    async fn readiness_wait_reports_attempts_in_error() {
        let port = ephemeral_port().await;
        let err = readiness_wait(
            &format!("127.0.0.1:{port}"),
            Duration::from_millis(10),
            Duration::from_millis(60),
        )
        .await
        .expect_err("down target must error");
        // Several attempts were made before the bounded timeout.
        assert!(err.to_string().contains("attempts"));
    }

    // ----- jwt fail-fast path (missing env + missing file → Err) -----

    #[test]
    fn jwt_resolves_from_env_and_derives_token() {
        // env present → the key is used and the token is the 64-char lowercase-hex HMAC.
        let dir = tempdir();
        let token = resolve_gateway_credentials(Some("my-secret-key"), &dir).unwrap();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn jwt_resolves_from_file_when_no_env() {
        let dir = tempdir();
        std::fs::write(dir.join("jwt_secret_key"), "file-key\n").unwrap();
        // env absent → the file key is used (same derived token as env-provided).
        let from_file = resolve_gateway_credentials(None, &dir).unwrap();
        let from_env = resolve_gateway_credentials(Some("file-key"), &dir).unwrap();
        assert_eq!(from_file, from_env);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn jwt_fail_fast_when_env_and_file_missing() {
        // Neither env nor the {data_dir}/jwt_secret_key file → fail-fast Err (never silent).
        let dir = tempdir(); // empty dir, no jwt_secret_key
        let err = resolve_gateway_credentials(None, &dir).expect_err("absent key must fail");
        assert!(err.contains("jwt_secret_key"), "error should name the key: {err}");
        assert!(err.contains("fail-fast"), "error should mark fail-fast: {err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn jwt_fail_fast_when_key_is_blank() {
        // A blank env (or blank file) is meaningful-failure too (an empty secret would 401
        // every usage report), so it is Err, not silently accepted.
        let dir = tempdir();
        assert!(resolve_gateway_credentials(Some("   \n"), &dir).is_err());
        assert!(resolve_gateway_credentials(Some(""), &dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ----- real admin/stats listeners bind + serve ----------

    #[tokio::test]
    async fn bootstrap_serves_admin_and_stats_on_real_listeners() {
        let metrics = Arc::new(Metrics::new());
        // Seed one sample so the prometheus families render a child series.
        metrics.record_request(200, "model_route");
        let admin = Arc::new(AdminState::new(metrics.clone(), None, None));
        let stats = Arc::new(StatsState::new(metrics.clone()));

        let admin_port = ephemeral_port().await;
        let stats_port = ephemeral_port().await;
        let cfg = GatewayConfig {
            admin_addr: format!("127.0.0.1:{admin_port}"),
            pilot_agent_metrics_port: stats_port,
            ..Default::default()
        };

        let server = build_server(&cfg, admin, stats).expect("build server");
        std::thread::spawn(move || {
            server.run_forever();
        });

        wait_tcp(admin_port).await;
        wait_tcp(stats_port).await;

        // admin: /healthz (open) + /metrics (open, prometheus).
        let hz = reqwest::get(format!("http://127.0.0.1:{admin_port}/healthz"))
            .await
            .expect("GET /healthz");
        assert_eq!(hz.status(), 200);
        assert_eq!(hz.text().await.unwrap(), "ok\n");

        let mt = reqwest::get(format!("http://127.0.0.1:{admin_port}/metrics"))
            .await
            .expect("GET /metrics");
        assert_eq!(mt.status(), 200);
        assert!(mt.text().await.unwrap().contains("hygress_requests_total"));

        // stats: 15020 shallow-compat /stats/prometheus.
        let sp = reqwest::get(format!("http://127.0.0.1:{stats_port}/stats/prometheus"))
            .await
            .expect("GET /stats/prometheus");
        assert_eq!(sp.status(), 200);
        assert!(sp.text().await.unwrap().contains("hygress_requests_total"));

        // stats: /stats shallow JSON.
        let st = reqwest::get(format!("http://127.0.0.1:{stats_port}/stats"))
            .await
            .expect("GET /stats");
        assert_eq!(st.status(), 200);
        assert!(st.text().await.unwrap().contains("LIVE"));
    }

    // ----- the pre-existing pure wiring tests -----

    #[test]
    fn data_state_builds_from_config() {
        let cfg = GatewayConfig::default();
        let ds = DataState::new(cfg, ConfigData::default()).unwrap();
        // The initial (empty) snapshot loads cleanly.
        assert_eq!(ds.shared.load().routes.len(), 0);
        // Real metrics + services build (seed one sample so the family renders).
        ds.metrics.record_request(200, "model_route");
        assert!(ds.metrics.encode().contains("hygress_requests_total"));
        let admin = ds.admin_state();
        assert_eq!(admin.route("GET", "/healthz", &hygress_core::transform::HeaderMap::new()).status, 200);
        let stats = ds.stats_state();
        assert_eq!(stats.route("GET", "/stats/prometheus").status, 200);
        // Empty SNI store (no certs yet).
        assert!(ds.tls.load().is_empty());
    }

    #[test]
    fn data_state_rejects_structurally_invalid_snapshot() {
        // A route with a malformed path regex is a structural failure that rejects the
        // whole snapshot (SharedConfig::new returns the issues).
        use hygress_core::prelude::{PathPred, RouteKind, RouteRule, Destination};
        let bad = ConfigData {
            routes: vec![
                RouteRule::new(
                    "m",
                    RouteKind::Main,
                    vec![PathPred::new("([unclosed")],
                    vec![Destination::new("a.static:80")],
                )
                .unwrap(),
            ],
            ..Default::default()
        };
        let cfg = GatewayConfig::default();
        assert!(DataState::new(cfg, bad).is_err());
    }
}
