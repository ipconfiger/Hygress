//! hygress-adapter — control-plane adapter (design §5).
//!
//! Implements **strategy 2**: Hygress never implements a Kubernetes API server; it is a pure
//! **read-only consumer** of GPUStack's CRD writes against the (kept) embedded file-storage
//! apiserver at `127.0.0.1:18443` (or an external cluster in topology B).
//!
//! Responsibilities (design §5):
//! - [`gvr`]       — Group/Version/Resource + [`ApiResource`]s for the 3 CRDs and the standard kinds
//! - [`client`]    — kube 4.x `Api` wiring; kubeconfig (file or in-cluster) loading
//! - [`snapshot`]  — full LIST (label selector `gpustack.ai/managed=true`) -> `ConfigData` objects
//! - [`translate`] — **pure** CRD JSON -> `hygress_core` translation (unit-tested with real fixtures)
//! - [`reconcile`] — api-resources discovery (60s/5s) + the gated topology-B IngressClass seed
//!   (invoked only from [`Controller::run`] when `seed_ingress_class` is enabled)
//!
//! Invariants (design):
//! - Pure consumer: no writes to the apiserver, no local persistence (CRDs are the truth), except
//!   the single gated IngressClass seed: [`Controller::run`] seeds the `higress` IngressClass
//!   (which GPUStack does not create and `is_supported_higress` probes by name) once, only when
//!   the controller is built with `seed_ingress_class = true` (topology B / external). Topology A
//!   (embedded apiserver) performs **zero** apiserver writes.
//! - Bind-ready: the first successful snapshot store fires [`Controller::ready`]; the data plane
//!   must await it before binding ports.
//! - Last-known-good: a transient LIST/transport failure (or a structurally rejected snapshot)
//!   keeps the previous snapshot; only a successful store swaps.
//! - Orphan tolerance: never delete upstream objects; `ai-route-model-<id>` is legacy; the
//!   unmanaged global custom-response EnvoyFilter is ignored (it lacks the managed label).
//! - ORA3-M16 (documented downgrade): the embedded topology does **NOT** consume the unlabeled
//!   `higress-config` timing ConfigMap (GPUStack writes it without the managed label, so the
//!   snapshot's label-selector listing never sees it — see `snapshot.rs`); `config.timing`
//!   stays at the built-in seed defaults (downstream 1800s / upstream 10s).
//!
//! ## Public API (the gateway depends on this)
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use hygress_core::SharedConfig;
//! use hygress_adapter::{Controller, ControllerHooks};
//!
//! # async fn example(shared: Arc<SharedConfig>) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let controller = Controller::new(
//!     shared,
//!     None, // kubeconfig: Some(<path>) for the embedded file kubeconfig, else in-cluster/KUBECONFIG
//!     "higress-system".into(),
//!     "higress".into(),
//!     Duration::from_secs(1),
//!     false,           // seed_ingress_class: topology B / external (seeds once inside `run`)
//!     ControllerHooks::default(), // observability hooks (optional; see [`ControllerHooks`])
//! )?;
//!
//! let ready = controller.ready();
//! // Await this (together with `run`'s shutdown) before binding the data-plane ports — it fires
//! // once when the first snapshot has been stored (bind-ready, design §5.3). Not run in a doc
//! // test (no cluster / tokio runtime here).
//! let _notified = ready.notified();
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod reconcile;
mod snapshot;

// Public module surface (the documented contract, design §5):
// - `gvr`: Group/Version/Resource + [`ApiResource`] definitions for the consumed kinds.
// - `translate`: the **pure** CRD JSON → `hygress_core` translation layer (unit-tested).
pub mod gvr;
pub mod translate;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use kube::runtime::watcher::{watcher, Config};
use kube::Api;

pub use client::Client;
pub use error::{Error, Result};
use snapshot::SnapshotFingerprint;
pub use translate::{Object, ObjectKind};

/// Mirror-ingress name env var (GPUStack `GATEWAY_MIRROR_INGRESS_NAME`, design §2.1.1 / §4.3).
const MIRROR_NAME_ENV: &str = "GATEWAY_MIRROR_INGRESS_NAME";

/// Convergence poll cadence (P4/1.1). The primary fast path is event-driven
/// (kube WATCH); this tick is the **convergence backstop** and — on the embedded
/// GPUStack apiserver, where WATCH is permanently unavailable
/// (`NoResourceVersion`) — the **only** propagation path for route/model
/// changes. The cadence is `max(poll_interval, this floor)`: the default
/// `POLL_INTERVAL=1000ms` therefore converges in ~1s, matching the cadence of
/// the real embedded-Higress pilot (a ~1s LIST poll) that this gateway replaces.
/// The rv-fingerprint short-circuit inside [`Controller::run`] → `sync_once`
/// keeps a steady-state wake a no-op store, and watcher-error logs are
/// rate-limited separately — so the tighter cadence adds LIST traffic, not log
/// or store churn.
const CONVERGE_MIN_TICK: Duration = Duration::from_secs(1);

/// ORA3-MAJ-1: optional observability hooks the controller invokes so the
/// gateway can expose control-plane health on `/metrics` (the adapter itself
/// stays dependency-free — no prometheus/tracing-coupled types leak in here).
///
/// All callbacks are cheap (`Fn` — never awaited); a `None` hook is a no-op.
#[derive(Default, Clone)]
pub struct ControllerHooks {
    /// Called `(kind, class)` on **every** classified watcher error — `kind`
    /// is the watched resource kind (`configmap`/`ingress`/`secret`/…),
    /// `class` is `"permanent"` (watch unsupported by the apiserver →
    /// convergence degraded to the ~1s poll tick) or `"transient"`
    /// (recoverable watch failure). Invoked regardless of log-rate limiting.
    pub on_watch_error: Option<Arc<dyn Fn(&'static str, &'static str) + Send + Sync>>,
    /// Called after **every** successful store of a new snapshot (the store
    /// path in [`Controller::run`] → `sync_once`); the gateway counts stores
    /// and stamps its last-store staleness gauge from here.
    pub on_snapshot_store: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Control-plane adapter (strategy 2): a read-only kube CRD consumer that LISTs the managed
/// Higress CRDs, translates them into [`hygress_core::ConfigData`], and stores the snapshot
/// into the gateway's [`SharedConfig`] on a poll interval.
pub struct Controller {
    shared: Arc<hygress_core::SharedConfig>,
    kubeconfig: Option<std::path::PathBuf>,
    gateway_namespace: String,
    ingress_class: String,
    poll_interval: std::time::Duration,
    /// Whether [`Controller::run`] performs the single best-effort `higress`
    /// IngressClass seed (AM-1). Enabled for topology B / external (the apiserver
    /// there never hosts the IngressClass and `is_supported_higress` probes it by
    /// name); disabled for topology A (embedded apiserver) so the control plane
    /// stays a pure read-only consumer with zero apiserver writes.
    seed_ingress_class: bool,
    mirror_name: String,
    ready: Arc<tokio::sync::Notify>,
    ready_notified: Arc<std::sync::atomic::AtomicBool>,
    /// Last LISTed snapshot fingerprint (P4 short-circuit). Interior-mutable: the main loop
    /// updates it once per reconcile wake. A `Mutex` is fine — one short critical section per
    /// wake in the control-plane runtime (never held across an `.await`).
    last_fingerprint: Mutex<Option<SnapshotFingerprint>>,
    /// ORA3-MAJ-1: observability hooks (see [`ControllerHooks`]).
    hooks: ControllerHooks,
}

impl Controller {
    /// Validate inputs and build the controller.
    ///
    /// `shared` is the gateway's config holder (the adapter stores snapshots into it);
    /// `kubeconfig` is the embedded file kubeconfig path (`None` → in-cluster / `KUBECONFIG`);
    /// `ingress_class` is the IngressClass name to probe/seed (topology B); `poll_interval`
    /// is the cadence of the poll-based convergence — [`Controller::run`] reconciles
    /// **event-driven** on watch changes with a `max(poll_interval, 1s)` tick as the backstop /
    /// degraded-mode propagation path (default `POLL_INTERVAL=1000ms` → ~1s convergence on the
    /// embedded apiserver where WATCH is unavailable, matching the real Higress pilot cadence;
    /// a shorter config value is floored to 1s); `seed_ingress_class` gates the single
    /// IngressClass seed inside [`Controller::run`] (AM-1) — set it for topology B /
    /// external, leave it `false` for topology A (embedded apiserver, zero writes);
    /// `hooks` carries the optional ORA3-MAJ-1 observability callbacks.
    ///
    /// Fails with [`Error::InvalidConfig`] on an empty namespace / ingress class, or a
    /// zero poll interval.
    pub fn new(
        shared: Arc<hygress_core::SharedConfig>,
        kubeconfig: Option<std::path::PathBuf>,
        gateway_namespace: String,
        ingress_class: String,
        poll_interval: std::time::Duration,
        seed_ingress_class: bool,
        hooks: ControllerHooks,
    ) -> Result<Self> {
        if gateway_namespace.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "gateway_namespace must be non-empty".into(),
            ));
        }
        if ingress_class.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "ingress_class must be non-empty".into(),
            ));
        }
        if poll_interval.is_zero() {
            return Err(Error::InvalidConfig("poll_interval must be > 0".into()));
        }

        let mirror_name = std::env::var(MIRROR_NAME_ENV)
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| translate::MIRROR_NAME.to_string());

        Ok(Self {
            shared,
            kubeconfig,
            gateway_namespace,
            ingress_class,
            poll_interval,
            seed_ingress_class,
            mirror_name,
            ready: Arc::new(tokio::sync::Notify::new()),
            ready_notified: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_fingerprint: Mutex::new(None),
            hooks,
        })
    }

    /// The bind-ready signal (design §5.3): fires once when the first snapshot has been
    /// stored. The data plane awaits [`tokio::sync::Notify::notified`] before binding ports.
    pub fn ready(&self) -> Arc<tokio::sync::Notify> {
        self.ready.clone()
    }

    /// Run the control-plane loop until `shutdown` resolves.
    ///
    /// 1. Connect (file kubeconfig or in-cluster / `KUBECONFIG`).
    /// 2. Wait for api-resources discovery (60s / 5s budget).
    /// 3. Best-effort, idempotent IngressClass seed — **only** when the controller was
    ///    built with `seed_ingress_class = true` (topology B / external). Topology A
    ///    (embedded apiserver) skips it entirely: zero apiserver writes (AM-1).
    /// 4. **First** full LIST → translate → store (bind-ready: the first successful store fires
    ///    [`Controller::ready`]) — identical to the old 1s-poll design's first iteration.
    /// 5. **Event-driven** (P4/1.1): one `kube::runtime::watcher` per managed kind. Each
    ///    `Ok` event marks a shared dirty flag + notifies the main loop; `Err` items are
    ///    logged and the stream is kept alive (kube-runtime reconnects / relists internally).
    ///    The main loop reconciles on a dirty flag or the convergence poll tick
    ///    (`max([`CONVERGE_MIN_TICK`], poll_interval)` — default ~1s); a LIST/transport
    ///    failure keeps the last-known-good snapshot and keeps polling.
    ///
    ///    On the embedded GPUStack apiserver (topology A) every watch is permanently
    ///    unsupported, so this poll tick **is** the route/model propagation path — at the
    ///    default 1s cadence it matches the real embedded-Higress pilot's ~1s LIST poll.
    ///
    /// Returns `Err` only on connect/discovery failure (the event loop never aborts on a
    /// transient error).
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
        let client =
            client::Client::connect(self.kubeconfig.as_deref(), self.gateway_namespace.clone())
                .await?;

        reconcile::wait_for_apiserver_ready(&client).await?;

        // Best-effort, idempotent seed (AM-1): gated on the construction flag so topology B
        // (external) seeds once here while topology A (embedded apiserver) never writes —
        // the 405/warn noise path is gone. A failure here does not stop the loop.
        if self.seed_ingress_class {
            if let Err(e) = reconcile::ensure_ingress_class(&client, &self.ingress_class).await {
                tracing::warn!("IngressClass seed (best-effort) failed: {e}");
            }
        }

        // ① First snapshot (bind-ready): the initial full LIST + store, unchanged from the old
        //    1s-poll design — the first successful store fires [`Controller::ready`].
        self.sync_once(&client).await;

        // ② Event-driven watchers (P4/1.1): one per managed kind. McpBridge is listed WITHOUT
        //    the managed selector (the plain-list invariant from the snapshot layer); every
        //    other kind uses the managed label selector.
        let dirty = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let managed = client::MANAGED_SELECTOR;
        let hooks = self.hooks.clone();
        let handles = vec![
            Self::spawn_watcher(
                client.mcpbridges(),
                Config::default(),
                "mcpbridge",
                &dirty,
                &notify,
                hooks.clone(),
            ),
            Self::spawn_watcher(
                client.wasmplugins(),
                Config::default().labels(managed),
                "wasmplugin",
                &dirty,
                &notify,
                hooks.clone(),
            ),
            Self::spawn_watcher(
                client.envoyfilters(),
                Config::default().labels(managed),
                "envoyfilter",
                &dirty,
                &notify,
                hooks.clone(),
            ),
            Self::spawn_watcher(
                client.ingresses(),
                Config::default().labels(managed),
                "ingress",
                &dirty,
                &notify,
                hooks.clone(),
            ),
            Self::spawn_watcher(
                client.secrets(),
                Config::default().labels(managed),
                "secret",
                &dirty,
                &notify,
                hooks.clone(),
            ),
            Self::spawn_watcher(
                client.configmaps(),
                Config::default().labels(managed),
                "configmap",
                &dirty,
                &notify,
                hooks,
            ),
        ];

        // ③ Main loop: reconcile on a dirty flag, the convergence poll tick, or shutdown.
        // On the embedded apiserver (WATCH permanently unavailable) this tick is the
        // route/model propagation path — default ~1s (see [`CONVERGE_MIN_TICK`]).
        let poll_tick = self.poll_interval.max(CONVERGE_MIN_TICK);
        // Pin the opaque shutdown future so `select!` can poll it (it isn't guaranteed `Unpin`).
        let mut shutdown = Box::pin(shutdown);
        loop {
            // `Notify::notified()` resolves immediately if a notify is already pending (a
            // watcher fired between the previous wake and now), so creating it fresh each
            // iteration cannot miss a wakeup.
            let notified = notify.notified();
            tokio::pin!(notified);
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("adapter: shutdown received; stopping watch loop");
                    break;
                }
                _ = &mut notified => {}
                _ = tokio::time::sleep(poll_tick) => {}
            }
            // R-2: reconcile on EVERY wake (event burst debounced by `dirty`, or the
            // convergence poll tick). The rv-fingerprint short-circuit inside
            // `sync_once` no-ops on a genuinely unchanged snapshot, and a rejected /
            // failed LIST is retried by the next wake (the old fingerprint is kept on
            // failure, so the same snapshot is re-attempted rather than skipped forever).
            dirty.swap(false, Ordering::Relaxed);
            self.sync_once(&client).await;
        }

        // Abort the (infinite, self-healing) watcher tasks so they don't leak past `run`.
        for h in handles {
            h.abort();
        }
        Ok(())
    }

    /// Spawn one `kube::runtime::watcher` stream for a managed kind. Each `Ok` event marks the
    /// shared dirty flag + notifies the main loop; `Err` items are rate-limit-logged, surfaced
    /// through the ORA3-MAJ-1 `hooks.on_watch_error` callback, and the loop backs off before
    /// polling again (kube-runtime does no backoff itself — see the inline comment). A
    /// transient error must never terminate the watcher task (P4/1.1); a permanently-unsupported
    /// watch (`NoResourceVersion`, embedded apiserver) retries slowly and the poll tick in
    /// [`Controller::run`] remains the convergence path.
    fn spawn_watcher<K>(
        api: Api<K>,
        config: Config,
        kind: &'static str,
        dirty: &Arc<AtomicBool>,
        notify: &Arc<tokio::sync::Notify>,
        hooks: ControllerHooks,
    ) -> tokio::task::JoinHandle<()>
    where
        K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug + Send + 'static,
    {
        let dirty = Arc::clone(dirty);
        let notify = Arc::clone(notify);
        tokio::spawn(async move {
            let stream = watcher(api, config);
            futures::pin_mut!(stream);
            // Backoff for watcher errors. kube-runtime makes NO recovery backoff of its
            // own: errors surface on every poll and the loop below would retry
            // immediately. Two regimes:
            //  - `NoResourceVersion`: the embedded GPUStack apiserver serves LISTs without
            //    a resourceVersion, so WATCH is permanently unsupported for this kind —
            //    retry slowly (the event stream can only ever work if the apiserver
            //    starts serving rv), never hot-loop. The poll tick in `run()` (~1s by
            //    default) already reconciles, so this watcher is best-effort only.
            //  - other (transient: network / 5xx / restart races): retry briskly but not
            //    at line rate; cap so a persistently failing watch stays quiet.
            const PERMANENT_RETRY: Duration = Duration::from_secs(60);
            const TRANSIENT_RETRY: Duration = Duration::from_secs(5);
            // Log-rate limit: a permanently-failing watch must not flood the log (real
            // box: ~2000 lines/s from 6 kinds once every few ms).
            const LOG_INTERVAL: Duration = Duration::from_secs(30);
            let mut last_log = std::time::Instant::now() - LOG_INTERVAL;
            // ORA3-MAJ-1: log the "degraded to tick-only" transition at most once per kind
            // (the first permanent error that also passes the log gate).
            let mut degraded_logged = false;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_event) => {
                        dirty.store(true, Ordering::Relaxed);
                        notify.notify_one();
                    }
                    Err(e) => {
                        let permanent =
                            matches!(e, kube::runtime::watcher::Error::NoResourceVersion);
                        // ORA3-MAJ-1: surface EVERY classified error to the gateway's
                        // watch-error counter (kind + class), independent of log-rate
                        // limiting (the per-error backoff below bounds the rate).
                        if let Some(hook) = &hooks.on_watch_error {
                            hook(kind, if permanent { "permanent" } else { "transient" });
                        }
                        // Rate-limited warn (first occurrence of a burst is logged, then
                        // at most one line per LOG_INTERVAL per kind).
                        if last_log.elapsed() >= LOG_INTERVAL {
                            tracing::warn!(
                                kind,
                                permanent,
                                "watch error ({}): {e}; backoff {}s",
                                if permanent {
                                    "watch unsupported by apiserver"
                                } else {
                                    "keeping stream alive"
                                },
                                if permanent {
                                    PERMANENT_RETRY.as_secs()
                                } else {
                                    TRANSIENT_RETRY.as_secs()
                                }
                            );
                            last_log = std::time::Instant::now();
                            // ORA3-MAJ-1: a permanently-unsupported watch means this kind
                            // can only ever converge through the poll tick — say so
                            // once, loudly (the metric carries the ongoing count).
                            if permanent && !degraded_logged {
                                degraded_logged = true;
                                tracing::info!(
                                    kind,
                                    "convergence degraded to poll-based (embedded apiserver): \
                                     {kind} watch unsupported; snapshot refreshes ride the \
                                     ~1s poll tick (POLL_INTERVAL)"
                                );
                            }
                        }
                        // Pause before polling again: kube-runtime recovers on the next
                        // poll, so sleeping here IS the backoff (watcher docs).
                        tokio::time::sleep(if permanent {
                            PERMANENT_RETRY
                        } else {
                            TRANSIENT_RETRY
                        })
                        .await;
                    }
                }
            }
            // The `watcher` stream is self-healing/infinite; reaching here means it ended
            // unexpectedly — log it loudly and mark dirty so the poll tick reconciles.
            tracing::error!(
                kind,
                "watch stream ended unexpectedly; degraded to poll-based convergence until back"
            );
            dirty.store(true, Ordering::Relaxed);
            notify.notify_one();
        })
    }

    /// Lock the last-fingerprint guard, recovering from a poisoned mutex instead of panicking.
    /// (Poisoning would require a panic while holding the lock — the only holder is the single
    /// reconcile loop, and the guarded `clone`/`assign` cannot panic, so this is purely
    /// defensive.)
    fn lock_fingerprint(&self) -> std::sync::MutexGuard<'_, Option<SnapshotFingerprint>> {
        self.last_fingerprint
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// One poll iteration: LIST → fingerprint short-circuit → translate → store; keep
    /// last-known-good on failure. When the fingerprint is unchanged since the last
    /// successful store, the expensive translate + store (and the downstream
    /// RouteTable/regex rebuild) are skipped entirely (P4).
    ///
    /// R-2 (convergence): the fingerprint is advanced **only after a successful
    /// `store`**. A structurally rejected snapshot therefore leaves the OLD
    /// fingerprint in place, so the next wake re-attempts the same (changed)
    /// snapshot instead of the fingerprint short-circuit skipping it forever; a
    /// LIST/transport failure likewise keeps the old fingerprint (retried on the
    /// next wake, which now reconciles unconditionally on every wake).
    async fn sync_once(&self, client: &client::Client) {
        let prev = self.lock_fingerprint().clone();
        let (fp, data) = match snapshot::build_snapshot(
            client,
            &self.gateway_namespace,
            &self.mirror_name,
            prev.as_ref(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                // Transport / LIST failure: keep last-known-good (snapshot AND fingerprint),
                // retry on the next wake.
                tracing::warn!("snapshot LIST failed; keeping last-known-good: {e}");
                return;
            }
        };

        // Unchanged since the last successful pass: skip the translate + store entirely
        // (fp == prev; the fingerprint is already correct — no advance needed).
        let Some(data) = data else {
            tracing::debug!("snapshot unchanged (fingerprint match); skipping rebuild");
            return;
        };

        match self.shared.store(data) {
            Ok(dropped) => {
                // ORA3-MAJ-1: notify the gateway that a NEW snapshot was stored (it
                // counts the store and stamps its last-store staleness gauge).
                if let Some(hook) = &self.hooks.on_snapshot_store {
                    hook();
                }
                // R-4: surface per-object validation skips (previously silent).
                if dropped > 0 {
                    tracing::warn!(
                        skipped = dropped,
                        "snapshot stored with per-object validation skips (see core config validation)"
                    );
                }
                // Advance the fingerprint ONLY on a successful store (R-2).
                *self.lock_fingerprint() = Some(fp);
                let notified = self
                    .ready_notified
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok();
                if notified {
                    tracing::info!("first snapshot stored; signalling bind-ready");
                    self.ready.notify_one();
                }
            }
            Err(issues) => {
                // Structural failure: reject the whole snapshot, keep last-known-good.
                // The fingerprint is NOT advanced, so the next wake re-attempts it
                // (retry-next-tick, R-2) rather than skipping it forever.
                tracing::warn!(
                    "snapshot structurally rejected; keeping last-known-good: {issues:?}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::ConfigData;

    fn shared(data: ConfigData) -> Arc<hygress_core::SharedConfig> {
        Arc::new(hygress_core::SharedConfig::new(data).unwrap())
    }

    #[test]
    fn new_validates_parameters() {
        let sc = shared(ConfigData::default());
        // Empty namespace is rejected.
        assert!(Controller::new(
            sc.clone(),
            None,
            String::new(),
            "higress".into(),
            std::time::Duration::from_secs(1),
            false,
            ControllerHooks::default()
        )
        .is_err());
        // Empty ingress class is rejected.
        assert!(Controller::new(
            sc.clone(),
            None,
            "higress-system".into(),
            String::new(),
            std::time::Duration::from_secs(1),
            false,
            ControllerHooks::default()
        )
        .is_err());
        // Zero poll interval is rejected.
        assert!(Controller::new(
            sc.clone(),
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(0),
            false,
            ControllerHooks::default()
        )
        .is_err());
        // Valid params build.
        let c = Controller::new(
            sc.clone(),
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(1),
            false,
            ControllerHooks::default(),
        )
        .unwrap();
        assert!(c.mirror_name == translate::MIRROR_NAME);
        // ready() hands out a usable Notify handle.
        let _ = c.ready();
    }

    #[test]
    fn new_wires_seed_ingress_class_flag() {
        // AM-1: the construction flag is the single gate for the IngressClass seed inside
        // `Controller::run` (the standalone pre-warm `seed_ingress_class()` is gone — `run()`
        // is always spawned by the bootstrap and is the only seeding path). Topology A
        // (embedded apiserver) passes `false` → run() never calls `ensure_ingress_class` →
        // zero apiserver writes; topology B passes `true`. The actual `run()` gating needs a
        // live apiserver (no mocks in this crate), so this test pins the parameter wiring
        // (unit layer) on top of the pure-function reconcile tests.
        let sc = shared(ConfigData::default());
        let off = Controller::new(
            sc.clone(),
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(1),
            false,
            ControllerHooks::default(),
        )
        .unwrap();
        assert!(
            !off.seed_ingress_class,
            "topology A: seed must be gated off"
        );
        let on = Controller::new(
            sc,
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(1),
            true,
            ControllerHooks::default(),
        )
        .unwrap();
        assert!(on.seed_ingress_class, "topology B: seed must be gated on");
    }

    #[test]
    fn new_wires_default_hooks_as_noops() {
        // ORA3-MAJ-1: a controller built with `ControllerHooks::default()` must accept and
        // store the hooks (all `None` → the control plane runs with no observability
        // callback); the gateway is the only caller that wires real closures.
        let sc = shared(ConfigData::default());
        let c = Controller::new(
            sc,
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(1),
            false,
            ControllerHooks::default(),
        )
        .unwrap();
        assert!(c.hooks.on_watch_error.is_none());
        assert!(c.hooks.on_snapshot_store.is_none());
    }

    #[test]
    fn controller_hooks_invoke_wired_callbacks() {
        // ORA3-MAJ-1: the hooks are plain `Fn` callbacks — exercise the wiring contract the
        // gateway relies on (kind/class for watcher errors, a bare call for store events)
        // without a cluster (the call sites inside `run`/`sync_once` need a live apiserver).
        use std::sync::atomic::AtomicUsize;
        use std::sync::Mutex as StdMutex;
        let errors: Arc<StdMutex<Vec<(String, String)>>> = Arc::default();
        let stores = Arc::new(AtomicUsize::new(0));
        let e2 = errors.clone();
        let s2 = stores.clone();
        let hooks = ControllerHooks {
            on_watch_error: Some(Arc::new(move |kind: &'static str, class: &'static str| {
                e2.lock()
                    .unwrap()
                    .push((kind.to_string(), class.to_string()));
            })),
            on_snapshot_store: Some(Arc::new(move || {
                s2.fetch_add(1, Ordering::Relaxed);
            })),
        };
        let sc = shared(ConfigData::default());
        let c = Controller::new(
            sc,
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(1),
            false,
            hooks,
        )
        .unwrap();
        let (h_err, h_store) = (
            c.hooks.on_watch_error.as_ref().unwrap(),
            c.hooks.on_snapshot_store.as_ref().unwrap(),
        );
        h_err("configmap", "permanent");
        h_err("ingress", "transient");
        h_store();
        h_store();
        let seen = errors.lock().unwrap();
        assert_eq!(
            *seen,
            vec![
                ("configmap".to_string(), "permanent".to_string()),
                ("ingress".to_string(), "transient".to_string()),
            ]
        );
        assert_eq!(stores.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn dirty_flag_debounces_a_burst_of_events() {
        // P4/1.1 debounce: the main loop reconciles on `dirty.swap(false)`. A burst of watch
        // events (each a `store(true)`) must collapse into exactly one reconcile, then stay
        // clean until the next event. This is the exact mechanism used in `run` (no kube mock
        // needed) — the rv-fingerprint short-circuit inside `sync_once` then no-ops if the
        // burst turned out to leave the snapshot unchanged.
        let dirty = AtomicBool::new(false);
        // No events yet → no reconcile.
        assert!(!dirty.swap(false, Ordering::Relaxed));
        // A burst of events (e.g. an `Init` + several `Apply`s) → exactly one reconcile.
        dirty.store(true, Ordering::Relaxed);
        dirty.store(true, Ordering::Relaxed);
        dirty.store(true, Ordering::Relaxed);
        assert!(
            dirty.swap(false, Ordering::Relaxed),
            "a dirty burst must trigger one reconcile"
        );
        assert!(
            !dirty.swap(false, Ordering::Relaxed),
            "no further reconcile until the next event"
        );
        // The next event triggers the next reconcile.
        dirty.store(true, Ordering::Relaxed);
        assert!(dirty.swap(false, Ordering::Relaxed));
    }
}
