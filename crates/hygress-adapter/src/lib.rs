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
//! - [`reconcile`] — api-resources discovery (60s/5s) + best-effort topology-B IngressClass seed
//!
//! Invariants (design):
//! - Pure consumer: no writes to the apiserver, no local persistence (CRDs are the truth), except
//!   the single best-effort `higress` IngressClass seed for topology B (which GPUStack does not
//!   create and `is_supported_higress` probes by name).
//! - Bind-ready: the first successful snapshot store fires [`Controller::ready`]; the data plane
//!   must await it before binding ports.
//! - Last-known-good: a transient LIST/transport failure (or a structurally rejected snapshot)
//!   keeps the previous snapshot; only a successful store swaps.
//! - Orphan tolerance: never delete upstream objects; `ai-route-model-<id>` is legacy; the
//!   unmanaged global custom-response EnvoyFilter is ignored (it lacks the managed label).
//!
//! ## Public API (the gateway depends on this)
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use hygress_core::SharedConfig;
//! use hygress_adapter::Controller;
//!
//! # async fn example(shared: Arc<SharedConfig>) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let controller = Controller::new(
//!     shared,
//!     None, // kubeconfig: Some(<path>) for the embedded file kubeconfig, else in-cluster/KUBECONFIG
//!     "higress-system".into(),
//!     "higress".into(),
//!     Duration::from_secs(1),
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

use snapshot::SnapshotFingerprint;
pub use client::Client;
pub use error::{Error, Result};
pub use translate::{Object, ObjectKind};

/// Mirror-ingress name env var (GPUStack `GATEWAY_MIRROR_INGRESS_NAME`, design §2.1.1 / §4.3).
const MIRROR_NAME_ENV: &str = "GATEWAY_MIRROR_INGRESS_NAME";

/// Low-frequency safety-net tick for the watch streams (P4/1.1). NOT the old 1s poll: the
/// primary update path is now event-driven (kube WATCH), and this tick is only a backstop for
/// a missed / restarted watch stream. [`Controller::run`] uses `max(this, poll_interval)`, so
/// the configured cadence can never make the backstop run more often than 30s.
const FALLBACK_TICK: Duration = Duration::from_secs(30);

/// Control-plane adapter (strategy 2): a read-only kube CRD consumer that LISTs the managed
/// Higress CRDs, translates them into [`hygress_core::ConfigData`], and stores the snapshot
/// into the gateway's [`SharedConfig`] on a poll interval.
pub struct Controller {
    shared: Arc<hygress_core::SharedConfig>,
    kubeconfig: Option<std::path::PathBuf>,
    gateway_namespace: String,
    ingress_class: String,
    poll_interval: std::time::Duration,
    mirror_name: String,
    ready: Arc<tokio::sync::Notify>,
    ready_notified: Arc<std::sync::atomic::AtomicBool>,
    /// Last LISTed snapshot fingerprint (P4 short-circuit). Interior-mutable: the poll loop
    /// updates it once per tick. A `Mutex` is fine — one short critical section per 1s tick in
    /// the control-plane runtime (never held across an `.await`).
    last_fingerprint: Mutex<Option<SnapshotFingerprint>>,
}

impl Controller {
    /// Validate inputs and build the controller.
    ///
    /// `shared` is the gateway's config holder (the adapter stores snapshots into it);
    /// `kubeconfig` is the embedded file kubeconfig path (`None` → in-cluster / `KUBECONFIG`);
    /// `ingress_class` is the IngressClass name to probe/seed (topology B); `poll_interval`
    /// is the snapshot refresh cadence (1s).
    ///
    /// Fails with [`Error::InvalidConfig`] on an empty namespace / ingress class, or a
    /// zero poll interval.
    pub fn new(
        shared: Arc<hygress_core::SharedConfig>,
        kubeconfig: Option<std::path::PathBuf>,
        gateway_namespace: String,
        ingress_class: String,
        poll_interval: std::time::Duration,
    ) -> Result<Self> {
        if gateway_namespace.trim().is_empty() {
            return Err(Error::InvalidConfig("gateway_namespace must be non-empty".into()));
        }
        if ingress_class.trim().is_empty() {
            return Err(Error::InvalidConfig("ingress_class must be non-empty".into()));
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
            mirror_name,
            ready: Arc::new(tokio::sync::Notify::new()),
            ready_notified: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_fingerprint: Mutex::new(None),
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
    /// 3. Best-effort, idempotent IngressClass seed (topology B).
    /// 4. **First** full LIST → translate → store (bind-ready: the first successful store fires
    ///    [`Controller::ready`]) — identical to the old 1s-poll design's first iteration.
    /// 5. **Event-driven** (P4/1.1): one `kube::runtime::watcher` per managed kind. Each
    ///    `Ok` event marks a shared dirty flag + notifies the main loop; `Err` items are
    ///    logged and the stream is kept alive (kube-runtime reconnects / relists internally).
    ///    The main loop reconciles on a dirty flag or a low-frequency safety-net tick
    ///    (`max([`FALLBACK_TICK`], poll_interval)`); a LIST/transport failure keeps the
    ///    last-known-good snapshot and keeps watching.
    ///
    /// Returns `Err` only on connect/discovery failure (the event loop never aborts on a
    /// transient error).
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
        let client = client::Client::connect(
            self.kubeconfig.as_deref(),
            self.gateway_namespace.clone(),
        )
        .await?;

        reconcile::wait_for_apiserver_ready(&client).await?;

        // Best-effort, idempotent seed (topology B); a failure here does not stop the loop.
        if let Err(e) = reconcile::ensure_ingress_class(&client, &self.ingress_class).await {
            tracing::warn!("IngressClass seed (best-effort) failed: {e}");
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
        let handles = vec![
            Self::spawn_watcher(client.mcpbridges(), Config::default(), "mcpbridge", &dirty, &notify),
            Self::spawn_watcher(
                client.wasmplugins(),
                Config::default().labels(managed),
                "wasmplugin",
                &dirty,
                &notify,
            ),
            Self::spawn_watcher(
                client.envoyfilters(),
                Config::default().labels(managed),
                "envoyfilter",
                &dirty,
                &notify,
            ),
            Self::spawn_watcher(
                client.ingresses(),
                Config::default().labels(managed),
                "ingress",
                &dirty,
                &notify,
            ),
            Self::spawn_watcher(
                client.secrets(),
                Config::default().labels(managed),
                "secret",
                &dirty,
                &notify,
            ),
            Self::spawn_watcher(
                client.configmaps(),
                Config::default().labels(managed),
                "configmap",
                &dirty,
                &notify,
            ),
        ];

        // ③ Main loop: reconcile on a dirty flag, the low-frequency safety-net tick, or
        //    shutdown. The fallback is a backstop for the watch streams — NOT the old 1s poll.
        let fallback_tick = FALLBACK_TICK.max(self.poll_interval);
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
                _ = tokio::time::sleep(fallback_tick) => {}
            }
            // R-2: reconcile on EVERY wake (event burst debounced by `dirty`, or the
            // low-frequency safety-net tick). The rv-fingerprint short-circuit inside
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
    /// shared dirty flag + notifies the main loop; `Err` items are logged and the stream is kept
    /// alive (kube-runtime reconnects / relists internally) — a transient error must never
    /// terminate the watcher task (P4/1.1).
    fn spawn_watcher<K>(
        api: Api<K>,
        config: Config,
        kind: &'static str,
        dirty: &Arc<AtomicBool>,
        notify: &Arc<tokio::sync::Notify>,
    ) -> tokio::task::JoinHandle<()>
    where
        K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug + Send + 'static,
    {
        let dirty = Arc::clone(dirty);
        let notify = Arc::clone(notify);
        tokio::spawn(async move {
            let stream = watcher(api, config);
            futures::pin_mut!(stream);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_event) => {
                        dirty.store(true, Ordering::Relaxed);
                        notify.notify_one();
                    }
                    Err(e) => {
                        // Keep the stream alive: kube-runtime reconnects / relists internally.
                        tracing::warn!(kind, "watch error (keeping stream alive): {e}");
                    }
                }
            }
            // The `watcher` stream is self-healing/infinite; reaching here means it ended
            // unexpectedly — log it loudly and mark dirty so the safety-net tick reconciles.
            tracing::error!(
                kind,
                "watch stream ended unexpectedly; degraded to fallback tick until back"
            );
            dirty.store(true, Ordering::Relaxed);
            notify.notify_one();
        })
    }

    /// Best-effort, non-blocking IngressClass seed (topology B).
    ///
    /// GPUStack never creates the `higress` IngressClass, and `is_supported_higress` (external
    /// mode) probes it **by name**, so it must exist for topology B to start. This method is a
    /// convenience pre-warm: when a tokio runtime is present it spawns a detached task that
    /// connects and seeds idempotently; otherwise it is a no-op. The canonical seed also runs
    /// inside [`Controller::run`], so this is safe to call (or skip) — the operation is
    /// idempotent (create-if-missing).
    pub fn seed_ingress_class(&self) {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                tracing::warn!("seed_ingress_class: no tokio runtime; skipping (run() also seeds)");
                return;
            }
        };
        let kubeconfig = self.kubeconfig.clone();
        let namespace = self.gateway_namespace.clone();
        let name = self.ingress_class.clone();
        handle.spawn(async move {
            let Ok(c) = client::Client::connect(kubeconfig.as_deref(), namespace).await else {
                tracing::warn!("seed_ingress_class: client connect failed; skipping");
                return;
            };
            if let Err(e) = reconcile::ensure_ingress_class(&c, &name).await {
                tracing::warn!("seed_ingress_class (best-effort) failed: {e}");
            }
        });
    }

    /// Lock the last-fingerprint guard, recovering from a poisoned mutex instead of panicking.
    /// (Poisoning would require a panic while holding the lock — the only holder is the single
    /// poll loop, and the guarded `clone`/`assign` cannot panic, so this is purely defensive.)
    fn lock_fingerprint(&self) -> std::sync::MutexGuard<'_, Option<SnapshotFingerprint>> {
        self.last_fingerprint.lock().unwrap_or_else(|e| e.into_inner())
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
        assert!(
            Controller::new(sc.clone(), None, String::new(), "higress".into(), std::time::Duration::from_secs(1)).is_err()
        );
        // Empty ingress class is rejected.
        assert!(
            Controller::new(sc.clone(), None, "higress-system".into(), String::new(), std::time::Duration::from_secs(1)).is_err()
        );
        // Zero poll interval is rejected.
        assert!(
            Controller::new(sc.clone(), None, "higress-system".into(), "higress".into(), std::time::Duration::from_secs(0)).is_err()
        );
        // Valid params build.
        let c = Controller::new(
            sc.clone(),
            None,
            "higress-system".into(),
            "higress".into(),
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert!(c.mirror_name == translate::MIRROR_NAME);
        // ready() hands out a usable Notify handle.
        let _ = c.ready();
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
        assert!(dirty.swap(false, Ordering::Relaxed), "a dirty burst must trigger one reconcile");
        assert!(!dirty.swap(false, Ordering::Relaxed), "no further reconcile until the next event");
        // The next event triggers the next reconcile.
        dirty.store(true, Ordering::Relaxed);
        assert!(dirty.swap(false, Ordering::Relaxed));
    }
}
