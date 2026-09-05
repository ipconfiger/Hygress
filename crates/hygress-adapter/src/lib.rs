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

use std::sync::{Arc, Mutex};

use snapshot::SnapshotFingerprint;
pub use client::Client;
pub use error::{Error, Result};
pub use translate::{Object, ObjectKind};

/// Mirror-ingress name env var (GPUStack `GATEWAY_MIRROR_INGRESS_NAME`, design §2.1.1 / §4.3).
const MIRROR_NAME_ENV: &str = "GATEWAY_MIRROR_INGRESS_NAME";

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
    /// 4. Poll every `poll_interval`: full LIST → translate → `SharedConfig::store`; keep
    ///    last-known-good on any failure; fire [`Controller::ready`] after the first successful
    ///    store.
    ///
    /// Returns `Err` only on connect/discovery failure (the per-poll loop never aborts on a
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

        // Pin the opaque shutdown future so `select!` can poll it (it isn't guaranteed `Unpin`).
        let mut shutdown = Box::pin(shutdown);
        loop {
            self.sync_once(&client).await;

            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!("adapter: shutdown received; stopping poll loop");
                    break;
                }
                () = tokio::time::sleep(self.poll_interval) => {}
            }
        }
        Ok(())
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
    /// last-known-good on failure. When the fingerprint is unchanged since the last tick, the
    /// expensive translate + store (and the downstream RouteTable/regex rebuild) are skipped
    /// entirely (P4). The fingerprint is always advanced after a successful LIST (changed or
    /// not); a LIST failure leaves it untouched and retries next tick.
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
                // retry next tick.
                tracing::warn!("snapshot LIST failed; keeping last-known-good: {e}");
                return;
            }
        };

        // Always record the fingerprint we just LISTed (changed or not) so the next tick
        // compares against the latest state.
        *self.lock_fingerprint() = Some(fp);

        // Unchanged since the last pass: skip the translate + store entirely.
        let Some(data) = data else {
            tracing::debug!("snapshot unchanged (fingerprint match); skipping rebuild");
            return;
        };

        match self.shared.store(data) {
            Ok(()) => {
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
}
