//! Toxiproxy control-plane helpers for fault-injection scenarios (`fault-injection` feature).
//!
//! Proving that a service handles network failures correctly — broker timeouts,
//! slow downstream APIs, connection resets — requires injecting those faults at the network layer
//! without modifying the service under test. Shakedown owns the Toxiproxy integration directly
//! so fault semantics are easy to setup, reliable and version-controlled alongside your scenarios.
//!
//! Register a [`crate::resources::ToxiproxyResource`] on your [`crate::lifecycle::Context`]
//! and use [`FaultHandle`] inside `#[shakedown::faulty_test]` to add toxics, reset proxies, and
//! remove faults. Shakedown installs the handle automatically when Toxiproxy provisions
//! successfully.
//!
//! Toxic add/remove and proxy reset enqueue [`crate::event::Event`] values on an internal
//! channel. Shakedown drains that channel into the caller-owned [`crate::event::EventLog`] after
//! the scenario body so failure timelines correlate faults with infrastructure and harness signals.
//!
//! # Example
//!
//! ```rust,ignore
//! #[shakedown::faulty_test(resources = [Toxiproxy])]
//! async fn inject_latency(ctx: &mut shakedown::Context) {
//!     let faults = ctx.fault_handle_mut().expect("FaultHandle after Toxiproxy provisions");
//!     faults
//!         .create_proxy("edge", "127.0.0.1:0", "127.0.0.1:9092")
//!         .await
//!         .expect("create proxy");
//!     let toxic = shakedown::Toxic {
//!         kind: shakedown::ToxicKind::Latency {
//!             latency: 200,
//!             jitter: 0,
//!         },
//!         name: "lag".to_string(),
//!         toxicity: 1.0,
//!         direction: shakedown::StreamDirection::Downstream,
//!     };
//!     faults.add_toxic("edge", &toxic).await.expect("add toxic");
//!     // ... exercise the system through the proxy ...
//!     faults.remove_toxic("edge", "lag").await.expect("remove toxic");
//! }
//! ```
//!
//! ## Stream direction ([`StreamDirection`])
//!
//! Toxiproxy splits each proxied connection into two named streams (same names as the HTTP API).
//! Re-exported from `noxious`, the variants mean:
//!
//! - **[`StreamDirection::Upstream`]** — toxics apply to **client → upstream** traffic (from the
//!   process connecting to the proxy’s listen address **toward** the `upstream` target you passed to
//!   [`FaultHandle::create_proxy`]). Use this to stress **outbound** behaviour (slow sends, timeouts on
//!   the request path).
//! - **[`StreamDirection::Downstream`]** — toxics apply to **upstream → client** traffic (responses
//!   and data coming **back** from the dependency). Use this to stress **inbound** behaviour (slow
//!   reads, broker lag visible to the client).
//!
//! Pick the stream that matches the failure you are simulating; symmetric toxics such as latency
//! still only affect one direction at a time unless you add two toxics.
//!
//! ## Composition with resources and the service under test
//!
//! There is **more than one way** to put traffic through Toxiproxy; what the harness does for you
//! depends on the resource and on [`crate::lifecycle::ScenarioKind`].
//!
//! ### Where wiring happens
//!
//! During **[`crate::lifecycle::run_scenario`]** provision, after **each** resource finishes
//! provisioning, the lifecycle may rewrite resource handles and [`crate::lifecycle::Context`]
//! bindings before the next resource runs. When the **`kafka`** and **`fault-injection`** features
//! are enabled, Kafka bootstrap URLs can be rewritten to a pass-through proxy listen address so
//! every later consumer of [`crate::lifecycle::Context::kafka_bindings`] — including service
//! container env mapping when you use [`crate::service::EnvMapping::Kafka`] — sees the **proxied**
//! bootstrap, not the broker’s direct address.
//!
//! [`FaultHandle`] is installed when a [`crate::resources::ToxiproxyResource`] provision yields
//! handles (so you can call [`crate::lifecycle::Context::fault_handle_mut`] in the scenario body).
//!
//! ### Automatic Kafka path (first-party harness wiring)
//!
//! This is **not** a sample pattern you copy from the docs — the lifecycle **implements** it when
//! the features and resources line up (proxy name, handle rewrites, and bindings updates are all
//! in-tree). You use the same [`FaultHandle::add_toxic`] API as for any other proxy; the special
//! part is that you **did not** call [`FaultHandle::create_proxy`] for `shakedown_kafka` yourself.
//!
//! When **all** of the following hold:
//!
//! - [`crate::lifecycle::ScenarioKind::Faulty`] (use `#[shakedown::faulty_test]`),
//! - both **Kafka** and **Toxiproxy** are registered on the context, and
//! - the `kafka` + `fault-injection` features are on,
//!
//! the harness creates a Toxiproxy proxy named **`shakedown_kafka`** (listen on an ephemeral port,
//! upstream set to the broker’s real bootstrap). It updates Kafka [`crate::error::ResourceHandle`]
//! metadata and scenario [`crate::resources::KafkaBindings`] so the bootstrap string is the **proxy
//! listen** address. Your **service under test** does not need a special image: inject Kafka with
//! [`crate::service::EnvMapping::Kafka`] as usual; it receives the proxied URL like any other
//! scenario. Add toxics on **`shakedown_kafka`** via [`FaultHandle::add_toxic`].
//!
//! If Kafka provisions **before** Toxiproxy in registration order, bindings may be **deferred** until
//! Toxiproxy appears; the harness fills in the proxy once both exist.
//!
//! ### Normal scenarios, or Kafka without a rewrite
//!
//! With [`crate::lifecycle::ScenarioKind::Normal`], or if Toxiproxy is absent, Kafka keeps the
//! **direct** broker bootstrap (even if Toxiproxy is registered for other use). See
//! `tests/fault_injection_kafka.rs` (`normal_kafka_bootstrap_stays_direct_with_toxiproxy_registered`).
//!
//! ### Bring-your-own proxy (manual composition)
//!
//! For dependencies **without** a built-in rewrite (Postgres, HTTP APIs, a second TCP hop, …), or
//! when you want a custom topology, call [`FaultHandle::create_proxy`] yourself and point the
//! service at the proxy **listen** address via explicit [`crate::service::ServiceUnderTest::env`]
//! (or your own prelude). That is the **same Toxiproxy mechanism** as Kafka, but **you** own proxy
//! creation and URL injection instead of the harness.
//!
//! ### MongoDB replica set (cluster)
//!
//! With **`mongodb-cluster`** and fault injection, Shakedown can build an **inter-node Toxiproxy
//! mesh** between replica-set members (see [`crate::cluster`] and the Mongo replica set wiring).
//! That is a **separate** composition from the Kafka bootstrap rewrite; you may use one, the other,
//! or both depending on resources registered.
//!
//! ### Diagram (Kafka fault path)
//!
//! ```text
//!   Client (SUT, tests, helpers)          Toxiproxy suite          Kafka broker
//!
//!        connect to bootstrap          ┌──────────────────┐       ┌─────────────┐
//!        from kafka_bindings()    ──►  │listen (ephemeral)│ ───►  │ upstream    │
//!        or EnvMapping::Kafka          │ proxy shakedown_ │       │ (real host) │
//!                                      │ kafka            │       └─────────────┘
//!                                      └──────────────────┘
//!                                                 ▲
//!                                    toxics via FaultHandle on this proxy name
//! ```
//!
//! # Book reference
//!
//! Part IV, *Chapter 17: Fault Injection* — covers `#[shakedown::faulty_test]`, the Toxiproxy
//! resource lifecycle, and patterns for asserting graceful degradation under network faults.

use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use displaydoc::Display;
use noxious_client::Client as ToxiproxyClient;

/// Which half of the proxied connection a toxic applies to; see **[`crate::fault_injection`]** for
/// when to use [`StreamDirection::Upstream`] vs [`StreamDirection::Downstream`].
pub use noxious_client::StreamDirection;
pub use noxious_client::{Toxic, ToxicKind};

use crate::event::Event;

/// `source` field on [`Event`] records emitted by [`FaultHandle`] for timeline filtering.
pub const FAULT_INJECTION_EVENT_SOURCE: &str = "fault-injection";

/// Capacity of the [`std::sync::mpsc::sync_channel`] between [`FaultHandle`] and
/// [`crate::lifecycle::run_scenario`].
///
/// Fault-injection logging is low volume; this only bounds memory until the harness drains the
/// receiver after the scenario body. When full, [`std::sync::mpsc::SyncSender::try_send`] fails and
/// that event is skipped.
pub(crate) const FAULT_EVENT_CHANNEL_CAPACITY: usize = 64;

/// Fault-injection failures (Toxiproxy control API and proxy registry).
#[derive(Display, Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    /// Failed to create Toxiproxy proxy: {0}
    ProxyCreationFailed(#[source] noxious_client::error::ClientError),
    /// Toxic operation failed: {0}
    ToxicOperationFailed(#[source] noxious_client::error::ClientError),
    /// Fault injection is not available (feature disabled or Toxiproxy not provisioned).
    NotAvailable,
    /// Proxy not found: {proxy_name}
    ProxyNotFound { proxy_name: String },
    /// Kafka fault proxy was deferred but Toxiproxy did not provision (register Toxiproxy in the same scenario).
    KafkaFaultProxyIncomplete,
    /// Toxiproxy proxy `{proxy_name}` was created but listen address was not recorded (framework invariant).
    ListenAddressMissingAfterProxyCreate { proxy_name: String },
}

/// Handle for controlling Toxiproxy proxies and toxics for one scenario run.
///
/// Wraps the Toxiproxy HTTP client, per-proxy listen/upstream bookkeeping, and an optional
/// channel sender for fault [`Event`] rows.
///
/// Scenarios call `add_toxic` / `remove_toxic` without touching raw HTTP; events must reach
/// the scenario [`crate::event::EventLog`] without `unsafe` pointers to caller-owned memory.
///
/// Constructed by Shakedown with [`FaultHandle::new`]; before the run phase,
/// [`FaultHandle::set_event_sender`] wires the sender half of an `mpsc` pair. Shakedown drains the
/// receiver into the log after the scenario body. [`crate::lifecycle::Context::clear_fault_handle`]
/// drops the handle at teardown.
#[cfg_attr(docsrs, doc(cfg(feature = "fault-injection")))]
#[derive(Debug)]
pub struct FaultHandle {
    client: ToxiproxyClient,
    api_base_url: String,
    proxy_listen_addresses: HashMap<String, String>,
    /// Upstream address per proxy (for event messages).
    proxy_upstreams: HashMap<String, String>,
    /// Sender for fault [`Event`] rows; set by [`crate::lifecycle::run_scenario`] before the scenario body.
    event_sender: Option<SyncSender<Event>>,
}

impl FaultHandle {
    /// Build a handle for the given Toxiproxy HTTP API base URL (e.g. `http://127.0.0.1:8474`).
    pub fn new(api_base_url: impl Into<String>) -> Self {
        let api_base_url = api_base_url.into();
        let client = ToxiproxyClient::new(&api_base_url);
        Self {
            client,
            api_base_url,
            proxy_listen_addresses: HashMap::new(),
            proxy_upstreams: HashMap::new(),
            event_sender: None,
        }
    }

    /// Toxiproxy control API base URL passed to [`FaultHandle::new`].
    pub fn api_base_url(&self) -> &str {
        self.api_base_url.as_str()
    }

    /// Wire the sender half of an `mpsc` pair so fault operations enqueue [`Event`] rows for the harness to drain.
    ///
    /// [`crate::lifecycle::run_scenario`] calls this immediately before the scenario body when a handle exists.
    pub(crate) fn set_event_sender(&mut self, sender: SyncSender<Event>) {
        self.event_sender = Some(sender);
    }

    /// Create a named proxy (`listen` / `upstream` in Toxiproxy address form, e.g. `127.0.0.1:0`).
    ///
    /// On success, the resolved listen address is recorded for [`FaultHandle::proxy_listen_address`].
    pub async fn create_proxy(
        &mut self,
        proxy_name: &str,
        listen: &str,
        upstream: &str,
    ) -> Result<(), Error> {
        let proxy = self
            .client
            .create_proxy(proxy_name, listen, upstream)
            .await
            .map_err(Error::ProxyCreationFailed)?;
        self.proxy_listen_addresses
            .insert(proxy_name.to_string(), proxy.config.listen.clone());
        self.proxy_upstreams
            .insert(proxy_name.to_string(), upstream.to_string());
        Ok(())
    }

    /// Add a toxic to an existing proxy (proxy must exist — typically via [`FaultHandle::create_proxy`]).
    pub async fn add_toxic(&self, proxy_name: &str, toxic: &Toxic) -> Result<(), Error> {
        self.require_known_proxy(proxy_name)?;
        let proxy = self
            .client
            .proxy(proxy_name)
            .await
            .map_err(Error::ToxicOperationFailed)?;
        proxy
            .add_toxic(toxic)
            .await
            .map_err(Error::ToxicOperationFailed)?;
        let upstream = self.upstream_label(proxy_name);
        let message = format!(
            "action=added proxy='{proxy_name}' upstream={upstream} toxic='{}' kind={} direction={}",
            toxic.name, toxic.kind, toxic.direction
        );
        self.push_fault_event(message);
        Ok(())
    }

    /// Remove a toxic by name from a proxy.
    pub async fn remove_toxic(&self, proxy_name: &str, toxic_name: &str) -> Result<(), Error> {
        self.require_known_proxy(proxy_name)?;
        let proxy = self
            .client
            .proxy(proxy_name)
            .await
            .map_err(Error::ToxicOperationFailed)?;
        proxy
            .remove_toxic(toxic_name)
            .await
            .map_err(Error::ToxicOperationFailed)?;
        let upstream = self.upstream_label(proxy_name);
        let message =
            format!("action=removed proxy='{proxy_name}' upstream={upstream} toxic='{toxic_name}'");
        self.push_fault_event(message);
        Ok(())
    }

    /// Remove all toxics from a proxy (best-effort list + delete).
    pub async fn reset_proxy(&self, proxy_name: &str) -> Result<(), Error> {
        self.require_known_proxy(proxy_name)?;
        let proxy = self
            .client
            .proxy(proxy_name)
            .await
            .map_err(Error::ToxicOperationFailed)?;
        let toxics = proxy.toxics().await.map_err(Error::ToxicOperationFailed)?;
        let removed_count = toxics.len();
        let upstream = self.upstream_label(proxy_name);
        for toxic in toxics {
            proxy
                .remove_toxic(&toxic.name)
                .await
                .map_err(Error::ToxicOperationFailed)?;
        }
        let message = format!(
            "action=reset proxy='{proxy_name}' upstream={upstream} removed_toxics_count={removed_count}"
        );
        self.push_fault_event(message);
        Ok(())
    }

    /// Enable or disable a proxy by name (inter-node mesh / partition simulation).
    ///
    /// Disabling drops active connections through the proxy; enabling restores pass-through.
    pub async fn set_proxy_enabled(&self, proxy_name: &str, enabled: bool) -> Result<(), Error> {
        let mut proxy = self
            .client
            .proxy(proxy_name)
            .await
            .map_err(Error::ToxicOperationFailed)?;
        if enabled {
            proxy.enable().await.map_err(Error::ToxicOperationFailed)?;
        } else {
            proxy.disable().await.map_err(Error::ToxicOperationFailed)?;
        }
        let upstream = self
            .proxy_upstreams
            .get(proxy_name)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let message = format!(
            "action={} proxy='{proxy_name}' upstream={upstream}",
            if enabled { "enabled" } else { "disabled" }
        );
        self.push_fault_event(message);
        Ok(())
    }

    /// Listen address for a proxy name after successful [`FaultHandle::create_proxy`], if known.
    pub fn proxy_listen_address(&self, proxy_name: &str) -> Option<&str> {
        self.proxy_listen_addresses
            .get(proxy_name)
            .map(|address| address.as_str())
    }

    fn require_known_proxy(&self, proxy_name: &str) -> Result<(), Error> {
        if self.proxy_listen_addresses.contains_key(proxy_name) {
            return Ok(());
        }
        Err(Error::ProxyNotFound {
            proxy_name: proxy_name.to_string(),
        })
    }

    fn upstream_label(&self, proxy_name: &str) -> String {
        self.proxy_upstreams
            .get(proxy_name)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Enqueue a fault-injection [`Event`] for the harness to append after the scenario body.
    fn push_fault_event(&self, message: String) {
        if let Some(sender) = &self.event_sender {
            let _ = sender.try_send(Event::info(FAULT_INJECTION_EVENT_SOURCE, message));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventLevel;
    use std::sync::mpsc::sync_channel;

    // These tests compile only with `fault-injection`; run via
    // `cargo test -p shakedown --features fault-injection` or `--all-features`.

    #[test]
    fn push_fault_event_delivers_when_sender_set() {
        // Given a FaultHandle wired with a channel sender
        let mut handle = FaultHandle::new("http://127.0.0.1:8474");
        let (sender, receiver) = sync_channel(FAULT_EVENT_CHANNEL_CAPACITY);
        handle.set_event_sender(sender);
        let message = "action=test proxy='p' upstream=u";

        // When we push a fault event
        handle.push_fault_event(message.to_string());

        // Then exactly one event arrives with the fault-injection source and message
        let events: Vec<_> = receiver.try_iter().collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, FAULT_INJECTION_EVENT_SOURCE);
        assert_eq!(events[0].level, EventLevel::Info);
        assert_eq!(events[0].message, message);
    }

    #[test]
    fn push_fault_event_no_panic_without_sender() {
        // Given a FaultHandle with no event sender
        let handle = FaultHandle::new("http://127.0.0.1:8474");

        // When we push a fault event
        handle.push_fault_event("noop".to_string());

        // Then the call completes without panic
    }
}
