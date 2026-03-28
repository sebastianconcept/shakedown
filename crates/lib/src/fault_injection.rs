//! Toxiproxy control-plane helpers for fault-injection scenarios (`fault-injection` feature).
//!
//! [`FaultHandle`] wraps the `noxious-client` API and a small registry of proxy listen addresses.
//! [`crate::lifecycle::run_scenario`] installs it when a [`crate::resources::ToxiproxyResource`]
//! provisions successfully.
//!
//! Toxic add/remove and proxy reset enqueue [`crate::event::Event`] values on an internal
//! [`std::sync::mpsc::SyncSender`] (set by the harness before the scenario body). The harness
//! drains the matching receiver into the caller-owned [`crate::event::EventLog`] after the scenario
//! body so failure timelines correlate faults with harness and infra signals — without storing raw
//! pointers to the log.

use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use displaydoc::Display;
use noxious_client::Client as ToxiproxyClient;
pub use noxious_client::{StreamDirection, Toxic, ToxicKind};

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
}

/// Handle for controlling Toxiproxy proxies and toxics for one scenario run.
///
/// **What:** Wraps the Toxiproxy HTTP client, per-proxy listen/upstream bookkeeping, and an optional
/// channel sender for fault [`Event`] rows.
///
/// **Why:** Scenarios call `add_toxic` / `remove_toxic` without touching raw HTTP; events must reach
/// the scenario [`crate::event::EventLog`] without `unsafe` pointers to caller-owned memory.
///
/// **How:** Constructed by the harness with [`FaultHandle::new`]; before the run phase,
/// [`FaultHandle::set_event_sender`] wires the sender half of an `mpsc` pair. The harness drains the
/// receiver into the log after the scenario body. [`crate::lifecycle::Context::clear_fault_handle`]
/// drops the handle at teardown.
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
