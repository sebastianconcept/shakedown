//! Toxiproxy control-plane helpers for fault-injection scenarios (`fault-injection` feature).
//!
//! [`FaultHandle`] wraps the `noxious-client` API and a small registry of proxy listen addresses.
//! [`crate::lifecycle::run_scenario`] installs it when a [`crate::resources::ToxiproxyResource`]
//! provisions successfully.

use std::collections::HashMap;

use displaydoc::Display;
use noxious_client::Client as ToxiproxyClient;
pub use noxious_client::{StreamDirection, Toxic, ToxicKind};

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
}

/// Handle for controlling Toxiproxy proxies and toxics for one scenario run.
#[derive(Debug)]
pub struct FaultHandle {
    client: ToxiproxyClient,
    api_base_url: String,
    proxy_listen_addresses: HashMap<String, String>,
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
        }
    }

    /// Toxiproxy control API base URL passed to [`FaultHandle::new`].
    pub fn api_base_url(&self) -> &str {
        self.api_base_url.as_str()
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
        for toxic in toxics {
            proxy
                .remove_toxic(&toxic.name)
                .await
                .map_err(Error::ToxicOperationFailed)?;
        }
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
}
