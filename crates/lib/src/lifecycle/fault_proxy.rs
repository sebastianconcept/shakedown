//! Kafka bootstrap rewrite through Toxiproxy for faulty-scenario runs.
//!
//! This module is useful when a faulty scenario must talk to Kafka through a proxy listen address.
//! This module rewrites Kafka handle metadata to the proxy listen address when both Kafka and
//! Toxiproxy are registered. This module does not apply toxics; the scenario body drives those.

use crate::error::{InfrastructureError, ResourceHandle};
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
use crate::resources::{BuiltinResource, KafkaBindings, RegisteredResource};

use super::context::Context;
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
use super::environment::ScenarioKind;

#[cfg(feature = "fault-injection")]
pub(crate) fn toxiproxy_api_url_from_handles(handles: &[ResourceHandle]) -> Option<String> {
    for handle in handles {
        let metadata = handle.metadata.as_ref()?;
        let engine = metadata.get("engine")?.as_str()?;
        if engine != "toxiproxy" {
            continue;
        }
        if let Some(url) = metadata.get("api_url").and_then(|value| value.as_str()) {
            return Some(url.to_string());
        }
        if !handle.connection_string.is_empty() {
            return Some(handle.connection_string.clone());
        }
    }
    None
}

#[cfg(feature = "fault-injection")]
pub(crate) fn install_fault_handle_if_toxiproxy_provisioned(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
    let Some(api_url) = toxiproxy_api_url_from_handles(handles) else {
        return;
    };
    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    ctx.set_toxiproxy_control_api_base_url(Some(api_url.clone()));
    if ctx.fault_handle().is_none() {
        ctx.set_fault_handle(Some(crate::fault_injection::FaultHandle::new(api_url)));
    }
}

/// Stable Toxiproxy proxy name for the suite Kafka broker (must match fault-injection wiring and tests).
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
pub(crate) const KAFKA_FAULT_PROXY_NAME: &str = "shakedown_kafka";

#[cfg(all(feature = "fault-injection", feature = "kafka"))]
pub(crate) fn is_kafka_provision_metadata(meta: &serde_json::Value) -> bool {
    meta.get("bootstrap_servers")
        .and_then(|v| v.as_str())
        .is_some()
        && meta.get("topic_prefix").and_then(|v| v.as_str()).is_some()
}

#[cfg(all(feature = "fault-injection", feature = "kafka"))]
pub(crate) fn context_registers_kafka_and_toxiproxy(ctx: &Context) -> bool {
    let mut has_kafka = false;
    let mut has_toxiproxy = false;
    for registered in ctx.resources() {
        match registered {
            RegisteredResource::Builtin(BuiltinResource::Kafka(_)) => has_kafka = true,
            RegisteredResource::Builtin(BuiltinResource::Toxiproxy(_)) => has_toxiproxy = true,
            _ => {}
        }
    }
    has_kafka && has_toxiproxy
}

#[cfg(all(feature = "fault-injection", feature = "kafka"))]
pub(crate) fn update_kafka_handle_bootstrap(handle: &mut ResourceHandle, proxied_bootstrap: &str) {
    handle.connection_string = proxied_bootstrap.to_string();
    if let Some(meta) = handle.metadata.as_mut() {
        if let Some(map) = meta.as_object_mut() {
            map.insert(
                "bootstrap_servers".to_string(),
                serde_json::Value::String(proxied_bootstrap.to_string()),
            );
        }
    }
}

/// When [`ScenarioKind::Faulty`] and both Kafka and Toxiproxy are registered, rewrites Kafka
/// bootstrap metadata to the Toxiproxy listen address (pass-through proxy) before bindings apply.
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
pub(crate) async fn apply_fault_injection_proxy_rewrites(
    ctx: &mut Context,
    handles: &mut [ResourceHandle],
) -> Result<(), InfrastructureError> {
    if let Some(url) = toxiproxy_api_url_from_handles(handles) {
        ctx.set_toxiproxy_control_api_base_url(Some(url));
    }

    let api_url = ctx
        .toxiproxy_control_api_base_url()
        .map(std::string::ToString::to_string)
        .or_else(|| toxiproxy_api_url_from_handles(handles));

    if ctx.scenario_kind() != ScenarioKind::Faulty || !context_registers_kafka_and_toxiproxy(ctx) {
        ctx.clear_kafka_fault_injection_deferred_state();
        return Ok(());
    }

    let Some(api_url) = api_url else {
        for handle in handles.iter_mut() {
            let Some(meta) = handle.metadata.as_ref() else {
                continue;
            };
            if !is_kafka_provision_metadata(meta) {
                continue;
            }
            let topic_prefix = meta
                .get("topic_prefix")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let direct_bootstrap = meta
                .get("bootstrap_servers")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            if let (Some(tp), Some(bs)) = (topic_prefix, direct_bootstrap) {
                ctx.set_kafka_fault_proxy_pending(Some((tp, bs)));
                ctx.set_kafka_bindings_apply_deferred(true);
            }
        }
        return Ok(());
    };

    if ctx.fault_handle().is_none() {
        ctx.set_fault_handle(Some(crate::fault_injection::FaultHandle::new(
            api_url.clone(),
        )));
    }

    if let Some((topic_prefix, direct_bootstrap)) = ctx.take_kafka_fault_proxy_pending() {
        ctx.set_kafka_bindings_apply_deferred(false);
        let fault_handle = ctx.fault_handle_mut().ok_or_else(|| {
            InfrastructureError::FaultInjection(crate::fault_injection::Error::NotAvailable)
        })?;
        if fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .is_none()
        {
            fault_handle
                .create_proxy(
                    KAFKA_FAULT_PROXY_NAME,
                    "127.0.0.1:0",
                    direct_bootstrap.as_str(),
                )
                .await
                .map_err(InfrastructureError::FaultInjection)?;
        }
        let listen = fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .ok_or_else(
                || crate::fault_injection::Error::ListenAddressMissingAfterProxyCreate {
                    proxy_name: KAFKA_FAULT_PROXY_NAME.to_string(),
                },
            )
            .map_err(InfrastructureError::FaultInjection)?
            .to_string();
        ctx.set_kafka_bindings(KafkaBindings::new(listen.as_str(), topic_prefix.as_str()));
    }

    for handle in handles.iter_mut() {
        let Some(meta) = handle.metadata.as_ref() else {
            continue;
        };
        if !is_kafka_provision_metadata(meta) {
            continue;
        }
        let direct_bootstrap = meta
            .get("bootstrap_servers")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let Some(direct_bootstrap) = direct_bootstrap else {
            continue;
        };
        let fault_handle = ctx.fault_handle_mut().ok_or_else(|| {
            InfrastructureError::FaultInjection(crate::fault_injection::Error::NotAvailable)
        })?;
        if fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .is_none()
        {
            fault_handle
                .create_proxy(
                    KAFKA_FAULT_PROXY_NAME,
                    "127.0.0.1:0",
                    direct_bootstrap.as_str(),
                )
                .await
                .map_err(InfrastructureError::FaultInjection)?;
        }
        let listen = fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .ok_or_else(
                || crate::fault_injection::Error::ListenAddressMissingAfterProxyCreate {
                    proxy_name: KAFKA_FAULT_PROXY_NAME.to_string(),
                },
            )
            .map_err(InfrastructureError::FaultInjection)?
            .to_string();
        update_kafka_handle_bootstrap(handle, listen.as_str());
    }

    Ok(())
}
