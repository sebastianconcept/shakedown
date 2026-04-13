//! Errors for missing named resource bindings on [`crate::lifecycle::Context`] helpers.
//!
//! Call sites live in feature-gated `resources::*/helpers` modules. When this crate is built as a
//! workspace dependency with only a subset of features enabled, this module may contain no
//! references and would otherwise trigger `dead_code` under `-D warnings`.

#![allow(dead_code)]

use crate::error::{InfrastructureError, ShakedownError};

/// Must match [`crate::lifecycle::DEFAULT_RESOURCE_BINDING_NAME`].
const DEFAULT_BINDING_NAME: &str = "default";

/// Build a [`ShakedownError::Infrastructure`] `NotProvisioned` error for a missing named helper binding.
///
/// Preserves the legacy message for the default binding key so existing error strings stay stable;
/// non-default names include the binding key for clarity.
pub(crate) fn not_provisioned_named_binding(
    resource_label: &str,
    binding_name: &str,
) -> ShakedownError {
    let message = if binding_name == DEFAULT_BINDING_NAME {
        resource_label.to_string()
    } else {
        format!("{resource_label} (binding name '{binding_name}')")
    };
    ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(message))
}
