//! InfluxDB 2.x typed helpers — [`InfluxDbHelpers`] (`influxdb` feature).

use std::error::Error;

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::InfluxDbBindings;

/// HTTP error from an InfluxDB REST API call.
///
/// Captures method, path, status code, and response body so callers and logs have full context
/// when an InfluxDB HTTP request fails (e.g. writing points, running Flux queries).
#[derive(Debug, displaydoc::Display)]
#[ignore_extra_doc_attributes]
pub(crate) struct InfluxDbHttpError {
    /// InfluxDB HTTP error: {method} {path} returned {status} — {body}
    method: String,
    path: String,
    status: reqwest::StatusCode,
    body: String,
}

impl Error for InfluxDbHttpError {}

/// Field value for [`InfluxDbHelpers::write_point`] line-protocol encoding.
#[derive(Debug, Clone)]
pub enum InfluxFieldValue {
    /// IEEE float (`f64`).
    Float(f64),
    /// Signed integer (line protocol `i` suffix).
    Integer(i64),
    /// Unsigned integer (`u` suffix).
    Unsigned(u64),
    /// Quoted string field.
    String(String),
    /// Boolean field.
    Boolean(bool),
}

impl InfluxFieldValue {
    fn format_line_protocol(&self) -> String {
        match self {
            Self::Float(value) => format!("{value}"),
            Self::Integer(value) => format!("{value}i"),
            Self::Unsigned(value) => format!("{value}u"),
            Self::String(value) => {
                let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("\"{escaped}\"")
            }
            Self::Boolean(value) => {
                if *value {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
        }
    }
}

fn escape_tag_component(component: &str) -> String {
    component
        .replace('\\', "\\\\")
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

fn flux_string_literal(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// True when Influx annotated CSV contains at least one data row.
///
/// Newer InfluxDB versions may format table rows slightly differently; we treat any non-comment,
/// non-empty line with enough comma-separated fields as data (after headers).
fn csv_response_has_data_row(csv_body: &str) -> bool {
    csv_body.lines().any(|line| {
        let t = line.trim_start();
        if t.is_empty() || t.starts_with('#') {
            return false;
        }
        if t.starts_with(",result") {
            return false;
        }
        let comma_count = t.matches(',').count();
        comma_count >= 2 && (t.starts_with(',') || comma_count >= 4)
    })
}

/// Facade for InfluxDB 2.x HTTP helpers bound to [`Context`] (`influxdb` feature).
pub struct InfluxDbHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> InfluxDbHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.influxdb_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("influxdb bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn influxdb_bindings(&self) -> Result<&InfluxDbBindings, ShakedownError> {
        self.context
            .influxdb_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("influxdb bindings", self.binding_name.as_str())
            })
    }

    /// `POST /api/v2/write` with line protocol into the scenario bucket (`precision=ns`).
    pub async fn write_point(
        &self,
        measurement: &str,
        tags: &[(&str, &str)],
        fields: &[(&str, InfluxFieldValue)],
        timestamp_nanos: Option<i64>,
    ) -> Result<(), ShakedownError> {
        let bindings = self.influxdb_bindings()?;
        let tag_segment = tags
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    escape_tag_component(key),
                    escape_tag_component(value)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let field_segment = fields
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    escape_tag_component(key),
                    value.format_line_protocol()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let mut line = if tag_segment.is_empty() {
            format!("{measurement} {field_segment}")
        } else {
            format!("{measurement},{tag_segment} {field_segment}")
        };
        if let Some(nanos) = timestamp_nanos {
            line.push(' ');
            line.push_str(&nanos.to_string());
        }

        let write_url = format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ns",
            bindings.url.trim_end_matches('/'),
            bindings.organization,
            bindings.bucket
        );
        let response = reqwest::Client::new()
            .post(&write_url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Token {}", bindings.token),
            )
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(line)
            .send()
            .await
            .map_err(map_backend_error)?;
        if !response.status().is_success() {
            let error = InfluxDbHttpError {
                method: "POST".to_string(),
                path: "/api/v2/write".to_string(),
                status: response.status(),
                body: response.text().await.unwrap_or_default(),
            };
            return Err(ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(error)),
            ));
        }
        Ok(())
    }

    /// `POST /api/v2/query` (Flux); returns the response body (annotated CSV).
    pub async fn query(&self, flux_query: &str) -> Result<String, ShakedownError> {
        let bindings = self.influxdb_bindings()?;
        let query_url = format!(
            "{}/api/v2/query?org={}",
            bindings.url.trim_end_matches('/'),
            bindings.organization
        );
        let response = reqwest::Client::new()
            .post(&query_url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Token {}", bindings.token),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/vnd.flux")
            .header(reqwest::header::ACCEPT, "application/csv")
            .body(flux_query.to_string())
            .send()
            .await
            .map_err(map_backend_error)?;
        if !response.status().is_success() {
            let error = InfluxDbHttpError {
                method: "POST".to_string(),
                path: "/api/v2/query".to_string(),
                status: response.status(),
                body: response.text().await.unwrap_or_default(),
            };
            return Err(ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(error)),
            ));
        }
        response.text().await.map_err(map_backend_error)
    }

    /// Assert at least one point exists for `measurement` with the given tag equalities (Flux `first()`).
    pub async fn assert_point_exists(
        &self,
        measurement: &str,
        tag_filter: &[(&str, &str)],
    ) -> Result<(), ShakedownError> {
        let bindings = self.influxdb_bindings()?;
        let bucket_lit = flux_string_literal(bindings.bucket.as_str());
        let measurement_lit = flux_string_literal(measurement);
        let mut flux = format!(
            "from(bucket: {bucket_lit}) |> range(start: -7d) |> filter(fn: (r) => r._measurement == {measurement_lit})"
        );
        for (tag_key, tag_value) in tag_filter {
            let key_lit = flux_string_literal(tag_key);
            let value_lit = flux_string_literal(tag_value);
            flux.push_str(&format!(
                " |> filter(fn: (r) => r[{key_lit}] == {value_lit})"
            ));
        }
        flux.push_str(" |> first()");
        const RETRIES: u32 = 15;
        const SLEEP_MS: u64 = 200;
        for attempt in 0..RETRIES {
            let csv_body = self.query(flux.as_str()).await?;
            if csv_response_has_data_row(csv_body.as_str()) {
                return Ok(());
            }
            if attempt + 1 == RETRIES {
                return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                    AssertionError::Unspecified {
                        detail: format!(
                            "expected InfluxDB point measurement={measurement:?} tags={tag_filter:?}; Flux result had no data rows"
                        ),
                    },
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(SLEEP_MS)).await;
        }
        unreachable!()
    }
}
