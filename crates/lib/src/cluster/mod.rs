//! Cluster topology support — shared Docker networks, node control, and engine-specific ops.
//!
//! Requires the **`cluster`** Cargo feature (pulls in [`bollard`] for Docker Engine API calls).

pub mod network;

pub use network::{cluster_network_name, create_cluster_network, remove_cluster_network};

use bollard::errors::Error as BollardError;
use displaydoc::Display;
use thiserror::Error;

/// Failure from cluster topology or Docker network operations.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
pub enum ClusterError {
    /// network management failed
    NetworkError {
        #[source]
        source: BollardError,
    },
}
