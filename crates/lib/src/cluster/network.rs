//! Create and remove suite-scoped Docker bridge networks for multi-node cluster resources.

use bollard::models::NetworkCreateRequest;
use bollard::Docker;

use super::ClusterError;

/// Build the Docker network name: `shakedown-{engine}-{suite_id}`.
///
/// Callers should pass short, filesystem-style `engine` and `suite_id` segments (alphanumeric,
/// `_`, `-`, `.`) so the name satisfies Docker’s network naming rules.
pub fn cluster_network_name(engine: &str, suite_id: &str) -> String {
    format!("shakedown-{engine}-{suite_id}")
}

/// Create a bridge network for cluster containers, or succeed if it already exists (idempotent).
///
/// Returns the network name (same as [`cluster_network_name`]).
pub async fn create_cluster_network(
    docker: &Docker,
    engine: &str,
    suite_id: &str,
) -> Result<String, ClusterError> {
    let network_name = cluster_network_name(engine, suite_id);
    let config = NetworkCreateRequest {
        name: network_name.clone(),
        driver: Some(if cfg!(windows) {
            String::from("transparent")
        } else {
            String::from("bridge")
        }),
        ..Default::default()
    };

    match docker.create_network(config).await {
        Ok(_) => Ok(network_name),
        Err(source) => {
            if network_already_exists(&source) {
                Ok(network_name)
            } else {
                Err(ClusterError::NetworkError { source })
            }
        }
    }
}

/// Remove a Docker network by name (e.g. the name returned from [`create_cluster_network`]).
pub async fn remove_cluster_network(
    docker: &Docker,
    network_name: &str,
) -> Result<(), ClusterError> {
    docker
        .remove_network(network_name)
        .await
        .map_err(|source| ClusterError::NetworkError { source })
}

fn network_already_exists(source: &bollard::errors::Error) -> bool {
    match source {
        bollard::errors::Error::DockerResponseServerError {
            status_code,
            message,
        } => *status_code == 409 || message.to_ascii_lowercase().contains("already exists"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_network_name_follows_convention() {
        assert_eq!(
            cluster_network_name("mongo", "suite-a"),
            "shakedown-mongo-suite-a"
        );
    }

    #[tokio::test]
    #[ignore = "requires a reachable Docker Engine socket"]
    async fn create_remove_network_roundtrip() {
        let Ok(docker) = Docker::connect_with_defaults() else {
            return;
        };
        if docker.ping().await.is_err() {
            return;
        }

        let name = create_cluster_network(&docker, "test-engine", "us001-roundtrip")
            .await
            .expect("create");

        assert_eq!(name, "shakedown-test-engine-us001-roundtrip");

        create_cluster_network(&docker, "test-engine", "us001-roundtrip")
            .await
            .expect("idempotent second create");

        remove_cluster_network(&docker, &name)
            .await
            .expect("remove");

        let err = remove_cluster_network(&docker, &name)
            .await
            .expect_err("gone");
        assert!(matches!(err, ClusterError::NetworkError { .. }));
    }
}
