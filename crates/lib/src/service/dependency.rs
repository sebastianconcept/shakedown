//! Topological ordering for `depends_on` service edges (`services` feature).

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{InternalError, ShakedownError};
use crate::service::state::ServiceState;

/// Returns service names in startup order: dependencies before dependents. Fails on cycles or
/// missing dependency names.
pub(crate) fn service_startup_order(
    services: &HashMap<String, ServiceState>,
) -> Result<Vec<String>, ShakedownError> {
    let names: HashSet<String> = services.keys().cloned().collect();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    let mut indegree: HashMap<String, usize> = HashMap::new();

    for name in &names {
        indegree.insert(name.clone(), 0);
    }

    for (service_name, state) in services {
        for dependency in &state.spec.depends_on {
            if !names.contains(dependency) {
                return Err(ShakedownError::Internal(
                    InternalError::InvalidConfiguration(format!(
                        "service '{service_name}' depends on '{dependency}', which is not registered"
                    )),
                ));
            }
            adjacency
                .entry(dependency.clone())
                .or_default()
                .push(service_name.clone());
            *indegree.get_mut(service_name).expect("service in map") += 1;
        }
    }

    let mut queue: VecDeque<String> = indegree
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(name, _)| name.clone())
        .collect();
    let mut ordered = Vec::new();

    while let Some(node) = queue.pop_front() {
        ordered.push(node.clone());
        if let Some(neighbors) = adjacency.get(&node) {
            for neighbor in neighbors {
                let entry = indegree
                    .get_mut(neighbor)
                    .expect("neighbor must exist in indegree map");
                *entry -= 1;
                if *entry == 0 {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    if ordered.len() != names.len() {
        return Err(ShakedownError::Internal(
            InternalError::InvalidConfiguration(
                "circular `depends_on` dependency between services".to_string(),
            ),
        ));
    }

    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::ServiceUnderTest;

    fn spec_named(name: &str, depends_on: &[&str]) -> ServiceState {
        let mut builder = ServiceUnderTest::docker("x:y").name(name).port(80);
        for dependency in depends_on {
            builder = builder.depends_on(*dependency);
        }
        ServiceState::new(builder.build().expect("build"))
    }

    #[test]
    fn linear_order() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), spec_named("a", &[]));
        map.insert("b".to_string(), spec_named("b", &["a"]));
        let order = service_startup_order(&map).expect("order");
        assert!(
            order.iter().position(|s| s == "a").unwrap()
                < order.iter().position(|s| s == "b").unwrap()
        );
    }

    #[test]
    fn diamond_order() {
        let mut map = HashMap::new();
        map.insert("root".to_string(), spec_named("root", &[]));
        map.insert("left".to_string(), spec_named("left", &["root"]));
        map.insert("right".to_string(), spec_named("right", &["root"]));
        map.insert("join".to_string(), spec_named("join", &["left", "right"]));
        let order = service_startup_order(&map).expect("order");
        let pos = |name: &str| order.iter().position(|s| s == name).unwrap();
        assert!(pos("root") < pos("left"));
        assert!(pos("root") < pos("right"));
        assert!(pos("left") < pos("join"));
        assert!(pos("right") < pos("join"));
    }

    #[test]
    fn cycle_errors() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), spec_named("a", &["b"]));
        map.insert("b".to_string(), spec_named("b", &["a"]));
        let error = service_startup_order(&map).expect_err("cycle");
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("circular"), "{message}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_dependency_errors() {
        let mut map = HashMap::new();
        map.insert("only".to_string(), spec_named("only", &["ghost"]));
        let error = service_startup_order(&map).expect_err("missing");
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("ghost"), "{message}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn two_independent_services_valid_ordering() {
        let mut map = HashMap::new();
        map.insert("left".to_string(), spec_named("left", &[]));
        map.insert("right".to_string(), spec_named("right", &[]));
        let order = service_startup_order(&map).expect("order");
        assert_eq!(order.len(), 2);
        assert!(order.contains(&"left".to_string()));
        assert!(order.contains(&"right".to_string()));
    }
}
