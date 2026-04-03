//! Suite-shared Toxiproxy resource (`fault-injection` feature).
mod resource;
#[cfg(test)]
mod tests;

pub(crate) use resource::toxiproxy_suite_container_id;
pub use resource::ToxiproxyResource;
