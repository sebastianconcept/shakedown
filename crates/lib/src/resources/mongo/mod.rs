//! Built-in MongoDB resource and typed helpers (`mongodb` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::MongoHelpers;
pub use resource::MongoResource;
