//! [`IntoInfrastructureError`] — wrap errors as [`super::InfrastructureError::Backend`].

use std::error::Error;

use super::InfrastructureError;

/// Ergonomic conversion of `std::error::Error` values into [`InfrastructureError::Backend`].
///
/// Implemented for all `Error + Send + Sync + 'static` types so environment or IO failures can be
/// wrapped without bespoke match arms at every site.
pub trait IntoInfrastructureError: Error + Send + Sync {
    /// Box `self` as the source of an [`InfrastructureError::Backend`].
    fn into_infrastructure_error(self) -> InfrastructureError;
}

impl<T> IntoInfrastructureError for T
where
    T: Error + Send + Sync + 'static,
{
    fn into_infrastructure_error(self) -> InfrastructureError {
        InfrastructureError::Backend(Box::new(self))
    }
}
