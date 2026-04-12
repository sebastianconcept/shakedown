//! Validated HTTP status codes for mock responses.
//!
//! [`HttpMockStatus`] wraps [`http::StatusCode`]. Only codes accepted by the `http` crate can be
//! constructed, which matches valid HTTP status code ranges used on the wire.

use std::fmt;

use http::StatusCode;

use super::super::types::MockServiceError;

/// Validated status for declarative mock routes ([`super::service::HttpMockService::respond`], etc.).
///
/// Wraps [`http::StatusCode`]. Construct with `StatusCode::OK.into()` or [`TryFrom<u16>`];
/// invalid codes are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HttpMockStatus(StatusCode);

impl HttpMockStatus {
    /// Returns the status as a `u16` (always in the range accepted by [`http::StatusCode`]).
    pub fn as_u16(self) -> u16 {
        self.0.as_u16()
    }

    /// The underlying [`http::StatusCode`].
    pub fn into_inner(self) -> StatusCode {
        self.0
    }
}

impl From<StatusCode> for HttpMockStatus {
    fn from(value: StatusCode) -> Self {
        Self(value)
    }
}

impl TryFrom<u16> for HttpMockStatus {
    type Error = MockServiceError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        StatusCode::from_u16(value)
            .map(Self)
            .map_err(|_| MockServiceError::InvalidHttpStatus { code: value })
    }
}

impl PartialEq<u16> for HttpMockStatus {
    fn eq(&self, other: &u16) -> bool {
        self.as_u16() == *other
    }
}

impl PartialEq<StatusCode> for HttpMockStatus {
    fn eq(&self, other: &StatusCode) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for HttpMockStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u16())
    }
}

#[cfg(test)]
mod tests {
    use super::HttpMockStatus;

    #[test]
    fn try_from_u16_rejects_invalid_status() {
        assert!(HttpMockStatus::try_from(99u16).is_err());
    }

    #[test]
    fn try_from_u16_accepts_standard_code() {
        assert_eq!(HttpMockStatus::try_from(404u16).expect("404").as_u16(), 404);
    }
}
