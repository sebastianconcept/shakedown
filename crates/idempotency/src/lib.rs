//! Idempotency **decisions** for at-least-once messaging and duplicate HTTP calls.
//!
//! This crate is **transport-agnostic**: the same [`IdempotentDecision`] applies whether the
//! work was triggered from a Kafka consumer or an HTTP handler. Callers run storage lookups
//! elsewhere, then map the outcome into `Proceed` vs `AlreadyHandled`.
//!
//! ## Book context
//!
//! Kafka (and similar brokers) typically deliver messages **at least once**. Without an
//! idempotency check, redelivery can duplicate side effects. HTTP clients can also retry
//! `POST` requests. Encode “should I run side effects?” once and reuse it per entry point.

/// Outcome of checking whether work for a business key was already done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotentDecision<T> {
    /// No prior completed work for this key — run inserts and downstream publishes.
    Proceed,
    /// Work was already completed; skip side effects (optionally return existing record id).
    AlreadyHandled { existing: T },
}

impl<T> IdempotentDecision<T> {
    /// Map `Proceed` to `Ok`, or return early with the existing value.
    pub fn proceed_or_else<F, R>(self, on_proceed: F) -> Result<R, T>
    where
        F: FnOnce() -> R,
    {
        match self {
            Self::Proceed => Ok(on_proceed()),
            Self::AlreadyHandled { existing } => Err(existing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdempotentDecision;

    #[test]
    fn proceed_runs_closure() {
        let decision: IdempotentDecision<&str> = IdempotentDecision::Proceed;
        let result = decision.proceed_or_else(|| 42);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn already_handled_skips_closure() {
        let decision = IdempotentDecision::AlreadyHandled { existing: "ful-1" };
        let result = decision.proceed_or_else(|| panic!("should not run"));
        assert_eq!(result, Err("ful-1"));
    }
}
