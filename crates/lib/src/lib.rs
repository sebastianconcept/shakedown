//! Main library crate for the workspace.

/// Returns the compile-time default greeting emitted by the `macros` crate.
pub fn greeting() -> &'static str {
    macros::default_greeting!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greeting_matches_expected() {
        // Given the default greeting macro installed for this crate
        // When we read the greeting string
        let message = greeting();
        // Then it is the expected phrase
        assert_eq!(message, "Hello, shakedown!");
    }
}
