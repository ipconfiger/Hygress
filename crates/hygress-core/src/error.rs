//! Crate-wide error type for `hygress-core`.
//!
//! All fallible pure operations (parsing destination strings, validating
//! routes/config, compiling path predicates) surface as one of the variants
//! below. The enum is `Clone`/`Eq` so it can be embedded or asserted in tests.

use thiserror::Error;

/// Unified error for parse / validation / lookup failures in the pure core.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    /// Input could not be parsed (annotation strings, service refs, ports,
    /// regexes, numeric fields).
    #[error("parse error: {0}")]
    Parse(String),

    /// Structurally unparsable but semantically well-formed input, or a
    /// structural invariant violation (empty key, missing port, ...).
    #[error("invalid: {0}")]
    Invalid(String),

    /// A referenced identifier (service type suffix, outbound proxy, fallback
    /// target) is unknown.
    #[error("unknown: {0}")]
    Unknown(String),
}

impl Error {
    /// Convenience constructor for parse failures.
    pub fn parse(msg: impl Into<String>) -> Self {
        Error::Parse(msg.into())
    }

    /// Convenience constructor for invariant violations.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Invalid(msg.into())
    }

    /// Convenience constructor for unknown references.
    pub fn unknown(msg: impl Into<String>) -> Self {
        Error::Unknown(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_variants() {
        assert_eq!(Error::parse("bad").to_string(), "parse error: bad");
        assert_eq!(Error::invalid("x").to_string(), "invalid: x");
        assert_eq!(Error::unknown("y").to_string(), "unknown: y");
    }

    #[test]
    fn equality_and_clone() {
        let a = Error::Parse("abc".to_string());
        assert_eq!(a.clone(), a);
        assert_ne!(a, Error::Invalid("abc".to_string()));
    }
}
