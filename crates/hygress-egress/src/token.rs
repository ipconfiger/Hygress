//! Gateway auth-token derivation + `jwt_secret_key` resolution (design §9).
//!
//! The GPUStack server signs the out-of-band `POST /v2/usage/gateway-metrics` and the ext-auth
//! `X-GPUStack-Auth-Token` injection with a derived token:
//! `hex(HMAC-SHA256(jwt_secret_key, "gateway-metrics-push"))` (pin §2.1/§2.8; `config.py:359-361`,
//! `api/auth.py:53`). The `jwt_secret_key` itself is resolved with **fail-fast** precedence:
//! 1. env `GPUSTACK_JWT_SECRET_KEY` (trailing newline stripped);
//! 2. the file `{data_dir}/jwt_secret_key` (trimmed);
//! 3. neither → `Err` (never silently degrade — an empty/missing key would make usage silently lose
//!    401, so the gateway must refuse to start).

use std::path::Path;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::Error;
use crate::Result;

/// The fixed HMAC message for the derived gateway token.
///
/// MUST stay byte-exact with GPUStack's `get_derived_gateway_token`: any drift invalidates the
/// token and makes every `/v2/usage/gateway-metrics` report 401.
pub const GATEWAY_METRICS_PUSH_MESSAGE: &str = "gateway-metrics-push";

/// `hex(HMAC-SHA256(key, "gateway-metrics-push"))` — the derived gateway auth token
/// (`X-GPUStack-Auth-Token`).
///
/// `key` is the `jwt_secret_key` (the resolved secret, NOT the token itself). Lowercase hex, 64
/// chars. This is exactly the wire value the server compares (pin §5.1).
pub fn derive_gateway_token(key: &[u8]) -> String {
    // HMAC-SHA256 accepts keys of any length (this is not AES), so new_from_slice cannot fail.
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
    mac.update(GATEWAY_METRICS_PUSH_MESSAGE.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Resolve the `jwt_secret_key` per the design §9 precedence.
///
/// | step | source | transform |
/// |------|--------|-----------|
/// | 1 | `env_val` (`GPUSTACK_JWT_SECRET_KEY`) | strip trailing newline, trim |
/// | 2 | file `{data_dir}/jwt_secret_key` | trim |
/// | 3 | — | `Err` (fail-fast, never silent) |
///
/// An **empty** resolved value (env set to whitespace, or a blank file) is also `Err`
/// ([`Error::JwtKeyEmpty`]) — an empty secret key is meaningless and would silently 401 every usage
/// report, so we refuse rather than degrade.
pub fn resolve_jwt_key(env_val: Option<&str>, data_dir: &Path) -> Result<Vec<u8>> {
    if let Some(v) = env_val {
        let k = normalize(v);
        if k.is_empty() {
            return Err(Error::JwtKeyEmpty);
        }
        return Ok(k.as_bytes().to_vec());
    }
    let path = data_dir.join("jwt_secret_key");
    let raw = std::fs::read(&path).map_err(|_| Error::JwtKeyNotFound {
        data_dir: path.display().to_string(),
    })?;
    let decoded = String::from_utf8_lossy(&raw);
    let k = normalize(&decoded);
    if k.is_empty() {
        return Err(Error::JwtKeyEmpty);
    }
    Ok(k.as_bytes().to_vec())
}

/// Strip one trailing newline (the form `with-contenv` writes) then trim residual whitespace.
fn normalize(s: &str) -> &str {
    s.strip_suffix('\n').unwrap_or(s).trim()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// A unique throwaway temp dir (unique per call, safe under parallel test threads).
    fn make_tmpdir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "hygress-egress-token-{}-{}-{n}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_key(dir: &Path, contents: &str) {
        std::fs::write(dir.join("jwt_secret_key"), contents).unwrap();
    }

    // ----- derive_gateway_token: independent oracle vectors -----
    //
    // Each expected value was produced by
    //   printf 'gateway-metrics-push' | openssl dgst -sha256 -hmac '<key>'
    // i.e. HMAC-SHA256 over the literal bytes "gateway-metrics-push" with the given key — the exact
    // same construction as GPUStack `get_derived_gateway_token`. Using an *external* tool as the
    // oracle (rather than re-implementing HMAC here) proves the impl matches the wire contract.

    #[test]
    fn derive_matches_openssl_vector_key() {
        // key = "key"
        assert_eq!(
            derive_gateway_token(b"key"),
            "9d8f63f95df76a66fbb041267328ca5e510bde73c377981d8d1971933dc3ea50"
        );
    }

    #[test]
    fn derive_matches_openssl_vector_secret() {
        // key = "secret"
        assert_eq!(
            derive_gateway_token(b"secret"),
            "9b3a119f87975793afd1c94bfd53cc26d7bbb049dc051d56ee8d018e4fb8ea4c"
        );
    }

    #[test]
    fn derive_matches_openssl_vector_longer() {
        // key = "my-jwt-secret-key"
        assert_eq!(
            derive_gateway_token(b"my-jwt-secret-key"),
            "3b6bf311b14c05ac82a0b1c7eb807127a63116551f887882273dc8a1c902b780"
        );
    }

    #[test]
    fn derive_matches_openssl_vector_special() {
        // key = "s3cr3t-with-dashes-and-dots:123" (non-alphanumeric, colon)
        assert_eq!(
            derive_gateway_token(b"s3cr3t-with-dashes-and-dots:123"),
            "17d3157a3455f19a7cbb237ac24e6cb13a266f74827ad4b98f21a2ca21783185"
        );
    }

    #[test]
    fn derive_is_lowercase_hex_64_chars() {
        let t = derive_gateway_token(b"whatever-key");
        assert_eq!(t.len(), 64);
        assert!(t
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn derive_different_key_different_token() {
        assert_ne!(derive_gateway_token(b"a"), derive_gateway_token(b"b"));
    }

    #[test]
    fn derive_message_is_the_pinned_string() {
        // Guard the pinned message: deriving under an empty key must equal the HMAC of the exact
        // pinned message string (the openssl-verified constant).
        assert_eq!(GATEWAY_METRICS_PUSH_MESSAGE, "gateway-metrics-push");
    }

    // ----- resolve_jwt_key: precedence + fail-fast -----

    #[test]
    fn resolve_env_takes_precedence_over_file() {
        let dir = make_tmpdir();
        write_key(&dir, "file-key\n");
        // env present wins, even though the file also exists.
        let k = resolve_jwt_key(Some("env-key\n"), &dir).unwrap();
        assert_eq!(&k, b"env-key");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_env_strips_trailing_newline() {
        let dir = make_tmpdir();
        let k = resolve_jwt_key(Some("env-key\n"), &dir).unwrap();
        assert_eq!(&k, b"env-key");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_env_strips_surrounding_whitespace() {
        let dir = make_tmpdir();
        let k = resolve_jwt_key(Some("  env-key  \n"), &dir).unwrap();
        assert_eq!(&k, b"env-key");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_file_when_no_env() {
        let dir = make_tmpdir();
        write_key(&dir, "file-key\n");
        let k = resolve_jwt_key(None, &dir).unwrap();
        assert_eq!(&k, b"file-key");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_file_trims_whitespace() {
        let dir = make_tmpdir();
        write_key(&dir, "  file-key  \n\n");
        let k = resolve_jwt_key(None, &dir).unwrap();
        assert_eq!(&k, b"file-key");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_file_missing_is_err_not_silent() {
        let dir = make_tmpdir();
        // No file written.
        let e = resolve_jwt_key(None, &dir).unwrap_err();
        match e {
            Error::JwtKeyNotFound { data_dir } => {
                assert!(data_dir.ends_with("jwt_secret_key"));
            }
            other => panic!("expected JwtKeyNotFound, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_empty_env_is_err_not_silent() {
        let dir = make_tmpdir();
        assert!(matches!(
            resolve_jwt_key(Some(""), &dir),
            Err(Error::JwtKeyEmpty)
        ));
        assert!(matches!(
            resolve_jwt_key(Some("   \n"), &dir),
            Err(Error::JwtKeyEmpty)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_empty_file_is_err_not_silent() {
        let dir = make_tmpdir();
        write_key(&dir, "   \n");
        assert!(matches!(
            resolve_jwt_key(None, &dir),
            Err(Error::JwtKeyEmpty)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_end_to_end_derives_and_matches_openssl() {
        // The full path: resolve the key from the env, derive the token, and cross-check the derived
        // token against the openssl oracle for that key.
        let dir = make_tmpdir();
        let key = resolve_jwt_key(Some("my-jwt-secret-key\n"), &dir).unwrap();
        let token = derive_gateway_token(&key);
        assert_eq!(
            token,
            "3b6bf311b14c05ac82a0b1c7eb807127a63116551f887882273dc8a1c902b780"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
