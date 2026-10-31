//! OAuth user information — normalized across providers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::https_get;
use crate::config::OAuthConfig;
use crate::error::OAuthError;
use crate::token::TokenResponse;

// ---------------------------------------------------------------------------
// OAuthUser
// ---------------------------------------------------------------------------

/// Normalized user information returned by a provider.
///
/// Provider-specific fields are available in `raw`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthUser {
    /// Provider-assigned user ID (as a string for cross-provider compatibility).
    pub id: String,
    /// Stable configured issuer/provider namespace. Link accounts by (provider, id).
    pub provider: String,
    /// Primary email address, if available.
    pub email: Option<String>,
    /// Display name or username.
    pub name: Option<String>,
    /// URL of the user's avatar image.
    pub avatar_url: Option<String>,
    /// Raw JSON from the userinfo endpoint (all provider-specific fields).
    pub raw: Value,
}

impl OAuthUser {
    /// Extract an `OAuthUser` from raw JSON (provider-agnostic field mapping).
    pub fn from_json(raw: Value) -> Option<Self> {
        let value = raw.get("sub").or_else(|| raw.get("id"))?;
        let id = match value {
            Value::String(s) if !s.trim().is_empty() => s.clone(),
            Value::Number(n) if n.as_u64().is_some_and(|n| n > 0) => n.to_string(),
            _ => return None,
        };

        let email = raw.get("email").and_then(Value::as_str).map(str::to_string);

        let name = raw
            .get("name")
            .or_else(|| raw.get("login")) // GitHub
            .or_else(|| raw.get("username")) // Discord
            .and_then(Value::as_str)
            .map(str::to_string);

        let avatar_url = raw
            .get("avatar_url") // GitHub / Discord
            .or_else(|| raw.get("picture")) // Google OIDC
            .and_then(Value::as_str)
            .map(str::to_string);

        Some(Self {
            provider: String::new(),
            id,
            email,
            name,
            avatar_url,
            raw,
        })
    }
    /// Parse only the configured subject field/type; no cross-provider fallback.
    pub fn from_provider_json(config: &OAuthConfig, raw: Value) -> Option<Self> {
        if config.provider.trim().is_empty() || raw.get("error").is_some() {
            return None;
        }
        let value = raw.get(&config.subject_field)?;
        let id = match value {
            Value::String(s) if !s.trim().is_empty() => s.clone(),
            Value::Number(n) if config.numeric_subject && n.as_u64().is_some_and(|n| n > 0) => {
                n.to_string()
            }
            _ => return None,
        };
        let mut user = Self::from_json(serde_json::json!({"sub": id}))?;
        user.email = raw.get("email").and_then(Value::as_str).map(str::to_owned);
        user.name = raw
            .get("name")
            .or_else(|| raw.get("login"))
            .or_else(|| raw.get("username"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        user.avatar_url = raw
            .get("avatar_url")
            .or_else(|| raw.get("picture"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        user.provider = config.provider.clone();
        user.raw = raw;
        Some(user)
    }
}

// ---------------------------------------------------------------------------
// Fetch userinfo
// ---------------------------------------------------------------------------

/// Fetch user information from the provider.
///
/// Uses the userinfo endpoint if configured, otherwise falls back to
/// basic claims from the token response (OIDC `id_token` not decoded here —
/// use the raw `id_token` field for that).
pub async fn fetch_userinfo(
    config: &OAuthConfig,
    tokens: &TokenResponse,
) -> Result<OAuthUser, OAuthError> {
    if let Some(ref url) = config.userinfo_url {
        let body = https_get(url, &tokens.access_token).await?;
        let raw: Value = serde_json::from_str(&body)
            .map_err(|e| OAuthError::UserInfo(format!("JSON parse: {e}")))?;
        OAuthUser::from_provider_json(config, raw)
            .ok_or_else(|| OAuthError::UserInfo("missing 'id' field in userinfo response".into()))
    } else {
        // No userinfo endpoint and no decoded, verified OIDC id_token subject:
        // refuse rather than invent an identity. The previous fallback used
        // the first 16 characters of the opaque access token as `id` — for
        // JWT-shaped tokens that prefix is the (shared) header, so distinct
        // users received exactly the same id and account linkage conflated
        // them. Configure `userinfo_url` (the built-in presets all do) or
        // verify the `id_token` claim yourself before linking accounts.
        Err(OAuthError::UserInfo(
            "provider has no userinfo_url; refusing to derive user identity from an opaque \
             access token — configure userinfo_url or verify the OIDC id_token"
                .to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Regression (RS-15): with no userinfo endpoint the old fallback
    /// returned a user whose `id` was the first 16 chars of the access
    /// token — for JWT-shaped tokens that is the shared header, so distinct
    /// users were conflated onto one identity. The fallback must fail closed.
    #[tokio::test]
    async fn missing_userinfo_url_fails_closed() {
        let config = OAuthConfig::new("https://example.com/authorize", "https://example.com/token");
        let tokens = crate::token::TokenResponse {
            access_token: "eyJhbGciOiJIUzI1NiJ9.SUBJECT_A.SIGNATURE".to_string(),
            refresh_token: None,
            expires_in: Some(3600),
            token_type: "bearer".to_string(),
            id_token: None,
            scope: None,
        };
        let err = fetch_userinfo(&config, &tokens).await.unwrap_err();
        assert!(matches!(err, crate::error::OAuthError::UserInfo(_)));
    }

    #[test]
    fn from_json_github_style() {
        let raw = json!({
            "id":         12345,
            "login":      "alice",
            "email":      "alice@example.com",
            "avatar_url": "https://avatars.github.com/u/12345"
        });
        let user = OAuthUser::from_json(raw).unwrap();
        assert_eq!(user.id, "12345");
        assert_eq!(user.name.as_deref(), Some("alice"));
        assert_eq!(user.email.as_deref(), Some("alice@example.com"));
        assert!(user.avatar_url.is_some());
    }

    #[test]
    fn from_json_google_oidc_style() {
        let raw = json!({
            "sub":     "107978799123456789",
            "email":   "alice@gmail.com",
            "name":    "Alice Smith",
            "picture": "https://lh3.googleusercontent.com/photo.jpg"
        });
        let user = OAuthUser::from_json(raw).unwrap();
        assert_eq!(user.id, "107978799123456789");
        assert_eq!(
            user.avatar_url.as_deref(),
            Some("https://lh3.googleusercontent.com/photo.jpg")
        );
    }

    #[test]
    fn from_json_discord_style() {
        let raw = json!({
            "id":       "123456789012345678",
            "username": "alice#1234",
            "email":    "alice@discord.com",
            "avatar":   "abc123"
        });
        let user = OAuthUser::from_json(raw).unwrap();
        assert_eq!(user.id, "123456789012345678");
        assert_eq!(user.name.as_deref(), Some("alice#1234"));
    }

    #[test]
    fn from_json_missing_id_returns_none() {
        let raw = json!({ "email": "alice@example.com" });
        assert!(OAuthUser::from_json(raw).is_none());
    }

    #[test]
    fn from_json_numeric_id_stringified() {
        let raw = json!({ "id": 42 });
        let user = OAuthUser::from_json(raw).unwrap();
        assert_eq!(user.id, "42");
    }

    #[test]
    fn from_json_preserves_raw() {
        let raw = json!({ "id": "x", "custom_field": "custom_value" });
        let user = OAuthUser::from_json(raw).unwrap();
        assert_eq!(user.raw["custom_field"], "custom_value");
    }

    #[test]
    fn configured_subject_rejects_malformed_values_and_namespaces_equal_ids() {
        let github = crate::config::OAuthProvider::github();
        let google = crate::config::OAuthProvider::google();
        for bad in [
            json!(null),
            json!(true),
            json!({}),
            json!([]),
            json!(""),
            json!("   "),
            json!(1.5),
        ] {
            assert!(OAuthUser::from_provider_json(&github, json!({"id":bad})).is_none());
        }
        assert!(
            OAuthUser::from_provider_json(&github, json!({"id": 1, "error":"denied"})).is_none()
        );
        let a = OAuthUser::from_provider_json(&github, json!({"id": 1, "sub":"wrong"})).unwrap();
        let b = OAuthUser::from_provider_json(&google, json!({"sub":"1", "id":false})).unwrap();
        assert_eq!(a.id, b.id);
        assert_ne!(a.provider, b.provider);
    }
}
