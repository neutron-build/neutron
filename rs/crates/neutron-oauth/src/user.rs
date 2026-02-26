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
    pub id:         String,
    /// Primary email address, if available.
    pub email:      Option<String>,
    /// Display name or username.
    pub name:       Option<String>,
    /// URL of the user's avatar image.
    pub avatar_url: Option<String>,
    /// Raw JSON from the userinfo endpoint (all provider-specific fields).
    pub raw:        Value,
}

impl OAuthUser {
    /// Extract an `OAuthUser` from raw JSON (provider-agnostic field mapping).
    pub fn from_json(raw: Value) -> Option<Self> {
        // Try common field names across providers
        let id = raw.get("id")
            .or_else(|| raw.get("sub"))
            .map(|v| match v {
                Value::Number(n) => n.to_string(),
                Value::String(s) => s.clone(),
                other            => other.to_string(),
            })?;

        let email = raw.get("email")
            .and_then(Value::as_str)
            .map(str::to_string);

        let name = raw.get("name")
            .or_else(|| raw.get("login"))        // GitHub
            .or_else(|| raw.get("username"))     // Discord
            .and_then(Value::as_str)
            .map(str::to_string);

        let avatar_url = raw.get("avatar_url")   // GitHub / Discord
            .or_else(|| raw.get("picture"))      // Google OIDC
            .and_then(Value::as_str)
            .map(str::to_string);

        Some(Self { id, email, name, avatar_url, raw })
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
        OAuthUser::from_json(raw)
            .ok_or_else(|| OAuthError::UserInfo("missing 'id' field in userinfo response".into()))
    } else {
        // No userinfo endpoint — return a minimal user from the token alone
        Ok(OAuthUser {
            id:         tokens.access_token.chars().take(16).collect(),
            email:      None,
            name:       None,
            avatar_url: None,
            raw:        Value::Null,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        assert_eq!(user.avatar_url.as_deref(), Some("https://lh3.googleusercontent.com/photo.jpg"));
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
}
