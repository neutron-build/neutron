//! OAuth2 / OIDC authorization code flow for the Neutron web framework.
//!
//! Supports PKCE (S256), signed anti-CSRF state cookies, token exchange,
//! and normalized user info across providers.
//!
//! # Built-in providers
//!
//! ```rust,ignore
//! use neutron_oauth::{OAuthProvider, oauth_redirect_handler, oauth_callback_handler};
//!
//! let config = OAuthProvider::github()
//!     .client_id(env::var("GITHUB_CLIENT_ID")?)
//!     .client_secret(env::var("GITHUB_CLIENT_SECRET")?)
//!     .redirect_uri("https://myapp.com/auth/github/callback")
//!     .secret(env::var("OAUTH_SECRET")?.into_bytes());
//!
//! let router = Router::new()
//!     .get("/auth/github",          oauth_redirect_handler(config.clone()))
//!     .get("/auth/github/callback", oauth_callback_handler(config, on_login));
//!
//! async fn on_login(user: OAuthUser, _req: Request) -> Response {
//!     // Create session, redirect home…
//!     (StatusCode::SEE_OTHER, [("location", "/")]).into_response()
//! }
//! ```

pub(crate) mod client;

pub mod config;
pub mod error;
pub mod handlers;
pub mod pkce;
pub mod state;
pub mod token;
pub mod user;

pub use config::{OAuthConfig, OAuthProvider};
pub use error::OAuthError;
pub use handlers::{oauth_callback_handler, oauth_redirect_handler};
pub use pkce::PkceChallenge;
pub use token::TokenResponse;
pub use user::OAuthUser;
