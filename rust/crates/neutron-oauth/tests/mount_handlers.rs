//! RS-29 regression: the published `Fn(Request)` handler factories must
//! satisfy the Router's `Handler` bounds when mounted exactly as documented.
//! `Request` previously implemented neither `FromRequestParts` nor
//! `FromRequest`, so these mounts did not compile; the identity extractor
//! added in neutron's extract.rs makes the raw-request factories usable.

use neutron::prelude::*;
use neutron_oauth::{OAuthConfig, OAuthProvider};

#[tokio::test]
async fn oauth_factories_mount_on_router() {
    let config = OAuthProvider::github()
        .client_id("cid")
        .client_secret("csecret")
        .redirect_uri("http://localhost:3000/auth/callback")
        .secret(b"0123456789abcdef0123456789abcdef".to_vec());

    let router = Router::new()
        .get(
            "/auth/github",
            neutron_oauth::oauth_redirect_handler(config.clone()),
        )
        .get(
            "/auth/github/callback",
            neutron_oauth::oauth_callback_handler(config, |_user, _req| async {
                (http::StatusCode::OK, "logged in").into_response()
            }),
        );

    let client = neutron::testing::TestClient::new(router);

    // The redirect handler must answer with a 302 to GitHub.
    let resp = client.get("/auth/github").send().await;
    assert_eq!(resp.status(), http::StatusCode::FOUND);
    let location = resp.header("location").unwrap().to_string();
    assert!(location.starts_with("https://github.com/login/oauth/authorize"), "{location}");
    assert!(location.contains("state="), "PKCE/CSRF state parameter: {location}");

    // The callback handler must be REACHABLE (missing code → 400 with the
    // error surfaced, never a routing failure).
    let resp = client.get("/auth/github/callback").send().await;
    assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
}
