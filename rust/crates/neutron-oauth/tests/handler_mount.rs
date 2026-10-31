use neutron::{
    handler::{Body, Request},
    router::Router,
    testing::TestClient,
};
use neutron_oauth::{oauth_callback_handler, oauth_redirect_handler, OAuthConfig};
#[tokio::test]
async fn exported_factories_mount_without_adapters() {
    let config = OAuthConfig::new("https://example.com/authorize", "https://example.com/token")
        .client_id("test")
        .redirect_uri("https://example.com/callback")
        .secret(vec![7; 32]);
    let router = Router::new()
        .get("/login", oauth_redirect_handler(config.clone()))
        .get(
            "/callback",
            oauth_callback_handler(config, |_user, _request: Request| async {
                http::Response::new(Body::empty())
            }),
        );
    let client = TestClient::new(router);
    assert_eq!(client.get("/login").send().await.status(), 302);
    assert_eq!(client.get("/callback").send().await.status(), 400);
}
