use std::sync::Arc;

use neutron::prelude::*;
use neutron::ws::{Message, WebSocket, WebSocketUpgrade};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

// -- Shared state -----------------------------------------------------------

struct AppState {
    app_name: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize, Clone)]
struct User {
    id: u64,
    name: String,
    email: String,
}

#[derive(Deserialize)]
struct NewUser {
    name: String,
    email: String,
}

// -- Handlers ---------------------------------------------------------------

async fn index(State(state): State<Arc<AppState>>) -> String {
    format!("Hello from {}!", state.app_name)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn list_users() -> Json<Vec<User>> {
    Json(vec![
        User {
            id: 1,
            name: "Alice".into(),
            email: "alice@example.com".into(),
        },
        User {
            id: 2,
            name: "Bob".into(),
            email: "bob@example.com".into(),
        },
    ])
}

async fn get_user(Path(id): Path<u64>) -> Json<User> {
    Json(User {
        id,
        name: "Alice".into(),
        email: "alice@example.com".into(),
    })
}

async fn create_user(Json(input): Json<NewUser>) -> (StatusCode, Json<User>) {
    let user = User {
        id: 3,
        name: input.name,
        email: input.email,
    };
    (StatusCode::CREATED, Json(user))
}

async fn delete_user(Path(id): Path<u64>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "deleted": id }))
}

async fn old_endpoint() -> Redirect {
    Redirect::to("/users")
}

async fn not_found() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "not found" })),
    )
}

// -- Cookie handlers --------------------------------------------------------

async fn login() -> SetCookie {
    SetCookie::new("session", "abc123")
        .path("/")
        .http_only()
        .same_site(SameSite::Lax)
        .max_age(86400)
}

async fn whoami(cookies: CookieJar) -> String {
    match cookies.get("session") {
        Some(session) => format!("Session: {session}"),
        None => "Not logged in".to_string(),
    }
}

async fn logout() -> SetCookie {
    SetCookie::remove("session").path("/")
}

// -- WebSocket handler -------------------------------------------------------

async fn ws_echo(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_ws)
}

async fn handle_ws(mut socket: WebSocket) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Message::Text(text) => {
                socket
                    .send(Message::text(format!("Echo: {text}")))
                    .await
                    .ok();
            }
            Message::Binary(data) => {
                socket.send(Message::binary(data)).await.ok();
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
}

// -- Middleware --------------------------------------------------------------

async fn timing(req: Request, next: Next) -> Response {
    let start = std::time::Instant::now();
    let mut response = next.run(req).await;
    let elapsed = start.elapsed();
    response.headers_mut().insert(
        "x-response-time",
        format!("{}ms", elapsed.as_millis()).parse().unwrap(),
    );
    response
}

// -- Main -------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let state = Arc::new(AppState {
        app_name: "Neutron Rust".into(),
    });

    // CORS configuration
    let cors = Cors::new()
        .allow_any_origin()
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_any_header()
        .max_age(3600);

    // User routes grouped into a sub-router
    let users = Router::new()
        .get("/", list_users)
        .get("/:id", get_user)
        .post("/", create_user)
        .delete("/:id", delete_user);

    let router = Router::new()
        .state(state)
        .middleware(BodyLimit::new(1024 * 1024)) // 1 MB
        .middleware(RequestId::new())
        .middleware(Logger::new())
        .middleware(Compress::new())
        .middleware(Timeout::from_secs(30))
        .middleware(cors)
        .middleware(timing)
        .get("/", index)
        .get("/health", health)
        .get("/old-users", old_endpoint)
        .get("/login", login)
        .get("/whoami", whoami)
        .get("/logout", logout)
        .get("/ws", ws_echo)
        .nest("/users", users)
        .fallback(not_found);

    let config = Config::from_env();

    Neutron::new().router(router).listen(config.socket_addr()).await
}
