// Passing case: a valid handler annotated with #[debug_handler] compiles and
// registers on a router.
use neutron::extract::Path;
use neutron::router::Router;

#[neutron::debug_handler]
async fn show(Path(id): Path<u64>) -> String {
    format!("item {id}")
}

#[neutron::debug_handler]
async fn index() -> &'static str {
    "ok"
}

fn main() {
    let _router = Router::<()>::new()
        .get("/", index)
        .get("/items/:id", show);
}
