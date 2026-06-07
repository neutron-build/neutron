// Failing case: an argument that is not an extractor. #[debug_handler] surfaces
// the error on the argument type (FromRequest not implemented) via its hidden
// assertion — no router registration needed, so the only diagnostic is the
// targeted one (keeping the snapshot stable across feature sets).
struct NotAnExtractor;

#[neutron::debug_handler]
async fn create(_payload: NotAnExtractor) -> &'static str {
    "never"
}

fn main() {}
