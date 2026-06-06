// Passing case: #[derive(FromRef)] + with_state + a State handler all compile.
use neutron::extract::State;
use neutron::router::Router;
use neutron::FromRef;

#[derive(Clone)]
struct Db(u32);

#[derive(Clone, FromRef)]
struct AppState {
    db: Db,
}

fn main() {
    // The derive produced FromRef<AppState> for Db.
    let app = AppState { db: Db(1) };
    let _db: Db = <Db as FromRef<AppState>>::from_ref(&app);

    // Typed router binds the state and erases to Router<()>.
    let _router = Router::<AppState>::new()
        .get("/", |State(_s): State<AppState>| async { "ok" })
        .with_state(app);
}
