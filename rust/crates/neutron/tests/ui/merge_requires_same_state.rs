// merge() requires both routers to share the same state type S.
// Merging Router<A> with Router<B> must fail to compile.
use neutron::router::Router;

#[derive(Clone)]
struct A;
#[derive(Clone)]
struct B;

fn main() {
    let _ = Router::<A>::new().merge(Router::<B>::new());
}
