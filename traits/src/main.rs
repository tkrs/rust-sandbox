#[derive(Debug)]
struct Foo<A> {
    a: A,
}

trait Get<A> {
    fn get() -> A;
}

impl Get<String> for String {
    fn get() -> String {
        "Hello!".to_string()
    }
}

impl<A> Foo<A>
where
    A: Get<A>,
{
    fn new() -> Foo<A> {
        Foo { a: A::get() }
    }
}

fn main() {
    let f: Foo<String> = Foo::new();

    println!("{:?}", f)
}
