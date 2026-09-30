extern crate foundationdb_gen;

fn main() {
    let mut code = String::new();
    foundationdb_gen::emit(&mut code).expect("couldn't generate options.rs code!");
    println!("{code}");
}
