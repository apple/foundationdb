pub mod endpoints;
pub mod fdbserver;
pub mod flow;
pub mod services;

// *sigh* want this to be encapsulated down inside endpoints; such is life.
// #[allow(non_snake_case)]
#[allow(dead_code, unused_imports)]
#[path = "../target/flatbuffers/common_generated.rs"]
mod common_generated;
