use foundationdb as fdb;
use rand::RngExt;
use rand::distr::Alphanumeric;

/// generate random string. Foundationdb watch only fires when value changed, so updating with same
/// value twice will not fire watches. To make examples work over multiple run, we use random
/// string as a value.
#[allow(unused)]
pub fn random_str(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[allow(unused)]
pub async fn database() -> fdb::FdbResult<fdb::Database> {
    fdb::Database::new_compat(None).await
}
