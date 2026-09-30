// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#[macro_use]
extern crate lazy_static;

use std::borrow::Cow;
use std::ops::Deref;
use std::thread;

use rand::rngs::ThreadRng;

use rand::prelude::IndexedRandom;

use foundationdb as fdb;
use foundationdb::tuple::{Subspace, pack, unpack};
use foundationdb::{
    Database, FdbBindingError, FdbError, FdbResult, RangeOption, RetryableError, Transaction,
};
use futures::TryStreamExt;

type Result<T> = std::result::Result<T, Error>;

// The blessed layer-error pattern: one enum for the whole layer, carrying the
// FdbError (and the retry loop's own FdbBindingError) as a source. The
// `RetryableError` impl below makes `db.run` retry-transparent for wrapped
// FdbErrors, and the caller gets this typed error back from `db.run`.
#[derive(Debug)]
enum Error {
    NoRemainingSeats,
    TooManyClasses,
    Fdb(FdbError),
    Binding(FdbBindingError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::NoRemainingSeats => write!(f, "No remaining seats"),
            Error::TooManyClasses => write!(f, "Too many classes"),
            Error::Fdb(e) => write!(f, "FoundationDB error: {e}"),
            Error::Binding(e) => write!(f, "Retry loop error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Fdb(e) => Some(e),
            Error::Binding(e) => Some(e),
            Error::NoRemainingSeats | Error::TooManyClasses => None,
        }
    }
}

impl From<FdbError> for Error {
    fn from(err: FdbError) -> Self {
        Error::Fdb(err)
    }
}

impl From<FdbBindingError> for Error {
    fn from(err: FdbBindingError) -> Self {
        Error::Binding(err)
    }
}

// The default retry_decision walks source() and finds the wrapped FdbError.
impl RetryableError for Error {}

// Data model:
// ("attends", student, class) = ""
// ("class", class_name) = seatsLeft

// Generate 1,620 classes like '9:00 chem for dummies'
const LEVELS: &[&str] = &[
    "intro",
    "for dummies",
    "remedial",
    "101",
    "201",
    "301",
    "mastery",
    "lab",
    "seminar",
];

const TYPES: &[&str] = &[
    "chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance",
];

const TIMES: &[&str] = &[
    "2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00",
];

lazy_static! {
    static ref ALL_CLASSES: Vec<String> = all_classes();
}

// TODO: make these tuples?
fn all_classes() -> Vec<String> {
    let mut class_names: Vec<String> = Vec::new();
    for level in LEVELS {
        for _type in TYPES {
            for time in TIMES {
                class_names.push(format!("{time} {_type} {level}"));
            }
        }
    }

    class_names
}

fn init_classes(trx: &Transaction, all_classes: &[String]) {
    let class_subspace = Subspace::from("class");
    for class in all_classes {
        trx.set(&class_subspace.pack(class), &pack(&100_i64));
    }
}

async fn init(db: &Database, all_classes: &[String]) {
    db.run(|trx, _maybe_committed| async move {
        trx.clear_subspace_range(&"attends".into());
        trx.clear_subspace_range(&"class".into());
        init_classes(&trx, all_classes);
        Ok::<_, Error>(())
    })
    .await
    .expect("failed to initialize data");
}

async fn get_available_classes(db: &Database) -> Vec<String> {
    db.run(|trx, _maybe_committed| async move {
        let range = RangeOption::from(&Subspace::from("class"));

        let mut available_classes = Vec::<String>::new();
        let mut stream = trx.get_ranges_keyvalues(range, false);
        while let Some(key_value) = stream.try_next().await? {
            let count: i64 = unpack(key_value.value()).expect("failed to decode count");

            if count > 0 {
                let class: String = unpack(key_value.key()).expect("failed to decode class");
                available_classes.push(class);
            }
        }

        Ok::<_, Error>(available_classes)
    })
    .await
    .expect("failed to get classes")
}

async fn ditch_trx(trx: &Transaction, student: &str, class: &str) -> FdbResult<()> {
    let attends_key = pack(&("attends", student, class));

    // TODO: should get take an &Encode? current impl does encourage &[u8] reuse...
    if trx.get(&attends_key, true).await?.is_none() {
        return Ok(());
    }

    let class_key = pack(&("class", class));
    let available_seats = trx
        .get(&class_key, true)
        .await?
        .expect("class seats were not initialized");
    let available_seats: i64 =
        unpack::<i64>(available_seats.deref()).expect("failed to decode i64") + 1;

    //println!("{} ditching class: {}", student, class);
    trx.set(&class_key, &pack(&available_seats));
    trx.clear(&attends_key);
    Ok(())
}

async fn ditch(db: &Database, student: String, class: String) -> Result<()> {
    db.run(move |trx, _maybe_committed| {
        let student = student.clone();
        let class = class.clone();
        async move {
            ditch_trx(&trx, &student, &class).await?;
            Ok::<_, Error>(())
        }
    })
    .await
}

async fn signup_trx(trx: &Transaction, student: &str, class: &str) -> Result<()> {
    let attends_key = pack(&("attends", student, class));
    if trx.get(&attends_key, true).await?.is_some() {
        //println!("{} already taking class: {}", student, class);
        return Ok(());
    }

    let class_key = pack(&("class", class));
    let available_seats: i64 = unpack(
        &trx.get(&class_key, true)
            .await?
            .expect("class seats were not initialized"),
    )
    .expect("failed to decode i64");

    if available_seats <= 0 {
        return Err(Error::NoRemainingSeats);
    }

    let attends_range = RangeOption::from(&("attends", &student).into());
    let attends: Vec<_> = trx
        .get_ranges_keyvalues(attends_range, false)
        .try_collect()
        .await?;
    if attends.len() >= 5 {
        return Err(Error::TooManyClasses);
    }

    //println!("{} taking class: {}", student, class);
    trx.set(&class_key, &pack(&(available_seats - 1)));
    trx.set(&attends_key, &pack(&""));

    Ok(())
}

async fn signup(db: &Database, student: String, class: String) -> Result<()> {
    db.run(move |trx, _maybe_committed| {
        let student = student.clone();
        let class = class.clone();
        async move { signup_trx(&trx, &student, &class).await }
    })
    .await
}

async fn switch_classes(
    db: &Database,
    student_id: String,
    old_class: String,
    new_class: String,
) -> Result<()> {
    db.run(move |trx, _maybe_committed| {
        let student_id = student_id.clone();
        let old_class = old_class.clone();
        let new_class = new_class.clone();
        async move {
            ditch_trx(&trx, &student_id, &old_class).await?;
            signup_trx(&trx, &student_id, &new_class).await?;
            Ok::<_, Error>(())
        }
    })
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mood {
    Add,
    Ditch,
    Switch,
}

async fn perform_op(
    db: &Database,
    rng: &mut ThreadRng,
    mood: Mood,
    student_id: &str,
    all_classes: &[String],
    my_classes: &mut Vec<String>,
) -> Result<()> {
    match mood {
        Mood::Add => {
            let class = all_classes.choose(rng).unwrap();
            signup(db, student_id.to_string(), class.to_string()).await?;
            my_classes.push(class.to_string());
        }
        Mood::Ditch => {
            let class = all_classes.choose(rng).unwrap();
            ditch(db, student_id.to_string(), class.to_string()).await?;
            my_classes.retain(|s| s != class);
        }
        Mood::Switch => {
            let old_class = my_classes.choose(rng).unwrap().to_string();
            let new_class = all_classes.choose(rng).unwrap();
            switch_classes(
                db,
                student_id.to_string(),
                old_class.to_string(),
                new_class.to_string(),
            )
            .await?;
            my_classes.retain(|s| s != &old_class);
            my_classes.push(new_class.to_string());
        }
    }
    Ok(())
}

async fn simulate_students(student_id: usize, num_ops: usize) {
    let db = Database::new_compat(None)
        .await
        .expect("failed to get database");
    db.set_option(fdb::options::DatabaseOption::TransactionTimeout(5000))
        .expect("failed to set transaction timeout");
    db.set_option(fdb::options::DatabaseOption::TransactionRetryLimit(3))
        .expect("failed to set transaction retry limit");

    let student_id = format!("s{student_id}");
    let mut rng = rand::rng();

    let mut available_classes = Cow::Borrowed(&*ALL_CLASSES);
    let mut my_classes = Vec::<String>::new();

    for _ in 0..num_ops {
        let mut moods = Vec::<Mood>::new();

        if !my_classes.is_empty() {
            moods.push(Mood::Ditch);
            moods.push(Mood::Switch);
        }

        if my_classes.len() < 5 {
            moods.push(Mood::Add);
        }

        let mood = moods.choose(&mut rng).copied().unwrap();

        // on errors we recheck for available classes
        if perform_op(
            &db,
            &mut rng,
            mood,
            &student_id,
            &available_classes,
            &mut my_classes,
        )
        .await
        .is_err()
        {
            println!("getting available classes");
            available_classes = Cow::Owned(get_available_classes(&db).await);
        }
    }
}

async fn run_sim(db: &Database, students: usize, ops_per_student: usize) {
    let mut threads: Vec<(usize, thread::JoinHandle<()>)> = Vec::with_capacity(students);
    for i in 0..students {
        // TODO: ClusterInner has a mutable pointer reference, if thread-safe, mark that trait as Sync, then we can clone DB here...
        threads.push((
            i,
            thread::spawn(move || {
                futures::executor::block_on(simulate_students(i, ops_per_student));
            }),
        ));
    }

    // explicitly join...
    for (id, thread) in threads {
        thread.join().expect("failed to join thread");

        let student_id = format!("s{id}");
        let student_id_clone = student_id.clone();

        db.run(move |trx, _maybe_committed| {
            let student_id_inner = student_id_clone.clone();
            async move {
                let attends_range = RangeOption::from(&("attends", &student_id_inner).into());
                let mut stream = trx.get_ranges_keyvalues(attends_range, false);
                while let Some(key_value) = stream.try_next().await? {
                    let (_, s, class) =
                        unpack::<(String, String, String)>(key_value.key()).unwrap();
                    assert_eq!(student_id_inner, s);

                    println!("{student_id_inner} is taking: {class}");
                }
                Ok::<_, Error>(())
            }
        })
        .await
        .expect("get_ranges_keyvalues failed");
    }

    println!("Ran {} transactions", students * ops_per_student);
}

#[tokio::main]
async fn main() {
    fdb::boot().expect("failed to initialize FoundationDB");
    // The network is stopped and joined automatically at process exit, which is
    // fine for tests and short-lived tools like this example. In a production
    // application, prefer a clean teardown: the network thread is the event loop
    // driving every transaction and you may still have on-going operations at
    // exit time. Finish or cancel your work, drop the Database handles, then
    // call `foundationdb::api::stop_network()` yourself (terminal: any
    // FoundationDB use afterwards fails with error 2025).
    let db = fdb::Database::new_compat(None)
        .await
        .expect("failed to get database");
    db.set_option(fdb::options::DatabaseOption::TransactionTimeout(5000))
        .expect("failed to set transaction timeout");
    db.set_option(fdb::options::DatabaseOption::TransactionRetryLimit(3))
        .expect("failed to set transaction retry limit");
    init(&db, &ALL_CLASSES).await;
    println!("Initialized");
    run_sim(&db, 10, 10).await;
}
