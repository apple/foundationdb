#[macro_use]
extern crate log;

use foundationdb as fdb;
use foundationdb_sys as fdb_sys;

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Write;
use std::mem::transmute;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;

use fdb::options::{ConflictRangeType, DatabaseOption, TransactionOption};
use fdb::tuple::{Bytes, Element, Subspace, TuplePack, pack, pack_into, unpack};
use fdb::*;
use futures::future;
use futures::prelude::*;
use num_bigint::BigInt;

static WAITED_FOR_EMPTY: Element = Element::Bytes(Bytes(Cow::Borrowed(b"WAITED_FOR_EMPTY")));
static RESULT_NOT_PRESENT: Element = Element::Bytes(Bytes(Cow::Borrowed(b"RESULT_NOT_PRESENT")));
static GOT_READ_VERSION: Element = Element::Bytes(Bytes(Cow::Borrowed(b"GOT_READ_VERSION")));
static GOT_COMMITTED_VERSION: Element =
    Element::Bytes(Bytes(Cow::Borrowed(b"GOT_COMMITTED_VERSION")));

static ERROR_NONE: Element = Element::Bytes(Bytes(Cow::Borrowed(b"ERROR: NONE")));
static ERROR_MULTIPLE: Element = Element::Bytes(Bytes(Cow::Borrowed(b"ERROR: MULTIPLE")));
static OK: Element = Element::Bytes(Bytes(Cow::Borrowed(b"OK")));
static ERROR_DIRECTORY: Element = Element::Bytes(Bytes(Cow::Borrowed(b"DIRECTORY_ERROR")));
static ESTIMATE_RANGE_RESPONSE: Element =
    Element::Bytes(Bytes(Cow::Borrowed(b"GOT_ESTIMATED_RANGE_SIZE")));
static GOT_APPROXIMATE_SIZE_RESPONSE: Element =
    Element::Bytes(Bytes(Cow::Borrowed(b"GOT_APPROXIMATE_SIZE")));
static GET_RANGE_SPLIT_POINTS_RESPONSE: Element =
    Element::Bytes(Bytes(Cow::Borrowed(b"GOT_RANGE_SPLIT_POINTS")));

use crate::fdb::options::{MutationType, StreamingMode};

use foundationdb::directory::DirectoryError;
use foundationdb::directory::DirectoryLayer;
use foundationdb::directory::{Directory, DirectoryOutput};
use foundationdb::tuple::{PackResult, TupleUnpack};

use tuple::VersionstampOffset;

fn mutation_from_str(s: &str) -> MutationType {
    match s {
        "ADD" => MutationType::Add,
        "AND" => MutationType::And,
        "BIT_AND" => MutationType::BitAnd,
        "OR" => MutationType::Or,
        "BIT_OR" => MutationType::BitOr,
        "XOR" => MutationType::Xor,
        "BIT_XOR" => MutationType::BitXor,
        "APPEND_IF_FITS" => MutationType::AppendIfFits,
        "MAX" => MutationType::Max,
        "MIN" => MutationType::Min,
        "SET_VERSIONSTAMPED_KEY" => MutationType::SetVersionstampedKey,
        "SET_VERSIONSTAMPED_VALUE" => MutationType::SetVersionstampedValue,
        "BYTE_MIN" => MutationType::ByteMin,
        "BYTE_MAX" => MutationType::ByteMax,
        "COMPARE_AND_CLEAR" => MutationType::CompareAndClear,
        _ => unimplemented!("mutation_from_str({})", s),
    }
}

pub fn streaming_from_value(val: i32) -> StreamingMode {
    match val {
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_WANT_ALL => StreamingMode::WantAll,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR => StreamingMode::Iterator,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_EXACT => StreamingMode::Exact,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_SMALL => StreamingMode::Small,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_MEDIUM => StreamingMode::Medium,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_LARGE => StreamingMode::Large,
        fdb_sys::FDBStreamingMode_FDB_STREAMING_MODE_SERIAL => StreamingMode::Serial,
        _ => unimplemented!("streaming_from_value({})", val),
    }
}

struct Instr {
    code: InstrCode,
    database: bool,
    snapshot: bool,
    starts_with: bool,
    selector: bool,
}

impl std::fmt::Debug for Instr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "[{:?}", self.code)?;
        if self.database {
            write!(fmt, " db")?;
        }
        if self.snapshot {
            write!(fmt, " snapshot")?;
        }
        if self.starts_with {
            write!(fmt, " starts_with")?;
        }
        if self.selector {
            write!(fmt, " selector")?;
        }
        write!(fmt, "]")
    }
}

#[derive(Debug)]
enum DirectoryStackItem {
    Directory(DirectoryLayer),
    DirectoryOutput(DirectoryOutput),
    Subspace(Subspace),
    Null,
}

impl Instr {
    fn pop_database(&mut self) -> bool {
        if self.database {
            self.database = false;
            true
        } else {
            false
        }
    }
    fn pop_snapshot(&mut self) -> bool {
        if self.snapshot {
            self.snapshot = false;
            true
        } else {
            false
        }
    }
    fn pop_starts_with(&mut self) -> bool {
        if self.starts_with {
            self.starts_with = false;
            true
        } else {
            false
        }
    }
    fn pop_selector(&mut self) -> bool {
        if self.selector {
            self.selector = false;
            true
        } else {
            false
        }
    }

    fn has_flags(&self) -> bool {
        self.database || self.snapshot || self.starts_with || self.selector
    }
}

#[derive(Debug)]
enum InstrCode {
    // data operations
    Push(Element<'static>),
    Dup,
    EmptyStack,
    Swap,
    Pop,
    Sub,
    Concat,
    LogStack,

    // foundationdb operations
    NewTransaction,
    UseTransaction,
    OnError,
    Get,
    GetKey,
    GetRange,
    GetReadVersion,
    GetVersionstamp,
    Set,
    SetReadVersion,
    Clear,
    ClearRange,
    AtomicOp,
    ReadConflictRange,
    WriteConflictRange,
    ReadConflictKey,
    WriteConflictKey,
    DisableWriteConflict,
    Commit,
    Reset,
    Cancel,
    GetCommittedVersion,
    GetApproximateSize,
    WaitFuture,
    GetEstimatedRangeSize,
    GetRangeSplitPoints,

    TuplePack,
    TuplePackWithVersionstamp,
    TupleUnpack,
    TupleRange,
    TupleSort,
    EncodeFloat,
    EncodeDouble,
    DecodeFloat,
    DecodeDouble,

    // Thread Operations
    StartThread,
    WaitEmpty,

    // misc
    UnitTests,

    // Directory/Subspace/Layer Creation
    DirectoryCreateSubspace,
    DirectoryCreateLayer,
    DirectoryCreateOrOpen,
    DirectoryCreate,
    DirectoryOpen,

    // Directory Management
    DirectoryChange,
    DirectorySetErrorIndex,

    // Directory Operations
    DirectoryMove,
    DirectoryMoveTo,
    DirectoryRemove,
    DirectoryRemoveIfExists,
    DirectoryList,
    DirectoryExists,

    // Subspace operation
    DirectoryPackKey,
    DirectoryUnpackKey,
    DirectoryRange,
    DirectoryContains,
    DirectoryOpenSubspace,

    // Directory Logging
    DirectoryLogSubspace,
    DirectoryLogDirectory,

    // Other
    DirectoryStripPrefix,
}

fn has_opt<'a>(cmd: &'a str, opt: &'static str) -> (&'a str, bool) {
    if cmd.ends_with(opt) {
        (&cmd[0..(cmd.len() - opt.len())], true)
    } else {
        (cmd, false)
    }
}

impl Instr {
    fn from(data: &[u8]) -> Self {
        use crate::InstrCode::*;

        let data = Bytes::from(data);
        trace!("inst {data:?}");
        let tup: Vec<Element> = unpack(&data).unwrap();
        let cmd = tup[0].as_str().unwrap();

        let (cmd, database) = has_opt(cmd, "_DATABASE");
        let (cmd, snapshot) = has_opt(cmd, "_SNAPSHOT");
        let (cmd, starts_with) = has_opt(cmd, "_STARTS_WITH");
        let (cmd, selector) = has_opt(cmd, "_SELECTOR");

        let code = match cmd {
            "PUSH" => Push(tup[1].clone().into_owned()),
            "DUP" => Dup,
            "EMPTY_STACK" => EmptyStack,
            "SWAP" => Swap,
            "POP" => Pop,
            "SUB" => Sub,
            "CONCAT" => Concat,
            "LOG_STACK" => LogStack,

            "NEW_TRANSACTION" => NewTransaction,
            "USE_TRANSACTION" => UseTransaction,
            "ON_ERROR" => OnError,
            "GET" => Get,
            "GET_KEY" => GetKey,
            "GET_RANGE" => GetRange,
            "GET_READ_VERSION" => GetReadVersion,
            "GET_VERSIONSTAMP" => GetVersionstamp,

            "SET" => Set,
            "SET_READ_VERSION" => SetReadVersion,
            "CLEAR" => Clear,
            "CLEAR_RANGE" => ClearRange,
            "ATOMIC_OP" => AtomicOp,
            "READ_CONFLICT_RANGE" => ReadConflictRange,
            "WRITE_CONFLICT_RANGE" => WriteConflictRange,
            "READ_CONFLICT_KEY" => ReadConflictKey,
            "WRITE_CONFLICT_KEY" => WriteConflictKey,
            "DISABLE_WRITE_CONFLICT" => DisableWriteConflict,
            "COMMIT" => Commit,
            "RESET" => Reset,
            "CANCEL" => Cancel,
            "GET_COMMITTED_VERSION" => GetCommittedVersion,
            "GET_APPROXIMATE_SIZE" => GetApproximateSize,
            "WAIT_FUTURE" => WaitFuture,
            "GET_ESTIMATED_RANGE_SIZE" => GetEstimatedRangeSize,
            "GET_RANGE_SPLIT_POINTS" => GetRangeSplitPoints,

            "TUPLE_PACK" => TuplePack,
            "TUPLE_PACK_WITH_VERSIONSTAMP" => TuplePackWithVersionstamp,
            "TUPLE_UNPACK" => TupleUnpack,
            "TUPLE_RANGE" => TupleRange,
            "TUPLE_SORT" => TupleSort,
            "ENCODE_FLOAT" => EncodeFloat,
            "ENCODE_DOUBLE" => EncodeDouble,
            "DECODE_FLOAT" => DecodeFloat,
            "DECODE_DOUBLE" => DecodeDouble,

            "START_THREAD" => StartThread,
            "WAIT_EMPTY" => WaitEmpty,

            "UNIT_TESTS" => UnitTests,

            "DIRECTORY_CREATE_SUBSPACE" => DirectoryCreateSubspace,
            "DIRECTORY_CREATE_LAYER" => DirectoryCreateLayer,
            "DIRECTORY_CREATE_OR_OPEN" => DirectoryCreateOrOpen,
            "DIRECTORY_CREATE" => DirectoryCreate,
            "DIRECTORY_OPEN" => DirectoryOpen,

            "DIRECTORY_CHANGE" => DirectoryChange,
            "DIRECTORY_SET_ERROR_INDEX" => DirectorySetErrorIndex,

            "DIRECTORY_MOVE" => DirectoryMove,
            "DIRECTORY_MOVE_TO" => DirectoryMoveTo,
            "DIRECTORY_REMOVE" => DirectoryRemove,
            "DIRECTORY_REMOVE_IF_EXISTS" => DirectoryRemoveIfExists,
            "DIRECTORY_LIST" => DirectoryList,
            "DIRECTORY_EXISTS" => DirectoryExists,

            "DIRECTORY_PACK_KEY" => DirectoryPackKey,
            "DIRECTORY_UNPACK_KEY" => DirectoryUnpackKey,
            "DIRECTORY_RANGE" => DirectoryRange,
            "DIRECTORY_CONTAINS" => DirectoryContains,
            "DIRECTORY_OPEN_SUBSPACE" => DirectoryOpenSubspace,

            "DIRECTORY_LOG_SUBSPACE" => DirectoryLogSubspace,
            "DIRECTORY_LOG_DIRECTORY" => DirectoryLogDirectory,

            "DIRECTORY_STRIP_PREFIX" => DirectoryStripPrefix,

            name => unimplemented!("inimplemented instr: {}", name),
        };
        Instr {
            code,
            database,
            snapshot,
            starts_with,
            selector,
        }
    }
}

struct StackResult {
    state: Option<(Bytes<'static>, TransactionState)>,
    data: FdbResult<Element<'static>>,
}
impl From<FdbError> for StackResult {
    fn from(err: FdbError) -> Self {
        StackResult {
            state: None,
            data: Err(err),
        }
    }
}
impl From<Element<'static>> for StackResult {
    fn from(data: Element<'static>) -> Self {
        StackResult {
            state: None,
            data: Ok(data),
        }
    }
}

impl From<FdbResult<Element<'static>>> for StackResult {
    fn from(data: FdbResult<Element<'static>>) -> Self {
        StackResult { state: None, data }
    }
}

type StackFuture = Pin<Box<dyn Future<Output = StackResult>>>;
struct StackItem {
    number: usize,
    data: Option<Element<'static>>,
    fut: Option<(usize, StackFuture)>,
}

impl StackItem {
    async fn await_fut(&mut self) -> Option<(Bytes<'static>, TransactionState)> {
        let mut ret = None;
        if let Some((_id, fut)) = self.fut.take() {
            let StackResult { state, mut data } = fut.await;

            if let Some((name, state)) = state {
                trace!("{name:?} = {state:?}");
                match state {
                    TransactionState::TransactionCommitError(e) => {
                        let err = FdbError::from_code(e.code());
                        ret = Some((name, TransactionState::TransactionCommitError(e)));
                        data = Err(err);
                    }
                    state => {
                        ret = Some((name, state));
                    }
                }
            }

            let data = data.unwrap_or_else(|err| {
                trace!("ERROR {err:?}");
                let packed = pack(&(
                    Bytes::from(b"ERROR".as_ref()),
                    Bytes::from(format!("{}", err.code()).into_bytes()),
                ));
                Element::Bytes(packed.into())
            });

            self.data = Some(data);
        }
        ret
    }
}

impl Clone for StackItem {
    fn clone(&self) -> Self {
        if self.fut.is_some() {
            panic!("cannot clone future stack item");
        }
        Self {
            number: self.number,
            data: self.data.clone(),
            fut: None,
        }
    }
}

impl std::fmt::Debug for StackItem {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "[item num={}, data={:?}]", self.number, self.data)
    }
}

fn range(prefix: Bytes) -> (Bytes<'static>, Bytes<'static>) {
    let begin = prefix.clone().into_owned();
    let end = strinc(prefix).into_owned();

    (begin.into(), end.into())
}

enum TransactionState {
    Transaction(Transaction),
    TransactionCancelled(TransactionCancelled),
    TransactionCommitted(TransactionCommitted),
    TransactionCommitError(TransactionCommitError),
    Pending(usize),
    Dead,
}
impl std::fmt::Debug for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use TransactionState as S;

        match self {
            S::Transaction(..) => "Transaction",
            S::TransactionCancelled(..) => "TransactionCancelled",
            S::TransactionCommitted(..) => "TransactionCommitted",
            S::TransactionCommitError(..) => "TransactionCommitError",
            S::Pending(..) => "Pending",
            S::Dead => "Dead",
        }
        .fmt(f)
    }
}

impl TransactionState {
    fn reset(&mut self) {
        use TransactionState as S;
        *self = match std::mem::replace(self, S::Dead) {
            S::TransactionCommitted(c) => S::Transaction(c.reset()),
            S::TransactionCommitError(c) => S::Transaction(c.reset()),
            c => c,
        }
    }

    fn as_mut(&mut self) -> &mut Transaction {
        use TransactionState as S;

        self.reset();
        match *self {
            S::Transaction(ref mut tr) => tr,
            S::TransactionCancelled(ref mut tr) => unsafe {
                // rust binding prevent accessing cancelled transaction
                &mut *(tr as *mut TransactionCancelled as *mut Transaction)
            },
            _ => panic!("transaction is owned by a future that is still not done"),
        }
    }

    fn take(&mut self, id: usize) -> Transaction {
        use TransactionState as S;

        self.reset();
        match std::mem::replace(self, S::Dead) {
            S::Transaction(tr) => {
                *self = S::Pending(id);
                tr
            }
            S::TransactionCancelled(tr) => {
                *self = S::Pending(id);
                unsafe {
                    // rust binding prevent accessing cancelled transaction
                    transmute::<TransactionCancelled, Transaction>(tr)
                }
            }
            _ => panic!("transaction is owned by a future that is still not done"),
        }
    }
}

struct StackMachine {
    prefix: Bytes<'static>,

    // A global transaction map from byte string to Transactions. This map is shared by all tester
    // 'threads'.
    transactions: HashMap<Bytes<'static>, TransactionState>,

    // A stack of data items of mixed types and their associated metadata. At a minimum, each item
    // should be stored with the 0-based instruction number which resulted in it being put onto the
    // stack. Your stack must support push and pop operations. It may be helpful if it supports
    // random access, clear and a peek operation. The stack is initialized to be empty.
    stack: Vec<StackItem>,

    // A current FDB transaction name (stored as a byte string). The transaction name should be
    // initialized to the prefix that instructions are being read from.
    cur_transaction: Bytes<'static>,

    // A last seen FDB version, which is a 64-bit integer.
    last_version: i64,

    threads: Vec<thread::JoinHandle<()>>,

    trx_counter: usize,

    directory_stack: Vec<DirectoryStackItem>,
    directory_index: usize,
    error_index: usize,
}

fn strinc(key: Bytes) -> Bytes {
    let mut key = key.into_owned();
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            return Bytes::from(key);
        } else {
            // stripping key from trailing 0xFF bytes
            key.remove(i);
        }
    }
    panic!("failed to strinc");
}

impl StackMachine {
    fn new(db: &Database, prefix: Bytes<'static>) -> Self {
        let cur_transaction = prefix.clone();
        let mut transactions = HashMap::new();
        transactions.insert(
            cur_transaction.clone(),
            TransactionState::Transaction(db.create_trx().unwrap()),
        );

        Self {
            prefix,
            transactions,
            stack: Vec::new(),
            cur_transaction,
            last_version: 0,
            threads: Vec::new(),
            trx_counter: 0,

            directory_stack: vec![DirectoryStackItem::Directory(DirectoryLayer::default())],
            directory_index: 0,
            error_index: 0,
        }
    }

    fn next_trx_id(&mut self) -> usize {
        self.trx_counter += 1;
        self.trx_counter
    }

    async fn fetch_instr(&self, trx: &Transaction) -> FdbResult<Vec<Instr>> {
        let opt = RangeOption::from(&Subspace::from(&self.prefix));
        debug!("opt = {opt:?}");
        let instrs = Vec::new();
        trx.get_ranges(opt, false)
            .try_fold(instrs, |mut instrs, res| {
                for kv in res.iter() {
                    let instr = Instr::from(kv.value());
                    instrs.push(instr);
                }
                future::ok(instrs)
            })
            .await
    }

    async fn pop(&mut self) -> StackItem {
        let mut item = self.stack.pop().expect("stack empty");
        if let Some((name, state)) = item.await_fut().await {
            self.transactions.insert(name, state);
        }
        item
    }

    async fn maybe_pop(&mut self) -> Option<StackItem> {
        let mut item = self.stack.pop()?;
        if let Some((name, state)) = item.await_fut().await {
            self.transactions.insert(name, state);
        }
        Some(item)
    }

    async fn pop_i64(&mut self) -> i64 {
        let element = self.pop_element().await;
        match element {
            Element::Int(v) => v,
            _ => panic!("i64 was expected, found {element:?}"),
        }
    }

    async fn pop_i32(&mut self) -> i32 {
        i32::try_from(self.pop_i64().await).expect("i64 to fit in i32")
    }

    async fn pop_usize(&mut self) -> usize {
        usize::try_from(self.pop_i64().await).expect("i64 to fit in usize")
    }

    async fn pop_str(&mut self) -> String {
        let element = self.pop_element().await;
        match element {
            Element::String(v) => v.into_owned(),
            _ => panic!("string was expected, found {element:?}"),
        }
    }

    async fn pop_bytes(&mut self) -> Bytes<'static> {
        let element = self.pop_element().await;
        match element {
            Element::Bytes(v) => v,
            // checking nested bytes in a Tuple as well
            Element::Tuple(elements) => match elements.first() {
                Some(Element::Bytes(b)) => b.clone(),
                _ => panic!("bytes were expected, found a Tuple with no Bytes in it"),
            },
            _ => panic!("bytes were expected, found {element:?}"),
        }
    }

    async fn pop_optional_bytes(&mut self) -> Option<Vec<u8>> {
        let element = self.pop_element().await;
        match element {
            Element::Bytes(v) => Some(v.to_vec()),
            _ => None,
        }
    }

    async fn pop_string_tuple(&mut self, count: usize) -> Vec<Vec<String>> {
        let mut result = vec![];

        if count == 0 {
            result.push(vec![]);
        } else {
            for _i in 0..count {
                let mut sub_result = vec![];
                let vec_size = self.pop_i64().await;
                for _j in 0..vec_size {
                    sub_result.push(self.pop_str().await);
                }
                result.push(sub_result);
            }
        }
        result
    }

    async fn pop_element(&mut self) -> Element<'static> {
        let item = self.pop().await;
        if let Some(data) = item.data {
            return data;
        }
        panic!("no data");
    }

    async fn pop_selector(&mut self) -> KeySelector<'static> {
        let key: Bytes = self.pop_bytes().await;
        let or_equal: i32 = i32::try_from(self.pop_i64().await).unwrap();
        let offset: i32 = i32::try_from(self.pop_i64().await).unwrap();

        KeySelector::new(key.0, or_equal != 0, offset)
    }

    fn push(&mut self, number: usize, data: Element<'static>) {
        self.stack.push(StackItem {
            number,
            data: Some(data),
            fut: None,
        });
    }

    fn push_fut(&mut self, number: usize, id: usize, fut: StackFuture) {
        let item = StackItem {
            number,
            data: None,
            fut: Some((id, fut)),
        };
        self.stack.push(item);
    }

    fn push_res(&mut self, number: usize, res: FdbResult<()>, ok_str: &[u8]) {
        match res {
            Ok(..) => self.push(number, Element::Bytes(ok_str.to_vec().into())),
            Err(err) => self.push_err(number, err),
        }
    }

    fn push_directory_err(&mut self, code: &InstrCode, number: usize, err: DirectoryError) {
        debug!("[{number}] DIRECTORY_ERROR during {code:?}: {err:?}");
        self.push(number, Element::Tuple(vec![ERROR_DIRECTORY.clone()]));

        if matches!(
            code,
            InstrCode::DirectoryCreateSubspace
                | InstrCode::DirectoryCreateOrOpen
                | InstrCode::DirectoryCreateLayer
                | InstrCode::DirectoryCreate
                | InstrCode::DirectoryOpen
                | InstrCode::DirectoryMove
                | InstrCode::DirectoryMoveTo
                | InstrCode::DirectoryOpenSubspace
        ) {
            debug!(
                "pushed NULL in the directory_stack at index {} because of the error",
                self.directory_stack.len()
            );
            self.directory_stack.push(DirectoryStackItem::Null);
        }
    }

    fn push_err(&mut self, number: usize, err: FdbError) {
        trace!("ERROR {err:?}");
        let packed = pack(&Element::Tuple(vec![
            Element::Bytes(Bytes::from(b"ERROR".as_ref())),
            Element::Bytes(Bytes::from(format!("{}", err.code()).into_bytes())),
        ]));
        self.push(number, Element::Bytes(packed.into()));
    }

    fn check<T>(&mut self, number: usize, r: FdbResult<T>) -> Result<T, ()> {
        match r {
            Ok(v) => Ok(v),
            Err(err) => {
                self.push_err(number, err);
                Err(())
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn run_step(
        &mut self,
        db: Arc<Database>,
        number: usize,
        mut instr: Instr,
    ) -> Result<(), ()> {
        use crate::InstrCode::*;

        let is_db = instr.pop_database();

        let mut mutation = false;
        let mut pending = false;

        let (mut trx, trx_name) = if is_db {
            (
                TransactionState::Transaction(self.check(number, db.create_trx())?),
                None,
            )
        } else {
            let mut trx = self
                .transactions
                .remove(&self.cur_transaction) // some instr requires transaction ownership
                .expect("failed to find trx");
            if let TransactionState::Pending(id) = trx {
                let stack_item = self.stack.iter_mut().find(|s| match s.fut {
                    Some((trx_id, ..)) => trx_id == id,
                    _ => false,
                });
                if let Some(stack_item) = stack_item {
                    if let Some((name, state)) = stack_item.await_fut().await {
                        assert_eq!(name, self.cur_transaction);
                        trx = state;
                    }
                }
            }
            (trx, Some(&self.cur_transaction))
        };

        match instr.code {
            // Pushes the provided item onto the stack.
            Push(ref element) => {
                debug!("push {element:?}");
                self.push(number, element.clone())
            }
            // Duplicates the top item on the stack. The instruction number for
            // the duplicate item should be the same as the original.
            Dup => {
                let top = self.pop().await;
                debug!("dup {top:?}");
                self.stack.push(top.clone());
                self.stack.push(top);
            }
            // Discards all items in the stack.
            EmptyStack => {
                debug!("empty_stack");
                self.stack.clear()
            }
            // Pops the top item off of the stack as INDEX.
            // Swaps the items in the stack at depth 0 and depth INDEX.
            // Does not modify the instruction numbers of the swapped items.
            Swap => {
                let depth: usize = self.pop_usize().await;
                assert!(
                    depth < self.stack.len(),
                    "swap index invalid {} >= {}",
                    depth,
                    self.stack.len()
                );
                let depth_0 = self.stack.len() - 1;
                let depth = depth_0 - depth;
                debug!("swap {depth_0} {depth}");
                self.stack.swap(depth_0, depth);
            }
            // Pops and discards the top item on the stack.
            Pop => {
                debug!("pop");
                self.pop().await;
            }
            // Pops the top two items off of the stack as A and B and then
            // pushes the difference (A-B) onto the stack.
            // A and B may be assumed to be integers.
            Sub => {
                let a = self.pop_element().await;
                let b = self.pop_element().await;
                debug!("sub {a:?} - {b:?}");
                let c = match (a, b) {
                    // checking for possible overflow
                    (Element::Int(a), Element::Int(b)) => match a.checked_sub(b) {
                        None => Element::BigInt(BigInt::from(a) - BigInt::from(b)),
                        Some(c) => Element::Int(c),
                    },
                    (Element::Float(a), Element::Float(b)) => Element::Float(a - b),
                    (Element::Double(a), Element::Double(b)) => Element::Double(a - b),
                    (Element::BigInt(a), Element::BigInt(b)) => Element::BigInt(a - b),
                    (Element::Int(a), Element::BigInt(b)) => Element::BigInt(a - b),
                    (Element::BigInt(a), Element::Int(b)) => Element::BigInt(a - b),
                    (a, b) => panic!("sub between invalid elements {a:?} {b:?}"),
                };
                self.push(number, c);
            }
            // Pops the top two items off the stack as A and B and then pushes
            // the concatenation of A and B onto the stack. A and B can be
            // assumed to be of the same type and will be either byte strings or
            // unicode strings.
            Concat => {
                let a = self.pop_element().await;
                let b = self.pop_element().await;
                debug!("concat {a:?} {b:?}");
                let c = match (a, b) {
                    (Element::String(a), Element::String(b)) => {
                        Element::String(format!("{a}{b}").into())
                    }
                    (Element::Bytes(a), Element::Bytes(b)) => {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(&a);
                        bytes.extend_from_slice(&b);
                        Element::Bytes(bytes.into())
                    }
                    (a, b) => panic!("concat between invalid elements {a:?} {b:?}"),
                };
                self.push(number, c);
            }
            // Pops the top item off the stack as PREFIX. Using a new
            // transaction with normal retry logic, inserts a key-value pair
            // into the database for each item in the stack of the form:
            //
            // PREFIX + tuple.pack((stackIndex, instructionNumber)) = tuple.pack((item,))
            //
            // where stackIndex is the current index of the item in the stack.
            // The oldest item in the stack should have stackIndex 0.
            //
            // If the byte string created by tuple packing the item exceeds 40000 bytes,
            // then the value should be truncated to the first 40000 bytes of the packed
            // tuple.
            //
            // When finished, the stack should be empty. Note that because the stack may be
            // large, it may be necessary to commit the transaction every so often (e.g.
            // after every 100 sets) to avoid past_version errors.
            LogStack => {
                let prefix: Bytes = self.pop_bytes().await;
                let mut stack_idx = self.stack.len();
                let trx_id = self.next_trx_id();
                let mut t = trx.take(trx_id);
                let mut n = 0;
                while let Some(stack_item) = self.maybe_pop().await {
                    stack_idx -= 1;
                    let mut key = prefix.clone().into_owned();
                    stack_idx.pack_into_vec(&mut key);
                    stack_item.number.pack_into_vec(&mut key);

                    let value = pack(&stack_item.data.unwrap());
                    t.set(&key, &value[..value.len().min(40000)]);
                    n += 1;
                    if n == 100 {
                        t = t.commit().await.unwrap().reset();
                        n = 0;
                    }
                }
                t = t.commit().await.unwrap().reset();
                trx = TransactionState::Transaction(t);
            }

            // Creates a new transaction and stores it in the global transaction map
            // under the currently used transaction name.
            NewTransaction => {
                let name = self.cur_transaction.clone();
                debug!("create_trx {name:?}");

                let trx = self.check(number, db.create_trx())?;
                trx.set_option(fdb::options::TransactionOption::DebugTransactionIdentifier(
                    "RUST".to_string(),
                ))
                .unwrap();
                trx.set_option(fdb::options::TransactionOption::LogTransaction)
                    .unwrap();
                self.transactions
                    .insert(name, TransactionState::Transaction(trx));
            }

            // Pop the top item off of the stack as TRANSACTION_NAME. Begin using the
            // transaction stored at TRANSACTION_NAME in the transaction map for future
            // operations. If no entry exists in the map for the given name, a new
            // transaction should be inserted.
            UseTransaction => {
                let name: Bytes = self.pop_bytes().await;
                debug!("use_transaction {name:?}");
                if !is_db {
                    // Popping the name may have resolved a pending future and restored the
                    // current transaction already. Keep that newer state if it exists.
                    self.transactions
                        .entry(self.cur_transaction.clone())
                        .or_insert_with(|| std::mem::replace(&mut trx, TransactionState::Dead));
                }
                if !self.transactions.contains_key(&name) {
                    let trx = self.check(number, db.create_trx())?;
                    self.transactions
                        .insert(name.clone(), TransactionState::Transaction(trx));
                }
                self.cur_transaction = name;
            }
            // Pops the top item off of the stack as ERROR_CODE. Passes ERROR_CODE in a
            // language-appropriate way to the on_error method of current transaction
            // object and blocks on the future. If on_error re-raises the error, bubbles
            // the error out as indicated above. May optionally push a future onto the
            // stack.
            OnError => {
                let trx_name = trx_name.cloned();
                let error_code: i32 = self.pop_i32().await;
                let error = FdbError::from_code(error_code);
                debug!("on_error {error:?}");
                let trx_id = self.next_trx_id();
                let f = trx
                    .take(trx_id)
                    .on_error(error)
                    .map(|res| match res {
                        Ok(trx) => StackResult {
                            state: trx_name.map(|n| (n, TransactionState::Transaction(trx))),
                            data: Ok(RESULT_NOT_PRESENT.clone().into_owned()),
                        },
                        Err(err) => StackResult::from(err),
                    })
                    .boxed_local();
                self.push_fut(number, 0, f);
                pending = true;
            }

            // Pops the top item off of the stack as KEY and then looks up KEY in the
            // database using the get() method. May optionally push a future onto the
            // stack.
            Get => {
                let key: Bytes = self.pop_bytes().await;
                debug!("get {key:?}");
                let f = trx
                    .as_mut()
                    .get(&key, instr.pop_snapshot())
                    .map_ok(|v| match v {
                        Some(v) => Element::Bytes(v.to_vec().into()),
                        None => RESULT_NOT_PRESENT.clone().into_owned(),
                    })
                    .map(StackResult::from)
                    .boxed_local();
                self.push_fut(number, 0, f);
                pending = true;
            }

            // Pops the top four items off of the stack as KEY, OR_EQUAL, OFFSET, PREFIX
            // and then constructs a key selector. This key selector is then resolved
            // using the get_key() method to yield RESULT. If RESULT starts with PREFIX,
            // then RESULT is pushed onto the stack. Otherwise, if RESULT < PREFIX, PREFIX
            // is pushed onto the stack. If RESULT > PREFIX, then strinc(PREFIX) is pushed
            // onto the stack. May optionally push a future onto the stack.
            GetKey => {
                let selector = self.pop_selector().await;
                let prefix: Bytes = self.pop_bytes().await;
                debug!("get_key {selector:?}, prefix = {prefix:?}");

                let f = trx
                    .as_mut()
                    .get_key(&selector, instr.pop_snapshot())
                    .map_ok(|key| {
                        let key = Bytes::from(key.as_ref());
                        let bytes = if key.starts_with(&prefix) {
                            key
                        } else if key < prefix {
                            prefix
                        } else {
                            assert!(key > prefix);
                            strinc(prefix)
                        };
                        Element::Bytes(bytes.to_vec().into())
                    })
                    .map(StackResult::from)
                    .boxed_local();
                self.push_fut(number, 0, f);
                pending = true;
            }

            // Pops the top five items off of the stack as BEGIN_KEY, END_KEY, LIMIT,
            // REVERSE and STREAMING_MODE. Performs a range read in a language-appropriate
            // way using these parameters. The resulting range of n key-value pairs are
            // packed into a tuple as [k1,v1,k2,v2,...,kn,vn], and this single packed value
            // is pushed onto the stack.
            //
            // _STARTS_WITH:
            //
            // Pops the top four items off of the stack as PREFIX, LIMIT, REVERSE and
            // STREAMING_MODE. Performs a prefix range read in a language-appropriate way
            // using these parameters. Output is pushed onto the stack as with GET_RANGE.
            //
            // _SELECTOR:
            //
            // Pops the top ten items off of the stack as BEGIN_KEY, BEGIN_OR_EQUAL,
            // BEGIN_OFFSET, END_KEY, END_OR_EQUAL, END_OFFSET, LIMIT, REVERSE,
            // STREAMING_MODE, and PREFIX. Constructs key selectors BEGIN and END from
            // the first six parameters, and then performs a range read in a language-
            // appropriate way using BEGIN, END, LIMIT, REVERSE and STREAMING_MODE. Output
            // is pushed onto the stack as with GET_RANGE, excluding any keys that do not
            // begin with PREFIX.
            GetRange => {
                let selector = instr.pop_selector();
                let starts_with = instr.pop_starts_with();
                let snapshot = instr.pop_snapshot();

                let (begin, end) = if starts_with {
                    let begin: Bytes = self.pop_bytes().await;
                    let end = strinc(begin.clone());
                    (
                        KeySelector::first_greater_or_equal(begin.0),
                        KeySelector::first_greater_or_equal(end.0),
                    )
                } else if selector {
                    let begin = self.pop_selector().await;
                    let end = self.pop_selector().await;
                    (begin, end)
                } else {
                    let begin: Bytes = self.pop_bytes().await;
                    let end: Bytes = self.pop_bytes().await;
                    (
                        KeySelector::first_greater_or_equal(begin.0),
                        KeySelector::first_greater_or_equal(end.0),
                    )
                };

                let limit: i64 = self.pop_i64().await;
                let reverse: i64 = self.pop_i64().await;
                let streaming_mode: i32 = self.pop_i32().await;
                let mode = streaming_from_value(streaming_mode);
                debug!(
                    "get_range begin={:?}\n, begin={:?}\n, end={:?}\n,end={:?}\n, limit={:?}, rev={:?}, mode={:?}",
                    begin,
                    unpack::<Element>(begin.key()),
                    end,
                    unpack::<Element>(end.key()),
                    limit,
                    reverse,
                    mode
                );

                let prefix: Option<Bytes> = if selector {
                    Some(self.pop_bytes().await)
                } else {
                    None
                };
                let opt = RangeOption {
                    begin,
                    end,
                    mode,
                    limit: if limit > 0 {
                        Some(limit as usize)
                    } else {
                        None
                    },
                    reverse: reverse != 0,
                    ..RangeOption::default()
                };

                let res = trx
                    .as_mut()
                    .get_ranges(opt, snapshot)
                    .try_fold(Vec::new(), move |mut out, kvs| {
                        for kv in kvs.iter() {
                            let key = kv.key();
                            let value = kv.value();
                            debug!(" - {:?} {:?}", Bytes::from(key), Bytes::from(value));
                            if let Some(ref prefix) = prefix {
                                if !key.starts_with(prefix) {
                                    continue;
                                }
                            }
                            pack_into(&Bytes::from(key), &mut out);
                            pack_into(&Bytes::from(value), &mut out);
                        }
                        future::ok(out)
                    })
                    .map_ok(|data| Element::Bytes(data.into()))
                    .await;
                match res {
                    Err(err) => self.push_err(number, err),
                    Ok(element) => self.push(number, element),
                }
            }

            // Gets the current read version and stores it in the internal stack machine
            // state as the last seen version. Pushed the string "GOT_READ_VERSION" onto
            // the stack.
            GetReadVersion => {
                let _snapshot = instr.pop_snapshot();
                let version = trx.as_mut().get_read_version().await;
                match version {
                    Ok(version) => {
                        self.last_version = version;
                        self.push(number, GOT_READ_VERSION.clone().into_owned());
                    }
                    Err(err) => self.push_err(number, err),
                }
            }

            // Calls get_versionstamp and pushes the resulting future onto the stack.
            GetVersionstamp => {
                let f = trx
                    .as_mut()
                    .get_versionstamp()
                    .map_ok(|v| Element::Bytes(v.to_vec().into()))
                    .map(StackResult::from)
                    .boxed_local();
                self.push_fut(number, 0, f);
                pending = true;
            }

            // Pops the top two items off of the stack as KEY and VALUE. Sets KEY to have
            // the value VALUE. A SET_DATABASE call may optionally push a future onto the
            // stack.
            Set => {
                let key: Bytes = self.pop_bytes().await;
                let value: Bytes = self.pop_bytes().await;
                debug!("set {key:?} {value:?}");
                trx.as_mut().set(&key, &value);
                mutation = true;
            }

            // Sets the current transaction read version to the internal state machine last
            // seen version.
            SetReadVersion => {
                debug!("set_read_version {:?}", self.last_version);
                trx.as_mut().set_read_version(self.last_version);
            }

            // Pops the top item off of the stack as KEY and then clears KEY from the
            // database. A CLEAR_DATABASE call may optionally push a future onto the stack.
            Clear => {
                let key: Bytes = self.pop_bytes().await;
                debug!("clear {key:?}");
                trx.as_mut().clear(&key);
                mutation = true;
            }

            // CLEAR_RANGE
            // Pops the top two items off of the stack as BEGIN_KEY and END_KEY. Clears the
            // range of keys from BEGIN_KEY to END_KEY in the database. A
            // CLEAR_RANGE_DATABASE call may optionally push a future onto the stack.
            //
            // CLEAR_RANGE_STARTS_WITH
            // Pops the top item off of the stack as PREFIX and then clears all keys from
            // the database that begin with PREFIX. A CLEAR_RANGE_STARTS_WITH_DATABASE call
            // may optionally push a future onto the stack.
            ClearRange => {
                let starts_with = instr.pop_starts_with();
                let (begin, end) = if starts_with {
                    let prefix = self.pop_bytes().await;
                    range(prefix)
                } else {
                    let begin: Bytes = self.pop_bytes().await;
                    let end: Bytes = self.pop_bytes().await;
                    (begin, end)
                };
                debug!("clear_range {begin:?} {end:?}");
                trx.as_mut().clear_range(&begin, &end);
                mutation = true;
            }

            // Pops the top three items off of the stack as OPTYPE, KEY, and VALUE.
            // Performs the atomic operation described by OPTYPE upon KEY with VALUE. An
            // ATOMIC_OP_DATABASE call may optionally push a future onto the stack.
            AtomicOp => {
                let optype: String = self.pop_str().await;
                let key: Bytes = self.pop_bytes().await;
                let value: Bytes = self.pop_bytes().await;
                debug!("atomic_op {key:?} {value:?} {optype:?}");

                let op = mutation_from_str(&optype);
                trx.as_mut().atomic_op(&key, &value, op);
                mutation = true;
            }

            // Pops the top two items off of the stack as BEGIN_KEY and END_KEY. Adds a
            // read conflict range or write conflict range from BEGIN_KEY to END_KEY.
            // Pushes the byte string "SET_CONFLICT_RANGE" onto the stack.
            ReadConflictRange => {
                let begin: Bytes = self.pop_bytes().await;
                let end: Bytes = self.pop_bytes().await;
                debug!("read_conflict_range {begin:?} {end:?}");
                self.push_res(
                    number,
                    trx.as_mut()
                        .add_conflict_range(&begin, &end, ConflictRangeType::Read),
                    b"SET_CONFLICT_RANGE",
                );
            }
            WriteConflictRange => {
                let begin: Bytes = self.pop_bytes().await;
                let end: Bytes = self.pop_bytes().await;
                debug!("write_conflict_range {begin:?} {end:?}");
                self.push_res(
                    number,
                    trx.as_mut()
                        .add_conflict_range(&begin, &end, ConflictRangeType::Write),
                    b"SET_CONFLICT_RANGE",
                );
            }
            // Pops the top item off of the stack as KEY. Adds KEY as a read conflict key
            // or write conflict key. Pushes the byte string "SET_CONFLICT_KEY" onto the
            // stack.
            ReadConflictKey => {
                let begin: Bytes = self.pop_bytes().await;
                let mut end = begin.clone().into_owned();
                end.push(0);
                debug!("read_conflict_key {begin:?} {end:?}");
                self.push_res(
                    number,
                    trx.as_mut()
                        .add_conflict_range(&begin, &end, ConflictRangeType::Read),
                    b"SET_CONFLICT_KEY",
                );
            }
            WriteConflictKey => {
                let begin: Bytes = self.pop_bytes().await;
                let mut end = begin.clone().into_owned();
                end.push(0);
                debug!("write_conflict_key {begin:?} {end:?}");
                self.push_res(
                    number,
                    trx.as_mut()
                        .add_conflict_range(&begin, &end, ConflictRangeType::Write),
                    b"SET_CONFLICT_KEY",
                );
            }
            // Sets the NEXT_WRITE_NO_WRITE_CONFLICT_RANGE transaction option on the
            // current transaction. Does not modify the stack.
            DisableWriteConflict => {
                debug!("disable_write_conflict");
                self.check(
                    number,
                    trx.as_mut()
                        .set_option(TransactionOption::NextWriteNoWriteConflictRange),
                )?
            }
            // Commits the current transaction (with no retry behavior). May optionally
            // push a future onto the stack.
            Commit => {
                debug!("commit");
                let trx_name = trx_name.cloned();
                let trx_id = self.next_trx_id();
                let f = trx
                    .take(trx_id)
                    .commit()
                    .map(|r| match r {
                        Ok(c) => StackResult {
                            state: trx_name.map(|n| (n, TransactionState::TransactionCommitted(c))),
                            data: Ok(RESULT_NOT_PRESENT.clone().into_owned()),
                        },
                        Err(c) => {
                            let err = FdbError::from_code(c.code());
                            StackResult {
                                state: trx_name
                                    .map(|n| (n, TransactionState::TransactionCommitError(c))),
                                data: Err(err),
                            }
                        }
                    })
                    .boxed_local();
                self.push_fut(number, trx_id, f);
                pending = true;
            }
            // Resets the current transaction.
            Reset => {
                debug!("reset");
                trx.as_mut().reset();
            }
            // Cancels the current transaction.
            Cancel => {
                debug!("cancel");
                let cancelled = trx.take(0).cancel();
                trx = TransactionState::TransactionCancelled(cancelled);
            }

            // Gets the committed version from the current transaction and stores it in the
            // internal stack machine state as the last seen version. Pushes the byte
            // string "GOT_COMMITTED_VERSION" onto the stack.
            GetCommittedVersion => {
                debug!("committed_version");
                if let TransactionState::TransactionCommitted(t) = &trx {
                    let last_version = t
                        .committed_version()
                        .expect("failed to get committed version");
                    self.last_version = last_version;
                    self.push(number, GOT_COMMITTED_VERSION.clone().into_owned());
                } else {
                    warn!("committed_version() called on a non commited transaction");
                    self.last_version = -1;
                    self.push(number, GOT_COMMITTED_VERSION.clone().into_owned());
                }
            }

            // Calls get_approximate_size and pushes the byte string "GOT_APPROXIMATE_SIZE"
            // onto the stack. Note bindings may issue GET_RANGE calls with different
            // limits, so these bindings can obtain different sizes back.
            GetApproximateSize => {
                debug!("get_approximate_size");
                match trx.as_mut().get_approximate_size().await {
                    Ok(_) => self.push(number, GOT_APPROXIMATE_SIZE_RESPONSE.clone().into_owned()),
                    Err(error) => self.push_err(number, error),
                }
            }

            GetRangeSplitPoints => {
                debug!("get_range_split_points");
                let begin: Bytes = self.pop_bytes().await;
                let end: Bytes = self.pop_bytes().await;
                let chunk_size = self.pop_i64().await;

                match trx
                    .as_mut()
                    .get_range_split_points(&begin, &end, chunk_size)
                    .await
                {
                    Ok(_) => {
                        self.push(number, GET_RANGE_SPLIT_POINTS_RESPONSE.clone().into_owned())
                    }
                    Err(error) => self.push_err(number, error),
                }
            }

            // Pops the top item off the stack and pushes it back on. If the top item on
            // the stack is a future, this will have the side effect of waiting on the
            // result of the future and pushing the result on the stack. Does not change
            // the instruction number of the item.
            WaitFuture => {
                debug!("wait_future");
                let item = self.pop().await;
                self.stack.push(item);
            }

            // Pops the top two items off of the stack as BEGIN_KEY and END_KEY to
            // construct a key range. Then call the `getEstimatedRangeSize` API of
            // the language binding. Make sure the API returns without error. Finally
            // push the string "GOT_ESTIMATED_RANGE_SIZE" onto the stack.
            GetEstimatedRangeSize => {
                debug!("get estimated range size");
                let begin = self.pop_bytes().await;
                let end = self.pop_bytes().await;

                match trx
                    .as_mut()
                    .get_estimated_range_size_bytes(&begin, &end)
                    .await
                {
                    Ok(estimate) => {
                        debug!("got an estimate of {estimate} bytes");
                        self.push(number, ESTIMATE_RANGE_RESPONSE.clone().into_owned());
                    }
                    Err(error) => {
                        self.push_err(number, error);
                    }
                };
            }

            // Pops the top item off of the stack as N. Pops the next N items off of the
            // stack and packs them as the tuple [item0,item1,...,itemN], and then pushes
            // this single packed value onto the stack.
            TuplePack => {
                let n: usize = self.pop_usize().await;
                debug!("tuple_pack {n}");
                let mut buf = Vec::new();
                for _ in 0..n {
                    let element: Element = self.pop_element().await;
                    debug!(" - {element:?}");
                    buf.push(element);
                }
                let tuple = Element::Tuple(buf);
                self.push(number, Element::Bytes(pack(&tuple).into()));
            }

            // Pops the top item off of the stack as a byte string prefix. Pops the next item
            // off of the stack as N. Pops the next N items off of the stack and packs them
            // as the tuple [item0,item1,...,itemN], with the provided prefix and tries to
            // append the position of the first incomplete versionstamp as if the byte
            // string were to be used as a key in a SET_VERSIONSTAMP_KEY atomic op. If there
            // are no incomplete versionstamp instances, then this pushes the literal byte
            // string 'ERROR: NONE' to the stack. If there is more than one, then this pushes
            // the literal byte string 'ERROR: MULTIPLE'. If there is exactly one, then it pushes
            // the literal byte string 'OK' and then pushes the packed tuple. (Languages that
            // do not contain a 'Versionstamp' tuple-type do not have to implement this
            // operation.)
            TuplePackWithVersionstamp => {
                let prefix = self.pop_bytes().await;
                let n: usize = self.pop_usize().await;
                debug!("tuple_pack_with_versionstamp {prefix:?} {n}");
                let mut buf = Vec::new();
                for _ in 0..n {
                    let element: Element = self.pop_element().await;
                    debug!(" - {element:?}");
                    buf.push(element);
                }

                let tuple = Element::Tuple(buf.clone());
                let i = tuple.count_incomplete_versionstamp();
                let mut vec = prefix.into_owned();
                let offset = buf.pack_into_vec_with_versionstamp(&mut vec);
                match offset {
                    VersionstampOffset::None { size: _ } => {
                        assert_eq!(i, 0);
                        self.push(number, ERROR_NONE.clone().into_owned());
                    }
                    VersionstampOffset::OneIncomplete { offset: _ } => {
                        assert_eq!(i, 1);
                        let data = Element::Bytes(vec.into());
                        self.push(number, OK.clone().into_owned());
                        self.push(number, data);
                    }
                    VersionstampOffset::MultipleIncomplete => {
                        assert!(i > 1);
                        self.push(number, ERROR_MULTIPLE.clone().into_owned());
                    }
                }
            }

            // Pops the top item off of the stack as PACKED, and then unpacks PACKED into a
            // tuple. For each element of the tuple, packs it as a new tuple and pushes it
            // onto the stack.
            TupleUnpack => {
                let data = self.pop_bytes().await;
                debug!("tuple_unpack {data:?}");
                let data: Vec<Element> = unpack(&data).unwrap();
                for element in data {
                    debug!(" - {element:?}");
                    self.push(number, Element::Bytes(pack(&(element,)).into()));
                }
            }
            // Pops the top item off of the stack as N. Pops the next N items off of the
            // stack, and passes these items as a tuple (or array, or language-appropriate
            // structure) to the tuple range method. Pushes the begin and end elements of
            // the returned range onto the stack.
            TupleRange => {
                let n: usize = self.pop_usize().await;
                debug!("tuple_range {n:?}");
                let mut tup = Vec::new();
                for _ in 0..n {
                    let element = self.pop_element().await;
                    tup.push(element);
                }

                let subspace = Subspace::from(tup);
                let (start, end) = subspace.range();
                self.push(number, Element::Bytes(start.into()));
                self.push(number, Element::Bytes(end.into()));
            }

            // Pops the top item off of the stack as N. Pops the next N items off of the
            // stack as packed tuples (i.e., byte strings), unpacks them, sorts the tuples,
            // repacks them into byte strings, and then pushes these packed tuples onto
            // the stack so that the final top of the stack now has the greatest
            // element. If the binding has some kind of tuple comparison function, it should
            // use that to sort. Otherwise, it should sort them lexicographically by
            // their byte representation. The choice of function should not affect final sort order.
            TupleSort => {
                let n: usize = self.pop_usize().await;
                debug!("tuple_sort {n:?}");
                let mut tup = Vec::<Element>::new();
                for _ in 0..n {
                    let packed = self.pop_bytes().await;
                    let element: Element = unpack(&packed).unwrap();
                    debug!("- {element:?}");
                    tup.push(element.into_owned());
                }
                tup.sort();
                debug!("tuple_sorted");
                for element in tup {
                    debug!("- {element:?}");
                    self.push(number, Element::Bytes(pack(&element).into()));
                }
            }

            // Pops the top item off of the stack. This will be a byte-string of length 4
            // containing the IEEE 754 encoding of a float in big-endian order.
            // This is then converted into a float and pushed onto the stack.
            EncodeFloat => {
                let bytes = self.pop_bytes().await;
                debug!("encode_float {bytes:?}");
                let mut arr = [0; 4];
                arr.copy_from_slice(&bytes);
                let f = f32::from_bits(u32::from_be_bytes(arr));
                self.push(number, Element::Float(f));
            }
            // Pops the top item off of the stack. This will be a byte-string of length 8
            // containing the IEEE 754 encoding of a double in big-endian order.
            // This is then converted into a double and pushed onto the stack.
            EncodeDouble => {
                let bytes = self.pop_bytes().await;
                debug!("encode_double {bytes:?}");
                let mut arr = [0; 8];
                arr.copy_from_slice(&bytes);
                let f = f64::from_bits(u64::from_be_bytes(arr));
                self.push(number, Element::Double(f));
            }
            // Pops the top item off of the stack. This will be a single-precision float.
            // This is converted into a (4 byte) byte-string of its IEEE 754 representation
            // in big-endian order, and pushed onto the stack.
            DecodeFloat => {
                let f: f32 = match self.pop_element().await {
                    Element::Float(v) => v,
                    element => panic!("float was expected, found {element:?}"),
                };
                debug!("decode_float {f}");
                self.push(
                    number,
                    Element::Bytes(f.to_bits().to_be_bytes().to_vec().into()),
                );
            }
            // Pops the top item off of the stack. This will be a double-precision float.
            // This is converted into a (8 byte) byte-string its IEEE 754 representation
            // in big-endian order, and pushed onto the stack.
            DecodeDouble => {
                let f: f64 = match self.pop_element().await {
                    Element::Double(v) => v,
                    element => panic!("double was expected, found {element:?}"),
                };
                debug!("decode_double {f}");
                self.push(
                    number,
                    Element::Bytes(f.to_bits().to_be_bytes().to_vec().into()),
                );
            }

            // Pops the top item off of the stack as PREFIX. Creates a new stack machine
            // instance operating on the same database as the current stack machine, but
            // operating on PREFIX. The new stack machine should have independent internal
            // state. The new stack machine should begin executing instructions concurrent
            // with the current stack machine through a language-appropriate mechanism.
            StartThread => {
                let prefix = self.pop_bytes().await;
                debug!("start_thread {prefix:?}");
                let db = db.clone();
                self.threads.push(
                    thread::Builder::new()
                        .name(format!("{prefix:?}"))
                        .spawn(move || {
                            let mut sm = StackMachine::new(&db, prefix.clone());
                            futures::executor::block_on(sm.run(db)).unwrap();
                            sm.join();
                            debug!("thread {prefix:?} exit");
                        })
                        .unwrap(),
                );
            }

            // Pops the top item off of the stack as PREFIX. Blocks execution until the
            // range with prefix PREFIX is not present in the database. This should be
            // implemented as a polling loop inside of a language- and binding-appropriate
            // retryable construct which synthesizes FoundationDB error 1020 when the range
            // is not empty. Pushes the string "WAITED_FOR_EMPTY" onto the stack when
            // complete.
            WaitEmpty => {
                let prefix = self.pop_bytes().await;
                debug!("wait_empty {prefix:?}");
                let (begin, end) = range(prefix);

                async fn wait_for_empty(
                    trx: &Transaction,
                    begin: &[u8],
                    end: &[u8],
                ) -> FdbResult<()> {
                    let range = trx
                        .get_range(&RangeOption::from((begin, end)), 1, false)
                        .await?;

                    debug!("wait_empty {:?} range {}", Bytes::from(begin), range.len());
                    if !range.is_empty() {
                        return Err(FdbError::from_code(1020));
                    }
                    Ok(())
                }
                let r = db
                    .transact_boxed_local(
                        (begin, end),
                        |trx, (begin, end)| wait_for_empty(trx, begin, end).boxed_local(),
                        TransactOption::default(),
                    )
                    .await;
                self.check(number, r)?;
                self.push(number, WAITED_FOR_EMPTY.clone().into_owned());
            }

            UnitTests => {
                async fn unit_test(db: &fdb::Database, tr: &mut fdb::Transaction) -> FdbResult<()> {
                    db.set_option(DatabaseOption::LocationCacheSize(100_001))?;
                    db.set_option(DatabaseOption::MaxWatches(100_001))?;
                    db.set_option(DatabaseOption::DatacenterId("dc_id".to_string()))?;
                    db.set_option(DatabaseOption::MachineId("machine_id".to_string()))?;
                    db.set_option(DatabaseOption::TransactionTimeout(100_000))?;
                    db.set_option(DatabaseOption::TransactionTimeout(0))?;
                    db.set_option(DatabaseOption::TransactionTimeout(0))?;
                    db.set_option(DatabaseOption::TransactionMaxRetryDelay(100))?;
                    db.set_option(DatabaseOption::TransactionRetryLimit(10))?;
                    db.set_option(DatabaseOption::TransactionRetryLimit(-1))?;
                    db.set_option(DatabaseOption::SnapshotRywEnable)?;
                    db.set_option(DatabaseOption::SnapshotRywDisable)?;

                    tr.set_option(TransactionOption::PrioritySystemImmediate)?;
                    tr.set_option(TransactionOption::PriorityBatch)?;
                    tr.set_option(TransactionOption::CausalReadRisky)?;
                    tr.set_option(TransactionOption::CausalWriteRisky)?;
                    tr.set_option(TransactionOption::ReadYourWritesDisable)?;
                    tr.set_option(TransactionOption::ReadSystemKeys)?;
                    tr.set_option(TransactionOption::AccessSystemKeys)?;
                    tr.set_option(TransactionOption::Timeout(60 * 1000))?;
                    tr.set_option(TransactionOption::RetryLimit(50))?;
                    tr.set_option(TransactionOption::MaxRetryDelay(100))?;
                    tr.set_option(TransactionOption::UsedDuringCommitProtectionDisable)?;
                    tr.set_option(TransactionOption::DebugTransactionIdentifier(
                        "my_transaction".to_string(),
                    ))?;
                    tr.set_option(TransactionOption::LogTransaction)?;
                    tr.set_option(TransactionOption::ReadLockAware)?;
                    tr.set_option(TransactionOption::LockAware)?;

                    tr.get(b"\xff", false).await?;

                    Ok(())
                }
                let r = unit_test(&db, trx.as_mut()).await;
                self.check(number, r)?;

                // TODO
                // test_cancellation(db)
                // test_retry_limits(db)
                // test_db_retry_limits(db)
                // test_timeouts(db)
                // test_db_timeouts(db)
                // test_combinations(db)
                // test_locality(db)
                // test_predicates()
            }

            // Pop 1 tuple off the stack as [path]. Pop 1 additional item as [raw_prefix].
            // Create a subspace with path as the prefix tuple and the specified
            // raw_prefix. Append it to the directory list.
            DirectoryCreateSubspace => {
                let tuple_prefix = self.pop_string_tuple(1).await;
                let raw_prefix = self.pop_bytes().await;
                let subspace = Subspace::from_bytes(raw_prefix.into_owned())
                    .subspace(tuple_prefix.first().unwrap());
                debug!(
                    "pushing a new subspace {:?} at index {}",
                    subspace,
                    self.directory_stack.len()
                );
                self.directory_stack
                    .push(DirectoryStackItem::Subspace(subspace));
            }

            // Pop 3 items off the stack as [index1, index2, allow_manual_prefixes]. Let
            // node_subspace be the object in the directory list at index1 and
            // content_subspace be the object in the directory list at index2. Create a new
            // directory layer with the specified node_subspace and content_subspace. If
            // allow_manual_prefixes is 1, then enable manual prefixes on the directory
            // layer. Append the resulting directory layer to the directory list.
            //
            // If either of the two specified subspaces are null, then do not create a
            // directory layer and instead push null onto the directory list.
            DirectoryCreateLayer => {
                let index_node_subspace = self.pop_usize().await;
                let index_content_subspace = self.pop_usize().await;
                let allow_manual_prefixes = self.pop_i64().await == 1;

                let node_subspace = self.directory_stack.get(index_node_subspace);
                let content_subspace = self.directory_stack.get(index_content_subspace);

                match (node_subspace, content_subspace) {
                    (Some(node_subspace), Some(content_subspace)) => {
                        let node_subspace = match node_subspace {
                            DirectoryStackItem::Subspace(s) => s.to_owned(),
                            _ => panic!("expecting subspace"),
                        };

                        let content_subspace = match content_subspace {
                            DirectoryStackItem::Subspace(s) => s.to_owned(),
                            _ => panic!("expecting subspace"),
                        };

                        debug!("pushed a directory at index {}", self.directory_stack.len());

                        self.directory_stack.push(DirectoryStackItem::Directory(
                            DirectoryLayer::new(
                                node_subspace,
                                content_subspace,
                                allow_manual_prefixes,
                            ),
                        ));
                    }
                    (node_subspace, content_subspace) => {
                        error!(
                            "pushing null on the directory list: {}, {}",
                            node_subspace.is_some(),
                            content_subspace.is_some()
                        );
                        self.directory_stack.push(DirectoryStackItem::Null);
                    }
                }
            }

            // Pop 1 tuple off the stack as [path]. Pop 2 additional items as
            // [layer, prefix]. create a directory with the specified path, layer,
            // and prefix. If either of layer or prefix is null, use the default value for
            // that parameter (layer='', prefix=null).
            DirectoryCreate => {
                let path = self.pop_string_tuple(1).await;
                let layer = self.pop_optional_bytes().await;
                let prefix = self.pop_optional_bytes().await;

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                debug!(
                    "creating path {:?} with layer {:?} and prefix {:?} using directory at index {}",
                    path.first().unwrap(),
                    layer,
                    prefix,
                    self.directory_index,
                );

                match directory
                    .create(
                        txn,
                        path.first().unwrap(),
                        prefix.as_deref(),
                        layer.as_deref(),
                    )
                    .await
                {
                    Ok(directory_subspace) => {
                        debug!(
                            "pushing DirectoryOutput at index {}",
                            self.directory_stack.len()
                        );
                        self.directory_stack
                            .push(DirectoryStackItem::DirectoryOutput(directory_subspace));
                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                };
            }

            // Pop 1 tuple off the stack as [path]. Pop 1 additional item as [layer]. Open
            // a directory with the specified path and layer. If layer is null, use the
            // default value (layer='').
            DirectoryOpen => {
                let path = self.pop_string_tuple(1).await;
                let bytes_layer = self.pop_bytes().await;

                let layer = if bytes_layer.is_empty() {
                    None
                } else {
                    Some(bytes_layer.to_vec())
                };

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                debug!(
                    "opening path {:?} with layer {:?} with index {}",
                    path.first().unwrap(),
                    layer,
                    self.directory_index
                );

                match directory
                    .open(txn, path.first().unwrap(), layer.as_deref())
                    .await
                {
                    Ok(directory_subspace) => {
                        debug!(
                            "pushing newly opened DirectoryOutput at index {}",
                            self.directory_stack.len()
                        );
                        self.directory_stack
                            .push(DirectoryStackItem::DirectoryOutput(directory_subspace));

                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 tuple off the stack as [path]. Pop 1 additional item as [layer]. Open
            // a directory with the specified path and layer. If layer is null, use the
            // default value (layer='').
            DirectoryCreateOrOpen => {
                let path = self.pop_string_tuple(1).await;
                let bytes_layer = self.pop_bytes().await;

                let layer = if bytes_layer.is_empty() {
                    None
                } else {
                    Some(bytes_layer.to_vec())
                };

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                debug!(
                    "create_or_open path {:?} with layer {:?} with index {}",
                    path.first().unwrap(),
                    layer,
                    self.directory_index
                );
                match directory
                    .create_or_open(txn, path.first().unwrap(), None, layer.as_deref())
                    .await
                {
                    Ok(directory_subspace) => {
                        debug!(
                            "pushing created_or_opened {:?} at index {}",
                            directory_subspace,
                            self.directory_stack.len()
                        );
                        self.directory_stack
                            .push(DirectoryStackItem::DirectoryOutput(directory_subspace));
                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => self.push_directory_err(&instr.code, number, e),
                };
            }

            // Pop the top item off the stack as [index]. Set the current directory list
            // index to index. In the event that the directory at this new index is null
            // (as the result of a previous error), set the directory list index to the
            // error index.
            DirectoryChange => {
                self.directory_index = self.pop_usize().await;
                debug!("setting directory_index to {}", self.directory_index);
                match self.directory_stack.get(self.directory_index) {
                    None => {
                        self.directory_index = self.error_index;
                        debug!(
                            "setting directory_index to error index {}: no directory found",
                            self.directory_index
                        );
                    }
                    Some(d) => {
                        if let DirectoryStackItem::Null = d {
                            self.directory_index = self.error_index;
                            debug!(
                                "setting directory_index to error index {}: because it is Null",
                                self.directory_index
                            );
                        }
                    }
                }
            }

            // Pop the top item off the stack as [error_index]. Set the current error index
            // to error_index.
            DirectorySetErrorIndex => {
                self.error_index = self.pop_usize().await;
            }

            // Use the current directory for this operation.
            //
            // Pop 2 tuples off the stack as [old_path, new_path]. Call move with the
            // specified old_path and new_path. Append the result onto the directory list.
            DirectoryMove => {
                let paths = self.pop_string_tuple(2).await;

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                debug!(
                    "moving {:?} to {:?} using directory at index {}",
                    paths.first().unwrap(),
                    paths.get(1).unwrap(),
                    self.directory_index
                );

                match directory
                    .move_to(txn, paths.first().unwrap(), paths.get(1).unwrap())
                    .await
                {
                    Ok(s) => {
                        debug!(
                            "pushing moved directory {:?} at index {}",
                            s,
                            self.directory_stack.len()
                        );
                        self.directory_stack
                            .push(DirectoryStackItem::DirectoryOutput(s));
                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 tuple off the stack as [new_absolute_path]. Call moveTo with the
            // specified new_absolute_path. Append the result onto the directory list.
            DirectoryMoveTo => {
                let paths = self.pop_string_tuple(1).await;

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                debug!("moving directory {:?} to {:?}", self.directory_index, paths);

                match directory
                    .move_directory(txn, paths.first().expect("popped tuple has no item"))
                    .await
                {
                    Ok(s) => {
                        debug!(
                            "pushing moved directory {:?} at index {}",
                            s,
                            self.directory_stack.len()
                        );
                        self.directory_stack
                            .push(DirectoryStackItem::DirectoryOutput(s));
                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
            // tuple off the stack as [path]. Call remove_if_exits, passing it path if one
            // was popped.
            DirectoryRemove => {
                let count = self.pop_usize().await;
                let paths = self.pop_string_tuple(count).await;

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                let paths = paths.first().expect("could not retrieve a path");
                debug!(
                    "removing path {:?} using directory at index {}",
                    paths, self.directory_index
                );
                match directory.remove(txn, paths).await {
                    Ok(deleted) => {
                        if !deleted {
                            self.push_directory_err(
                                &instr.code,
                                number,
                                DirectoryError::Other(String::from("directory does not exists")),
                            );
                        } else if is_db {
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                }
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
            // tuple off the stack as [path]. Call remove_if_exits, passing it path if one
            // was popped.
            DirectoryRemoveIfExists => {
                let count = self.pop_usize().await;
                let paths = self.pop_string_tuple(count).await;

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                let paths = paths.first().expect("could not retrieve a path");
                match directory.remove_if_exists(txn, paths).await {
                    Ok(_) => {
                        if is_db {
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(err) => self.push_directory_err(&instr.code, number, err),
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
            // tuple off the stack as [path]. Call list, passing it path if one was popped.
            // Pack the resulting list of directories using the tuple layer and push the
            // packed string onto the stack.
            DirectoryList => {
                let count = self.pop_usize().await;
                let paths = match count {
                    1 => {
                        let paths = self.pop_string_tuple(count).await;
                        paths.first().expect("could not retrieve a path").clone()
                    }
                    0 => vec![],
                    _ => panic!(),
                };

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db
                    .create_trx()
                    .expect("could not create a local transaction");
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                match directory.list(txn, &paths).await {
                    Ok(children) => {
                        let mut elements: Vec<Element> = vec![];
                        debug!(
                            "found {} items under path {:?} with directory at index {}:",
                            children.len(),
                            paths,
                            self.directory_index
                        );
                        for child in children {
                            debug!("\t - {}", child);
                            let element = Element::String(Cow::from(child));
                            elements.push(element);
                        }
                        let tuple = Element::Tuple(elements);
                        self.push(number, Element::Bytes(Bytes::from(tuple.pack_to_vec())));
                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [count] (either 0 or 1). If count is 1, pop 1
            // tuple off the stack as [path]. Call exists, passing it path if one
            // was popped. Push 1 onto the stack if the path exists and 0 if it does not.
            DirectoryExists => {
                let count = self.pop_usize().await;

                let paths = self.pop_string_tuple(count).await;

                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let local_trx = db.create_trx().unwrap();
                let txn = match is_db {
                    true => &local_trx,
                    false => match trx {
                        TransactionState::Transaction(ref t) => t,
                        _ => {
                            panic!("could not find an active transaction");
                        }
                    },
                };

                let paths = paths.first().expect("could not retrieve a path");
                match directory.exists(txn, paths).await {
                    Ok(exists) => {
                        self.push(number, Element::Int(i64::from(exists)));
                        if is_db {
                            debug!("commiting local trx");
                            local_trx.commit().await.expect("could not commit");
                        }
                    }
                    Err(e) => {
                        self.push_directory_err(&instr.code, number, e);
                    }
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 tuple off the stack as [key_tuple]. Pack key_tuple and push the result
            // onto the stack.
            DirectoryPackKey => {
                let n: usize = self.pop_usize().await;
                debug!("DirectoryPackKey {n}");
                let mut buf = Vec::with_capacity(n);
                for _ in 0..n {
                    let element: Element = self.pop_element().await;
                    debug!(" - {element:?}");
                    buf.push(element);
                }

                let tuple = Element::Tuple(buf);

                match self.pack_with_current_subspace(&tuple) {
                    None => self.push_directory_err(
                        &instr.code,
                        number,
                        DirectoryError::Other(String::from("cannot pack with current item")),
                    ),
                    Some(bytes) => {
                        self.push(number, Element::Bytes(bytes.into()));
                    }
                }
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [key]. Unpack key and push the resulting tuple
            // onto the stack one item at a time.
            DirectoryUnpackKey => {
                let data = self.pop_bytes().await;
                let data = data.to_vec();
                debug!("directory_unpack {data:?}");
                match self.unpack_with_current_subspace(&data) {
                    None => self.push_directory_err(
                        &instr.code,
                        number,
                        DirectoryError::Other(String::from(
                            "Cannot unpack with Directory Partition",
                        )),
                    ),
                    Some(packed) => {
                        let data: Vec<Element> = packed.unwrap();
                        for element in data {
                            debug!(" - {element:?}");
                            self.push(number, Element::Tuple(vec![element.into_owned()]));
                        }
                    }
                }
            }

            // Use the current directory for this operation.
            //
            // Pop 1 tuple off the stack as [tuple]. Create a range using tuple and push
            // range.begin and range.end onto the stack.
            DirectoryRange => {
                let n: usize = self.pop_usize().await;
                let mut buf = Vec::with_capacity(n);
                for _ in 0..n {
                    let element: Element = self.pop_element().await;
                    debug!(" - {element:?}");
                    buf.push(element);
                }

                match self.get_current_directory_item() {
                    Some(DirectoryStackItem::DirectoryOutput(
                        DirectoryOutput::DirectoryPartition(_d),
                    )) => {
                        self.push_directory_err(
                            &instr.code,
                            number,
                            DirectoryError::Other(String::from(
                                "operation not allowed on directoryPartition",
                            )),
                        );
                    }
                    _ => {
                        let tuple = Element::Tuple(buf);
                        match self.subspace_with_current_item(&tuple) {
                            None => {
                                self.push_directory_err(
                                    &instr.code,
                                    number,
                                    DirectoryError::Other(String::from(
                                        "operation not allowed on directoryPartition",
                                    )),
                                );
                            }
                            Some(subspace) => {
                                let (begin_range, end_range) = subspace.range();
                                self.push(number, Element::Bytes(begin_range.into()));
                                self.push(number, Element::Bytes(end_range.into()));
                            }
                        }
                    }
                }
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [key]. Check if the current directory contains
            // the specified key. Push 1 if it does and 0 if it doesn't.
            DirectoryContains => {
                let raw_prefix = self.pop_bytes().await;
                let maybe_contains = match self.get_current_directory_item() {
                    None => None,
                    Some(DirectoryStackItem::Subspace(s)) => Some(s.is_start_of(&raw_prefix)),
                    Some(DirectoryStackItem::DirectoryOutput(
                        DirectoryOutput::DirectorySubspace(s),
                    )) => Some(s.is_start_of(&raw_prefix)),
                    _ => None,
                };
                match maybe_contains {
                    None => self.push_directory_err(
                        &instr.code,
                        number,
                        DirectoryError::Other(String::from("cannot perform contains")),
                    ),
                    Some(bool) => self.push(number, Element::Int(bool as i64)),
                }
            }

            // Use the current directory for this operation.
            //
            // Pop 1 tuple off the stack as [tuple]. Open the subspace of the current
            // directory specified by tuple and push it onto the directory list.
            DirectoryOpenSubspace => {
                let n: usize = self.pop_usize().await;
                debug!("DirectoryRange {n}");
                let mut buf = Vec::with_capacity(n);
                for _ in 0..n {
                    let element: Element = self.pop_element().await;
                    debug!(" - {element:?}");
                    buf.push(element);
                }

                let tuple = Element::Tuple(buf);
                match self.subspace_with_current_item(&tuple) {
                    None => self.push_directory_err(
                        &instr.code,
                        number,
                        DirectoryError::Other(String::from("Cannot subspace with current item")),
                    ),
                    Some(s) => self.directory_stack.push(DirectoryStackItem::Subspace(s)),
                }
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [prefix]. Let key equal
            // prefix + tuple.pack([dir_index]). Set key to be the result of calling
            // directory.key() in the current transaction.
            DirectoryLogSubspace => {
                debug!(
                    "logging subspace {}/{}",
                    self.directory_index,
                    self.directory_stack.len()
                );
                let raw_prefix = self.pop_bytes().await;
                let txn = match trx {
                    TransactionState::Transaction(ref t) => t,
                    _ => {
                        panic!("could not find an active transaction");
                    }
                };
                let key = Subspace::from_bytes(raw_prefix.into_owned()).pack(&self.directory_index);

                match self.directory_stack.get(self.directory_index) {
                    None => panic!("nothing in the stack"),
                    Some(DirectoryStackItem::Null) => panic!("Directory is NULL"),
                    Some(DirectoryStackItem::Directory(_)) => {
                        panic!("trying to get a subspace, got a Directory")
                    }
                    Some(DirectoryStackItem::DirectoryOutput(
                        DirectoryOutput::DirectorySubspace(d),
                    )) => {
                        txn.set(&key, d.bytes());
                        debug!(
                            "logging subspace [{}] {:?}={:?}",
                            self.directory_index,
                            unpack::<Vec<Element>>(&key).unwrap(),
                            d.bytes(),
                        );
                    }
                    Some(DirectoryStackItem::DirectoryOutput(
                        DirectoryOutput::DirectoryPartition(_),
                    )) => {
                        self.push_directory_err(
                            &instr.code,
                            number,
                            DirectoryError::Other(String::from(
                                "cannot get key for the root of a directory partition",
                            )),
                        );
                    }
                    Some(DirectoryStackItem::Subspace(s)) => {
                        txn.set(&key, s.bytes());
                        debug!(
                            "logging subspace [{}] {:?}={:?}",
                            self.directory_index,
                            unpack::<Vec<Element>>(&key).unwrap(),
                            s.bytes(),
                        );
                    }
                };
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [raw_prefix]. Create a subspace log_subspace
            // with path (dir_index) and the specified raw_prefix. Set:
            //
            // tr[log_subspace[u'path']] = the tuple packed path of the directory.
            //
            // tr[log_subspace[u'layer']] = the tuple packed layer of the directory.
            //
            // tr[log_subspace[u'exists']] = the packed tuple containing a 1 if the
            // directory exists and 0 if it doesn't.
            //
            // tr[log_subspace[u'children']] the tuple packed list of children of the
            // directory.
            //
            // Where log_subspace[u<str>] is the subspace packed tuple containing only the
            // single specified unicode string <str>.
            DirectoryLogDirectory => {
                debug!("logging directory {}", self.directory_index);
                let directory = self
                    .get_current_directory()
                    .expect("could not find a directory");

                let txn = match trx {
                    TransactionState::Transaction(ref t) => t,
                    _ => {
                        panic!("could not find an active transaction");
                    }
                };

                let raw_prefix = self.pop_bytes().await;
                let subspace =
                    Subspace::from_bytes(raw_prefix.into_owned()).subspace(&self.directory_index);

                let key_path = subspace.pack(&"path");
                let value_path = pack(&self.get_path_for_current_directory().unwrap());

                let key_layer = subspace.pack(&"layer");
                let value_layer = pack(&self.get_layer_for_current_directory().unwrap());

                let exists = directory
                    .exists(txn, &[])
                    .await
                    .expect("could not list directory");

                let key_exists = subspace.pack(&"exists");
                let value_exists = pack(&Element::Tuple(vec![Element::Int(exists.into())]));

                let children = if exists {
                    directory.list(txn, &[]).await.unwrap()
                } else {
                    vec![]
                };

                let tuple_children = Element::Tuple(
                    children
                        .iter()
                        .map(|s| Element::String(Cow::Owned(s.clone())))
                        .collect(),
                );

                let key_children = subspace.pack(&"children");
                let value_children = pack(&tuple_children);

                txn.set(&key_path, &value_path);
                debug!(
                    "[{}] {:?}={:?}",
                    self.directory_index,
                    unpack::<Vec<Element>>(&key_path).unwrap(),
                    unpack::<Vec<Element>>(&value_path).unwrap(),
                );
                txn.set(&key_layer, &value_layer);
                debug!(
                    "[{}] {:?}={:?}",
                    self.directory_index,
                    unpack::<Vec<Element>>(&key_layer).unwrap(),
                    unpack::<Vec<Element>>(&value_layer).unwrap(),
                );
                txn.set(&key_exists, &value_exists);
                debug!(
                    "[{}] {:?}={:?}",
                    self.directory_index,
                    unpack::<Vec<Element>>(&key_exists).unwrap(),
                    unpack::<Vec<Element>>(&value_exists).unwrap(),
                );
                txn.set(&key_children, &value_children);
                debug!(
                    "[{}] {:?}={:?}",
                    self.directory_index,
                    unpack::<Vec<Element>>(&key_children).unwrap(),
                    unpack::<Vec<Element>>(&value_children).unwrap(),
                );
            }

            // Use the current directory for this operation.
            //
            // Pop 1 item off the stack as [byte_array]. Call .key() on the current
            // subspace and store the result as [prefix]. Throw an error if the popped
            // array does not start with prefix. Otherwise, remove the prefix from the
            // popped array and push the result onto the stack.
            DirectoryStripPrefix => match self.pop_optional_bytes().await {
                None => {
                    self.push_directory_err(
                        &instr.code,
                        number,
                        DirectoryError::Other(String::from("bad input on bytes")),
                    );
                }
                Some(raw_prefix) => match self.get_bytes_for_current_directory() {
                    None => self.push_directory_err(
                        &instr.code,
                        number,
                        DirectoryError::Version(String::from(
                            "cannot get bytes for DirectoryPartition",
                        )),
                    ),
                    Some(ssb) => {
                        if !raw_prefix.to_vec().starts_with(&ssb) {
                            self.push_directory_err(
                                &instr.code,
                                number,
                                DirectoryError::Version(String::from(
                                    "String does not start with raw prefix",
                                )),
                            );
                        } else {
                            self.push(
                                number,
                                Element::Bytes(Bytes::from(raw_prefix[ssb.len()..].to_owned())),
                            );
                        }
                    }
                },
            },
        }

        if is_db && pending {
            let item = self.pop().await;
            self.stack.push(item);
        }

        if is_db && mutation {
            let r = trx.take(0).commit().await.map_err(|e| e.into());
            self.check(number, r)?;
            self.push(number, RESULT_NOT_PRESENT.clone().into_owned());
        } else if !self.transactions.contains_key(&self.cur_transaction) {
            self.transactions.insert(self.cur_transaction.clone(), trx);
        }

        if instr.has_flags() {
            panic!("flag not handled for instr: {instr:?}");
        }

        Ok(())
    }

    async fn run(&mut self, db: Arc<Database>) -> FdbResult<()> {
        info!("Fetching instructions...");
        let instrs = self.fetch_instr(&db.create_trx()?).await?;
        info!("{} instructions found", instrs.len());

        for (i, instr) in instrs.into_iter().enumerate() {
            trace!("{}/{}, {:?}", i, self.stack.len(), instr);
            let _ = self.run_step(db.clone(), i, instr).await;
        }

        Ok(())
    }

    fn join(&mut self) {
        for handle in self.threads.drain(0..) {
            handle.join().expect("joined thread to not panic");
        }
    }

    fn get_current_directory_item(&mut self) -> Option<&DirectoryStackItem> {
        self.directory_stack.get(self.directory_index)
    }

    fn get_current_directory(&mut self) -> Option<Box<dyn Directory>> {
        match self.get_current_directory_item() {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::Directory(d) => Some(Box::new(d.clone())),
                DirectoryStackItem::DirectoryOutput(d) => Some(Box::new((*d).clone())),
                _ => None,
            },
        }
    }

    fn pack_with_current_subspace<T: TuplePack>(&self, v: &T) -> Option<Vec<u8>> {
        match self.directory_stack.get(self.directory_index) {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::DirectoryOutput(DirectoryOutput::DirectorySubspace(d)) => {
                    Some(d.pack(v))
                }
                DirectoryStackItem::Subspace(d) => Some(d.pack(v)),
                _ => None,
            },
        }
    }

    // It can return a None if it is a DirectoryPartition
    fn unpack_with_current_subspace<'de, T: TupleUnpack<'de>>(
        &self,
        key: &'de [u8],
    ) -> Option<PackResult<T>> {
        match self.directory_stack.get(self.directory_index) {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::DirectoryOutput(d) => match d {
                    DirectoryOutput::DirectorySubspace(directory_subspace) => {
                        Some(directory_subspace.unpack(key))
                    }
                    DirectoryOutput::DirectoryPartition(_) => None,
                },
                DirectoryStackItem::Subspace(d) => Some(d.unpack(key)),
                _ => None,
            },
        }
    }

    // It can return a None if it is a DirectoryPartition
    fn subspace_with_current_item<T: TuplePack>(&self, t: &T) -> Option<Subspace> {
        match self.directory_stack.get(self.directory_index) {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::DirectoryOutput(d) => match d {
                    DirectoryOutput::DirectorySubspace(s) => Some(s.subspace(t)),
                    DirectoryOutput::DirectoryPartition(_) => None,
                },
                DirectoryStackItem::Subspace(d) => Some(d.subspace(t)),
                _ => None,
            },
        }
    }

    fn get_path_for_current_directory(&self) -> Option<&[String]> {
        match self.directory_stack.get(self.directory_index) {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::Directory(d) => Some(d.get_path()),
                DirectoryStackItem::DirectoryOutput(d) => Some(d.get_path()),
                _ => None,
            },
        }
    }

    fn get_layer_for_current_directory(&self) -> Option<&[u8]> {
        match self.directory_stack.get(self.directory_index) {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::DirectoryOutput(d) => Some(d.get_layer()),
                DirectoryStackItem::Directory(_) => Some(&[]),
                _ => None,
            },
        }
    }

    fn get_bytes_for_current_directory(&self) -> Option<Vec<u8>> {
        match self.directory_stack.get(self.directory_index) {
            None => None,
            Some(directory_or_subspace) => match directory_or_subspace {
                DirectoryStackItem::DirectoryOutput(d) => match d {
                    DirectoryOutput::DirectorySubspace(s) => Some(Vec::from(s.bytes())),
                    DirectoryOutput::DirectoryPartition(_) => None,
                },
                DirectoryStackItem::Subspace(subspace) => Some(Vec::from(subspace.bytes())),
                _ => None,
            },
        }
    }
}

fn main() {
    let now = std::time::Instant::now();
    env_logger::Builder::from_default_env()
        .format(move |buf, record| {
            let current_thread = thread::current();
            let thread_name = current_thread.name().unwrap_or("?");
            writeln!(
                buf,
                "{} {} {} - {}",
                (std::time::Instant::now() - now).as_millis(),
                thread_name,
                record.level(),
                record.args()
            )
        })
        .format_timestamp_millis()
        .target(env_logger::Target::Stdout)
        .init();

    let args = std::env::args().collect::<Vec<_>>();
    let prefix = &args[1];

    let cluster_path = if args.len() > 3 {
        Some(args[3].as_str())
    } else {
        None
    };

    let api_version = args[2].parse::<i32>().expect("failed to parse api version");

    info!("Starting rust bindingtester with api_version {api_version}");
    api::FdbApiBuilder::default()
        .set_runtime_version(api_version)
        .build()
        .expect("failed to initialize FoundationDB API")
        .boot()
        .expect("failed to start the FoundationDB network");

    let db = Arc::new(
        futures::executor::block_on(fdb::Database::new_compat(cluster_path))
            .expect("failed to get database"),
    );

    let mut sm = StackMachine::new(&db, Bytes::from(prefix.to_owned().into_bytes()));
    futures::executor::block_on(sm.run(db)).unwrap();
    sm.join();

    info!("Closing...");

    info!("Done.");
}
