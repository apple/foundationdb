//! Implementation of the official tuple layer typecodes
//!
//! The official specification can be found [here](https://github.com/apple/foundationdb/blob/master/design/tuple.md).

mod element;
mod pack;
mod subspace;
mod versionstamp;

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::io;
use std::ops::Deref;
use std::result;

#[cfg(feature = "uuid")]
pub use uuid::Uuid;

pub use element::Element;
pub use pack::{TuplePack, TupleUnpack, VersionstampOffset};
pub use subspace::Subspace;
pub use versionstamp::Versionstamp;

const NIL: u8 = 0x00;
const BYTES: u8 = 0x01;
const STRING: u8 = 0x02;
const NESTED: u8 = 0x05;
const NEGINTSTART: u8 = 0x0b;
const INTZERO: u8 = 0x14;
const POSINTEND: u8 = 0x1d;
const FLOAT: u8 = 0x20;
const DOUBLE: u8 = 0x21;
const FALSE: u8 = 0x26;
const TRUE: u8 = 0x27;
#[cfg(feature = "uuid")]
const UUID: u8 = 0x30;
// Not a single official binding is implementing 80 Bit versionstamp...
// const VERSIONSTAMP_88: u8 = 0x32;
const VERSIONSTAMP: u8 = 0x33;

const ESCAPE: u8 = 0xff;

/// Tracks the depth of a Tuple decoding chain
#[derive(Copy, Clone)]
pub struct TupleDepth(usize);

impl TupleDepth {
    fn new() -> Self {
        TupleDepth(0)
    }

    /// Increment the depth by one, this be called when calling into `Tuple::{encode, decode}` of tuple-like datastructures
    pub fn increment(self) -> Self {
        TupleDepth(self.0 + 1)
    }

    /// Returns the current depth in any recursive tuple processing, 0 representing there having been no recursion
    pub fn depth(self) -> usize {
        self.0
    }
}

/// A packing/unpacking error
#[derive(Debug)]
pub enum PackError {
    Message(Box<str>),
    IoError(io::Error),
    TrailingBytes,
    MissingBytes,
    BadStringFormat,
    BadCode {
        found: u8,
        expected: Option<u8>,
    },
    BadPrefix,
    #[cfg(feature = "uuid")]
    BadUuid,
    UnsupportedIntLength,
}

impl From<io::Error> for PackError {
    fn from(err: io::Error) -> Self {
        PackError::IoError(err)
    }
}

impl Display for PackError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PackError::Message(s) => s.fmt(f),
            PackError::IoError(err) => err.fmt(f),
            PackError::TrailingBytes => write!(f, "trailing bytes"),
            PackError::MissingBytes => write!(f, "missing bytes"),
            PackError::BadStringFormat => write!(f, "not an utf8 string"),
            PackError::BadCode { found, .. } => write!(f, "bad code, found {found}"),
            PackError::BadPrefix => write!(f, "bad prefix"),
            #[cfg(feature = "uuid")]
            PackError::BadUuid => write!(f, "bad uuid"),
            PackError::UnsupportedIntLength => write!(f, "integer length was to large"),
        }
    }
}

impl std::error::Error for PackError {}

/// Alias for `Result<..., tuple::Error>`
pub type PackResult<T> = result::Result<T, PackError>;

/// Represent a sequence of bytes (i.e. &[u8])
///
/// This sequence can be either owned or borrowed.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes<'a>(pub Cow<'a, [u8]>);

impl fmt::Debug for Bytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Bytes<'_> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "b\"")?;
        for &byte in self.0.iter() {
            if byte == b'\\' {
                write!(fmt, r"\\")?;
            } else if byte.is_ascii_alphanumeric() {
                write!(fmt, "{}", byte as char)?;
            } else {
                write!(fmt, "\\x{byte:02x}")?;
            }
        }
        write!(fmt, "\"")
    }
}

impl Bytes<'_> {
    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }
}

impl Deref for Bytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl AsRef<[u8]> for Bytes<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'a> From<&'a [u8]> for Bytes<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self(Cow::Borrowed(bytes))
    }
}
impl From<Vec<u8>> for Bytes<'static> {
    fn from(vec: Vec<u8>) -> Self {
        Self(Cow::Owned(vec))
    }
}

impl<'a> From<&'a str> for Bytes<'a> {
    fn from(s: &'a str) -> Self {
        s.as_bytes().into()
    }
}
impl From<String> for Bytes<'static> {
    fn from(vec: String) -> Self {
        vec.into_bytes().into()
    }
}

/// Pack value and returns the packed buffer
///
/// # Panics
///
/// Panics if the encoded data size doesn't fit in `u32`.
pub fn pack<T: TuplePack>(v: &T) -> Vec<u8> {
    v.pack_to_vec()
}

/// Pack value and returns the packed buffer
///
/// # Panics
///
/// Panics if there is multiple versionstamp present or if the encoded data size doesn't fit in `u32`.
pub fn pack_with_versionstamp<T: TuplePack>(v: &T) -> Vec<u8> {
    v.pack_to_vec_with_versionstamp()
}

/// Pack value into the given buffer
///
/// # Panics
///
/// Panics if the encoded data size doesn't fit in `u32`.
pub fn pack_into<T: TuplePack>(v: &T, output: &mut Vec<u8>) -> VersionstampOffset {
    v.pack_into_vec(output)
}

/// Pack value into the given buffer
///
/// # Panics
///
/// Panics if there is multiple versionstamp present or if the encoded data size doesn't fit in `u32`.
pub fn pack_into_with_versionstamp<T: TuplePack>(v: &T, output: &mut Vec<u8>) {
    let offset = v.pack_into_vec_with_versionstamp(output);
    if let VersionstampOffset::MultipleIncomplete = offset {
        panic!("pack_into_with_versionstamp does not allow multiple versionstamps");
    }
}

/// Unpack input
pub fn unpack<'de, T: TupleUnpack<'de>>(input: &'de [u8]) -> PackResult<T> {
    T::unpack_root(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NIL_VAL: Option<()> = None;

    fn test_serde<'de, T>(val: T, buf: &'de [u8])
    where
        T: TuplePack + TupleUnpack<'de> + fmt::Debug + PartialEq,
    {
        assert_eq!(Bytes::from(pack(&val)), Bytes::from(buf));
        assert_eq!(unpack::<'de, T>(buf).unwrap(), val);
    }

    #[test]
    fn test_spec() {
        test_serde(NIL_VAL, &[NIL]);
        test_serde((NIL_VAL,), &[NIL]);
        test_serde(((NIL_VAL,),), &[NESTED, NIL, ESCAPE, NIL]);
        // assert_eq!(to_bytes(b"foo\x00bar").unwrap(), b"\x01foo\x00\xffbar\x00");
        test_serde("FÔO\x00bar".to_owned(), b"\x02F\xc3\x94O\x00\xffbar\x00");
        test_serde(
            (("foo\x00bar".to_owned(), NIL_VAL, ()),),
            b"\x05\x02foo\x00\xffbar\x00\x00\xff\x05\x00\x00",
        );
        test_serde(-1, b"\x13\xfe");
        test_serde(-5551212, b"\x11\xabK\x93");
        test_serde(-42f32, b"\x20\x3d\xd7\xff\xff");
    }

    #[test]
    fn test_simple() {
        // bool
        test_serde(false, &[FALSE]);
        test_serde(true, &[TRUE]);

        // int
        test_serde(0i64, &[INTZERO]);
        test_serde(1i64, &[0x15, 1]);
        test_serde(-1i64, &[0x13, 254]);
        test_serde(100i64, &[21, 100]);

        test_serde(10000i32, &[22, 39, 16]);
        test_serde(-100i16, &[19, 155]);
        test_serde(-10000i64, &[18, 216, 239]);
        test_serde(-1000000i64, &[17, 240, 189, 191]);

        // boundary condition
        test_serde(255u16, &[0x15, 255]);
        test_serde(256i32, &[0x16, 1, 0]);
        test_serde(-255i16, &[0x13, 0]);
        test_serde(-256i64, &[0x12, 254, 255]);

        // versionstamp
        test_serde(
            Versionstamp::complete(*b"\xaa\xbb\xcc\xdd\xee\xff\x00\x01\x02\x03", 0),
            b"\x33\xaa\xbb\xcc\xdd\xee\xff\x00\x01\x02\x03\x00\x00",
        );
        test_serde(
            Versionstamp::complete(*b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a", 657),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );

        test_serde(0, b"\x14");
        test_serde(1, b"\x15\x01");
        test_serde(-1, b"\x13\xfe");
        test_serde(255, b"\x15\xff");
        test_serde(-255, b"\x13\x00");
        test_serde(256, b"\x16\x01\x00");
        test_serde(-256, b"\x12\xfe\xff");
        test_serde(65536, b"\x17\x01\x00\x00");
        test_serde(-65536, b"\x11\xfe\xff\xff");
        test_serde(i64::MAX, b"\x1C\x7f\xff\xff\xff\xff\xff\xff\xff");
        test_serde(i64::MAX as u64 + 1, b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00");
        test_serde(u64::MAX, b"\x1C\xff\xff\xff\xff\xff\xff\xff\xff");
        test_serde(
            u128::MAX,
            b"\x1D\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            u64::MAX as u128 + 1,
            b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            u64::MAX as i128 + 1,
            b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            i64::MIN as i128 + 1,
            b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            i64::MIN as i128 - 1,
            b"\x0C\x7f\xff\xff\xff\xff\xff\xff\xfe",
        );
        test_serde(-(u64::MAX as i128), b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00");
        test_serde(
            -(u64::MAX as i128) - 1,
            b"\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            -(u64::MAX as i128) - 2,
            b"\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xfe",
        );
        test_serde(
            (u64::MAX as i128) * -2,
            b"\x0b\xf6\xfe\x00\x00\x00\x00\x00\x00\x00\x01",
        );
        test_serde(
            (u64::MAX as i128) * 2,
            b"\x1d\x09\x01\xff\xff\xff\xff\xff\xff\xff\xfe",
        );
        test_serde(-4294967295i64, b"\x10\x00\x00\x00\x00");
        test_serde(i64::MIN + 2, b"\x0C\x80\x00\x00\x00\x00\x00\x00\x01");
        test_serde(i64::MIN + 1, b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00");
        test_serde(i64::MIN, b"\x0C\x7f\xff\xff\xff\xff\xff\xff\xff");

        test_serde(9252427359321063944i128, b"\x1c\x80g9\xa9np\x02\x08");
        assert!(matches!(
            unpack::<i64>(b"\x1c\x80g9\xa9np\x02\x08").unwrap_err(),
            PackError::UnsupportedIntLength
        ));

        test_serde(
            -9252427359321063944i128,
            b"\x0c\x7f\x98\xc6V\x91\x8f\xfd\xf7",
        );
        assert!(matches!(
            unpack::<i64>(b"\x0c\x7f\x98\xc6V\x91\x8f\xfd\xf7").unwrap_err(),
            PackError::UnsupportedIntLength
        ));

        test_serde(u64::MAX as i128, b"\x1c\xff\xff\xff\xff\xff\xff\xff\xff");
        assert!(matches!(
            unpack::<i64>(b"\x1c\xff\xff\xff\xff\xff\xff\xff\xff").unwrap_err(),
            PackError::UnsupportedIntLength
        ));

        test_serde(-(u64::MAX as i128), b"\x0c\x00\x00\x00\x00\x00\x00\x00\x00");
        assert!(matches!(
            unpack::<i64>(b"\x0c\x00\x00\x00\x00\x00\x00\x00\x00").unwrap_err(),
            PackError::UnsupportedIntLength
        ));

        test_serde(
            (i64::MAX as i128) + 1,
            b"\x1c\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        assert!(matches!(
            unpack::<i64>(b"\x1c\x80\x00\x00\x00\x00\x00\x00\x00").unwrap_err(),
            PackError::UnsupportedIntLength
        ));

        test_serde(
            (i64::MIN as i128) - 1,
            b"\x0c\x7f\xff\xff\xff\xff\xff\xff\xfe",
        );
        assert!(matches!(
            unpack::<i64>(b"\x0c\x7f\xff\xff\xff\xff\xff\xff\xfe").unwrap_err(),
            PackError::UnsupportedIntLength
        ));
    }

    #[test]
    fn test_typecode_band_ordering() {
        // Order-preserving keys rely on packed tuples comparing byte-lexicographically
        // (Rust's `Vec<u8>` ordering). This mirrors a subspace-prefix scheme where the
        // prefix is a u32 discriminant, with a negative "header" band strictly before it
        // and an f32 band strictly after it. The typecode band, not the value, decides:
        //   negatives (0x0c..=0x13)  <  u32 (0x14..=0x18)  <  f32 (0x20 ...)
        let hex = |b: &[u8]| {
            b.iter()
                .map(|x| format!("{x:02x}"))
                .collect::<Vec<_>>()
                .join(" ")
        };

        // "before" band: signed ints, min and max of the range we might use.
        let neg_min = pack(&i64::MIN); // 0c 7f ff ff ff ff ff ff ff (most negative)
        let neg_max = pack(&-1i64); //    13 fe                      (closest to u32 band)

        // middle band: the u32 subspace prefix, min and max discriminant.
        let u32_min = pack(&0u32); //     14                         (INTZERO)
        let u32_max = pack(&u32::MAX); //  18 ff ff ff ff            (4-byte positive int)

        // "after" band: f32 markers >= 1.0, min and max of the range we might use.
        let f_min = pack(&1.0f32); //     20 bf 80 00 00             (closest to u32 band)
        let f_max = pack(&f32::MAX); //    20 ff 7f ff ff

        println!("i64::MIN = {}", hex(&neg_min));
        println!("-1i64    = {}", hex(&neg_max));
        println!("0u32     = {}", hex(&u32_min));
        println!("u32::MAX = {}", hex(&u32_max));
        println!("1.0f32   = {}", hex(&f_min));
        println!("f32::MAX = {}", hex(&f_max));

        // Within each band numeric order is preserved (nothing weird inside a band).
        assert!(neg_min < neg_max); // 0c ..    < 13 fe
        assert!(u32_min < u32_max); // 14       < 18 ..
        assert!(f_min < f_max); //     20 bf .. < 20 ff ..

        // Whole "before" band sorts under the u32 band; tightest pair is -1 vs 0.
        assert!(neg_max < u32_min); // 13 fe < 14
        assert!(neg_min < u32_min); // 0c .. < 14 (even the most negative stays below)

        // Whole "after" band sorts over the u32 band; tightest pair is u32::MAX vs 1.0.
        assert!(u32_max < f_min); //   18 ff ff ff ff < 20 bf 80 00 00
        assert!(u32_max < f_max); //   18 ..          < 20 ff .. (magnitude irrelevant)
    }

    #[cfg(feature = "num-bigint")]
    #[test]
    fn test_bigint() {
        use num_bigint::{BigInt, BigUint};
        // int
        test_serde(BigInt::from(0i64), &[INTZERO]);
        test_serde(BigUint::from(0u64), &[INTZERO]);
        test_serde(BigInt::from(1i64), &[0x15, 1]);
        test_serde(BigUint::from(1u64), &[0x15, 1]);
        test_serde(BigInt::from(-1i64), &[0x13, 254]);
        test_serde(BigInt::from(100i64), &[0x15, 100]);
        test_serde(BigUint::from(100u64), &[0x15, 100]);

        test_serde(BigInt::from(10000i32), &[0x16, 39, 16]);
        test_serde(BigUint::from(10000u32), &[0x16, 39, 16]);
        test_serde(BigInt::from(-100i16), &[19, 155]);
        test_serde(BigInt::from(-10000i64), &[18, 216, 239]);
        test_serde(BigInt::from(-1000000i64), &[17, 240, 189, 191]);

        // boundary condition
        test_serde(BigInt::from(255u16), &[0x15, 255]);
        test_serde(BigUint::from(255u16), &[0x15, 255]);
        test_serde(BigInt::from(256i32), &[0x16, 1, 0]);
        test_serde(BigInt::from(-255i16), &[0x13, 0]);
        test_serde(BigInt::from(-256i64), &[0x12, 254, 255]);

        test_serde(BigInt::from(0), b"\x14");
        test_serde(BigUint::from(0u64), b"\x14");
        test_serde(BigInt::from(1), b"\x15\x01");
        test_serde(BigUint::from(1u64), b"\x15\x01");
        test_serde(BigInt::from(-1), b"\x13\xfe");
        test_serde(BigInt::from(255), b"\x15\xff");
        test_serde(BigUint::from(255u64), b"\x15\xff");
        test_serde(BigInt::from(-255), b"\x13\x00");
        test_serde(BigInt::from(256), b"\x16\x01\x00");
        test_serde(BigUint::from(256u64), b"\x16\x01\x00");
        test_serde(BigInt::from(-256), b"\x12\xfe\xff");
        test_serde(BigInt::from(65536), b"\x17\x01\x00\x00");
        test_serde(BigUint::from(65536u64), b"\x17\x01\x00\x00");
        test_serde(BigInt::from(-65536), b"\x11\xfe\xff\xff");
        test_serde(
            BigInt::from(i64::MAX),
            b"\x1C\x7f\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigUint::from(i64::MAX as u64),
            b"\x1C\x7f\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigInt::from(i64::MAX as u64 + 1),
            b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigUint::from(i64::MAX as u64 + 1),
            b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigInt::from(u64::MAX),
            b"\x1C\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigUint::from(u64::MAX),
            b"\x1C\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigInt::from(u128::MAX),
            b"\x1D\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigUint::from(u128::MAX),
            b"\x1D\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigInt::from(u64::MAX as u128 + 1),
            b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigUint::from(u64::MAX as u128 + 1),
            b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigInt::from(u64::MAX as i128 + 1),
            b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigUint::from(u64::MAX as u128 + 1),
            b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigInt::from(i64::MIN as i128 + 1),
            b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigInt::from(i64::MIN as i128 - 1),
            b"\x0C\x7f\xff\xff\xff\xff\xff\xff\xfe",
        );
        test_serde(
            BigInt::from(-(u64::MAX as i128)),
            b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigInt::from(-(u64::MAX as i128) - 1),
            b"\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xff",
        );
        test_serde(
            BigInt::from(-(u64::MAX as i128) - 2),
            b"\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xfe",
        );
        test_serde(
            BigInt::from((u64::MAX as i128) * -2),
            b"\x0b\xf6\xfe\x00\x00\x00\x00\x00\x00\x00\x01",
        );
        test_serde(
            BigInt::from((u64::MAX as i128) * 2),
            b"\x1d\x09\x01\xff\xff\xff\xff\xff\xff\xff\xfe",
        );
        test_serde(BigInt::from(-4294967295i64), b"\x10\x00\x00\x00\x00");
        test_serde(
            BigInt::from(i64::MIN + 2),
            b"\x0C\x80\x00\x00\x00\x00\x00\x00\x01",
        );
        test_serde(
            BigInt::from(i64::MIN + 1),
            b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(
            BigInt::from(i64::MIN),
            b"\x0C\x7f\xff\xff\xff\xff\xff\xff\xff",
        );

        test_serde(
            Element::BigInt(9252427359321063944i128.into()),
            b"\x1c\x80g9\xa9np\x02\x08",
        );
        test_serde(
            Element::BigInt((-9252427359321063944i128).into()),
            b"\x0c\x7f\x98\xc6V\x91\x8f\xfd\xf7",
        );
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid() {
        use uuid::Uuid;

        test_serde(
            Element::Uuid(
                Uuid::from_slice(
                    b"\xba\xff\xff\xff\xff\x5e\xba\x11\x00\x00\x00\x00\x5c\xa1\xab\x1e",
                )
                .unwrap(),
            ),
            b"\x30\xba\xff\xff\xff\xff\x5e\xba\x11\x00\x00\x00\x00\x5c\xa1\xab\x1e",
        );
    }

    #[test]
    fn test_bindingtester() {
        test_serde("NEW_TRANSACTION".to_string(), b"\x02NEW_TRANSACTION\x00");
        test_serde(
            vec!["NEW_TRANSACTION".to_string()],
            b"\x02NEW_TRANSACTION\x00",
        );
        test_serde(
            vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Bytes(Bytes::from(
                    b"\x01tester_output\x00\x01results\x00\x14".as_ref(),
                )),
            ],
            b"\x02PUSH\x00\x01\x01tester_output\x00\xff\x01results\x00\xff\x14\x00",
        );
        test_serde(
            vec![Element::String(Cow::Borrowed("PUSH")), Element::Nil],
            b"\x02PUSH\x00\x00",
        );
        test_serde(
            vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Tuple(vec![
                    Element::Nil,
                    Element::Float(3299069000000.0),
                    Element::Float(-0.000000000000000000000000000000000000011883096),
                ]),
            ],
            b"\x02PUSH\x00\x05\x00\xff \xd4@\x07\xf5 \x7f~\x9a\xc2\x00",
        );
        test_serde(
            vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Int(-133525682914243904),
            ],
            b"\x02PUSH\x00\x0c\xfe%\x9f\x19M\x81J\xbf",
        );

        test_serde(
            Element::Tuple(vec![Element::Nil, Element::Nil]),
            b"\x00\x00",
        );
        test_serde(
            Element::Tuple(vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Tuple(vec![Element::Double(-8.251343508909708e-15)]),
            ]),
            b"\x02PUSH\x00\x05!B\xfdkl\x9f\xcc\x8eK\x00",
        );
    }

    #[test]
    fn test_element() {
        test_serde(Element::Bool(true), &[TRUE]);
        test_serde(Element::Bool(false), &[FALSE]);
        test_serde(Element::Int(-1), &[0x13, 254]);
        test_serde(
            Element::Versionstamp(Versionstamp::complete(
                *b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a",
                657,
            )),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
        test_serde(
            (Element::Versionstamp(Versionstamp::complete(
                *b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a",
                657,
            )),),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
        test_serde(
            (Element::Versionstamp(Versionstamp::complete(
                *b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a",
                657,
            )),),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
        test_serde(
            vec![Element::Bool(true), Element::Bool(false)],
            &[TRUE, FALSE],
        );
        test_serde(
            vec![Element::Tuple(vec![
                Element::Bool(true),
                Element::Bool(false),
            ])],
            &[NESTED, TRUE, FALSE, NIL],
        );
        test_serde(Vec::<Element>::new(), &[]);
        test_serde(Element::Tuple(vec![]), &[]);
    }

    #[test]
    fn test_verstionstamp() {
        assert_eq!(
            Bytes::from(pack(&("foo", Versionstamp::incomplete(0)))),
            Bytes::from(&b"\x02foo\x00\x33\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00"[..])
        );
        assert_eq!(
            Bytes::from(pack_with_versionstamp(&(
                "foo",
                Versionstamp::incomplete(0)
            ))),
            Bytes::from(
                &b"\x02foo\x00\x33\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x06\x00\x00\x00"
                    [..]
            )
        );
    }
}
