use super::pack::{f32_to_u32_be_bytes, f64_to_u64_be_bytes};
use super::{Bytes, Versionstamp};
use std::{borrow::Cow, cmp};

#[cfg(feature = "num-bigint")]
use num_bigint::Sign;
#[cfg(feature = "num-bigint")]
use std::convert::TryFrom;

#[derive(Clone, Debug)]
pub enum Element<'a> {
    Nil,
    Bytes(Bytes<'a>),
    String(Cow<'a, str>),
    Tuple(Vec<Element<'a>>),
    Int(i64),
    #[cfg(feature = "num-bigint")]
    BigInt(num_bigint::BigInt),
    Float(f32),
    Double(f64),
    Bool(bool),
    #[cfg(feature = "uuid")]
    Uuid(uuid::Uuid),
    Versionstamp(Versionstamp),
}

struct CmpElement<'a, 'b>(&'a Element<'b>);

impl PartialEq for CmpElement<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}
impl Eq for CmpElement<'_, '_> {}

impl PartialOrd for CmpElement<'_, '_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CmpElement<'_, '_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.0
            .code()
            .cmp(&other.0.code())
            .then_with(|| match (&self.0, &other.0) {
                (Element::Bytes(a), Element::Bytes(b)) => a.cmp(b),
                (Element::String(a), Element::String(b)) => a.cmp(b),
                (Element::Tuple(a), Element::Tuple(b)) => {
                    let a_values = a.iter().map(CmpElement);
                    let b_values = b.iter().map(CmpElement);
                    a_values.cmp(b_values)
                }
                (Element::Int(a), Element::Int(b)) => a.cmp(b),
                #[cfg(feature = "num-bigint")]
                (Element::BigInt(a), Element::BigInt(b)) => a.cmp(b),
                #[cfg(feature = "num-bigint")]
                (Element::BigInt(a), Element::Int(b)) => match i64::try_from(a) {
                    Ok(a) => a.cmp(b),
                    Err(_) => a.sign().cmp(&Sign::NoSign),
                },
                #[cfg(feature = "num-bigint")]
                (Element::Int(a), Element::BigInt(b)) => match i64::try_from(b) {
                    Ok(b) => a.cmp(&b),
                    Err(_) => Sign::NoSign.cmp(&b.sign()),
                },
                (Element::Float(a), Element::Float(b)) => {
                    f32_to_u32_be_bytes(*a).cmp(&f32_to_u32_be_bytes(*b))
                }
                (Element::Double(a), Element::Double(b)) => {
                    f64_to_u64_be_bytes(*a).cmp(&f64_to_u64_be_bytes(*b))
                }
                #[cfg(feature = "uuid")]
                (Element::Uuid(a), Element::Uuid(b)) => a.cmp(b),
                (Element::Versionstamp(a), Element::Versionstamp(b)) => a.cmp(b),
                _ => cmp::Ordering::Equal,
            })
    }
}

impl PartialEq for Element<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}
impl Eq for Element<'_> {}

impl PartialOrd for Element<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Element<'_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.cmp_at_root(other)
    }
}

impl<'a> Element<'a> {
    fn code(&self) -> u8 {
        match self {
            Element::Nil => super::NIL,
            Element::Bytes(_) => super::BYTES,
            Element::String(_) => super::STRING,
            Element::Tuple(_) => super::NESTED,
            Element::Int(_) => super::INTZERO,
            #[cfg(feature = "num-bigint")]
            Element::BigInt(_) => super::INTZERO,
            Element::Float(_) => super::FLOAT,
            Element::Double(_) => super::DOUBLE,
            Element::Bool(v) => {
                if *v {
                    super::TRUE
                } else {
                    super::FALSE
                }
            }
            #[cfg(feature = "uuid")]
            Element::Uuid(_) => super::UUID,
            Element::Versionstamp(_) => super::VERSIONSTAMP,
        }
    }

    #[inline]
    fn cmp_values(&self) -> &[Self] {
        match self {
            Element::Tuple(v) => v.as_slice(),
            v => std::slice::from_ref(v),
        }
    }

    fn cmp_at_root(&self, b: &Element<'_>) -> cmp::Ordering {
        let a_values = self.cmp_values().iter().map(CmpElement);
        let b_values = b.cmp_values().iter().map(CmpElement);
        a_values.cmp(b_values)
    }

    pub fn into_owned(self) -> Element<'static> {
        match self {
            Element::Nil => Element::Nil,
            Element::Bytes(v) => Element::Bytes(v.into_owned().into()),
            Element::String(v) => Element::String(Cow::Owned(v.into_owned())),
            Element::Tuple(v) => Element::Tuple(v.into_iter().map(|e| e.into_owned()).collect()),
            Element::Int(v) => Element::Int(v),
            #[cfg(feature = "num-bigint")]
            Element::BigInt(v) => Element::BigInt(v),
            Element::Float(v) => Element::Float(v),
            Element::Double(v) => Element::Double(v),
            Element::Bool(v) => Element::Bool(v),
            #[cfg(feature = "uuid")]
            Element::Uuid(v) => Element::Uuid(v),
            Element::Versionstamp(v) => Element::Versionstamp(v),
        }
    }

    pub fn as_bytes(&self) -> Option<&Bytes<'_>> {
        match self {
            Element::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Element::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_tuple(&self) -> Option<&[Element<'a>]> {
        match self {
            Element::Tuple(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Element::Int(v) => Some(*v),
            #[cfg(feature = "num-bigint")]
            Element::BigInt(v) => i64::try_from(v).ok(),
            _ => None,
        }
    }

    #[cfg(feature = "num-bigint")]
    pub fn as_bigint(&self) -> Option<&num_bigint::BigInt> {
        match self {
            Element::BigInt(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Element::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Element::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Element::Bool(v) => Some(v),
            _ => None,
        }
    }

    #[cfg(feature = "uuid")]
    pub fn as_uuid(&self) -> Option<&uuid::Uuid> {
        match self {
            Element::Uuid(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_versionstamp(&self) -> Option<&Versionstamp> {
        match self {
            Element::Versionstamp(v) => Some(v),
            _ => None,
        }
    }
}
