use super::{Bytes, Element};
use std::fmt;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Versionstamp {
    bytes: [u8; 12],
}

impl fmt::Debug for Versionstamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Bytes::from(&self.bytes[..]).fmt(f)
    }
}

impl Versionstamp {
    pub fn incomplete(user_version: u16) -> Self {
        let mut bytes = [0xff; 12];
        bytes[10..].copy_from_slice(&user_version.to_be_bytes());
        Versionstamp { bytes }
    }

    pub fn complete(tr_version: [u8; 10], user_version: u16) -> Self {
        let mut bytes = [0xff; 12];
        bytes[0..10].copy_from_slice(&tr_version);
        bytes[10..].copy_from_slice(&user_version.to_be_bytes());
        Versionstamp { bytes }
    }

    pub fn transaction_version(&self) -> &[u8] {
        &self.bytes[0..10]
    }

    pub fn user_version(&self) -> u16 {
        let mut user_version = [0; 2];
        user_version.copy_from_slice(&self.bytes[10..12]);
        u16::from_be_bytes(user_version)
    }

    pub fn is_complete(&self) -> bool {
        self.bytes[0..10] != [0xff; 10]
    }

    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.bytes
    }
}

impl From<[u8; 12]> for Versionstamp {
    fn from(bytes: [u8; 12]) -> Self {
        Versionstamp { bytes }
    }
}

impl Element<'_> {
    pub fn count_incomplete_versionstamp(&self) -> usize {
        match self {
            Element::Versionstamp(v) if !v.is_complete() => 1,
            Element::Tuple(v) => v.iter().map(Element::count_incomplete_versionstamp).sum(),
            _ => 0,
        }
    }
}
