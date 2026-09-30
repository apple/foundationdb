// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use super::*;
use std::fmt::Formatter;
use std::hash::Hash;

/// Represents a well-defined region of keyspace in a FoundationDB database
///
/// It provides a convenient way to use FoundationDB tuples to define namespaces for
/// different categories of data. The namespace is specified by a prefix tuple which is prepended
/// to all tuples packed by the subspace. When unpacking a key with the subspace, the prefix tuple
/// will be removed from the result.
///
/// As a best practice, API clients should use at least one subspace for application data. For
/// general guidance on subspace usage, see the Subspaces section of the [Developer Guide].
///
/// [Developer Guide]: https://apple.github.io/foundationdb/developer-guide.html#subspaces
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Subspace {
    prefix: Vec<u8>,
    versionstamp_offset: VersionstampOffset,
}

impl Display for Subspace {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for &b in &self.prefix {
            if (32..127).contains(&b) && b != b'\\' {
                write!(f, "{}", b as char)?;
            } else if b == b'\\' {
                write!(f, "\\\\")?;
            } else {
                write!(f, "\\x{b:02x}")?;
            }
        }
        Ok(())
    }
}

impl<E: TuplePack> From<E> for Subspace {
    fn from(e: E) -> Self {
        let mut prefix = vec![];
        let versionstamp_offset = pack_into(&e, &mut prefix);
        Self {
            prefix,
            versionstamp_offset,
        }
    }
}

impl Subspace {
    /// `all` returns the Subspace corresponding to all keys in a FoundationDB database.
    pub fn all() -> Self {
        Self {
            prefix: Vec::new(),
            versionstamp_offset: VersionstampOffset::None { size: 0 },
        }
    }

    /// Returns a new Subspace from the provided bytes.
    pub fn from_bytes(prefix: impl Into<Vec<u8>>) -> Self {
        let prefix = prefix.into();
        Self {
            versionstamp_offset: VersionstampOffset::None {
                size: u32::try_from(prefix.len()).expect("data doesn't fit u32"),
            },
            prefix,
        }
    }

    /// Convert into prefix key bytes
    pub fn into_bytes(self) -> Vec<u8> {
        self.prefix
    }

    /// Returns a new Subspace whose prefix extends this Subspace with a given tuple encodable.
    pub fn subspace<T: TuplePack>(&self, t: &T) -> Self {
        let mut prefix = self.prefix.clone();
        let mut versionstamp_offset = self.versionstamp_offset;
        versionstamp_offset += pack_into(t, &mut prefix);
        Self {
            prefix,
            versionstamp_offset,
        }
    }

    /// `bytes` returns the literal bytes of the prefix of this Subspace.
    pub fn bytes(&self) -> &[u8] {
        self.prefix.as_slice()
    }

    /// Returns the key encoding the specified Tuple with the prefix of this Subspace
    /// prepended.
    pub fn pack<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        let mut out = self.prefix.clone();
        pack_into(t, &mut out);
        out
    }

    /// Returns the key encoding the specified Tuple with the prefix of this Subspace
    /// prepended, with a versionstamp.
    pub fn pack_with_versionstamp<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        let mut output = self.prefix.clone();
        let mut versionstamp_offset = self.versionstamp_offset;
        versionstamp_offset += pack_into(t, &mut output);
        match versionstamp_offset {
            VersionstampOffset::OneIncomplete { offset } => {
                output.extend_from_slice(&offset.to_le_bytes());
            }
            VersionstampOffset::MultipleIncomplete => {
                panic!("Subspace cannot contain more than one incomplete versionstamp");
            }
            _ => {}
        }
        output
    }

    /// `unpack` returns the Tuple encoded by the given key with the prefix of this Subspace
    /// removed.  `unpack` will return an error if the key is not in this Subspace or does not
    /// encode a well-formed Tuple.
    pub fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        if !self.is_start_of(key) {
            return Err(PackError::BadPrefix);
        }
        let key = &key[self.prefix.len()..];
        unpack(key)
    }

    /// `is_start_of` returns true if the provided key starts with the prefix of this Subspace,
    /// indicating that the Subspace logically contains the key.
    pub fn is_start_of(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }

    /// `range` returns first and last key of given Subspace
    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        let mut begin = Vec::with_capacity(self.prefix.len() + 1);
        begin.extend_from_slice(&self.prefix);
        begin.push(0x00);

        let mut end = Vec::with_capacity(self.prefix.len() + 1);
        end.extend_from_slice(&self.prefix);
        end.push(0xff);

        (begin, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn sub() {
        let ss0: Subspace = 1.into();
        let ss1 = ss0.subspace(&2);

        let ss2: Subspace = (1, 2).into();

        assert_eq!(ss1.bytes(), ss2.bytes());
    }

    #[test]
    fn pack_unpack() {
        let ss0: Subspace = 1.into();
        let tup = (2, 3);

        let packed = ss0.pack(&tup);
        let expected = pack(&(1, 2, 3));
        assert_eq!(expected, packed);

        let tup_unpack: (i64, i64) = ss0.unpack(&packed).unwrap();
        assert_eq!(tup, tup_unpack);

        assert!(ss0.unpack::<(i64, i64, i64)>(&packed).is_err());
    }

    #[test]
    fn subspace_pack_with_versionstamp() {
        let subspace: Subspace = 1.into();
        let tup = (Versionstamp::incomplete(0), 2);

        let packed = subspace.pack_with_versionstamp(&tup);
        let expected = pack_with_versionstamp(&(1, Versionstamp::incomplete(0), 2));
        assert_eq!(expected, packed);
    }

    #[test]
    fn subspace_unpack_with_versionstamp() {
        // On unpack, the versionstamp will be complete, so pack_with_versionstamp won't append
        // the offset.
        let subspace: Subspace = 1.into();
        let tup = (Versionstamp::complete([1; 10], 0), 2);
        let packed = subspace.pack_with_versionstamp(&tup);
        let tup_unpack: (Versionstamp, i64) = subspace.unpack(&packed).unwrap();
        assert_eq!(tup, tup_unpack);

        assert!(
            subspace
                .unpack::<(i64, Versionstamp, i64)>(&packed)
                .is_err()
        );
    }

    #[test]
    fn unpack_with_subspace_versionstamp() {
        let subspace: Subspace = Versionstamp::complete([1; 10], 2).into();
        let tup = (Versionstamp::complete([1; 10], 0), 2);
        let packed = subspace.pack_with_versionstamp(&tup);
        let tup_unpack: (Versionstamp, i64) = subspace.unpack(&packed).unwrap();
        assert_eq!(tup, tup_unpack);
        assert!(
            subspace
                .unpack::<(Versionstamp, Versionstamp, i64)>(&packed)
                .is_err()
        );
    }

    #[test]
    fn subspace_can_use_incomplete_versionstamp() {
        let subspace: Subspace = Versionstamp::incomplete(0).into();
        let tup = (1, 2);

        let packed = subspace.pack_with_versionstamp(&tup);
        let expected = pack_with_versionstamp(&(Versionstamp::incomplete(0), 1, 2));
        assert_eq!(expected, packed);
    }

    #[test]
    fn child_subspace_can_use_incomplete_versionstamp() {
        let subspace: Subspace = 1.into();
        let subspace = subspace.subspace(&Versionstamp::incomplete(0));
        let tup = (1, 2);
        let packed = subspace.pack_with_versionstamp(&tup);
        let expected = pack_with_versionstamp(&(1, Versionstamp::incomplete(0), 1, 2));
        assert_eq!(expected, packed);
    }

    #[test]
    #[should_panic]
    fn subspace_cannot_use_multiple_incomplete_versionstamps() {
        let subspace: Subspace = Versionstamp::incomplete(0).into();
        let subspace = subspace.subspace(&Versionstamp::incomplete(0));
        subspace.pack_with_versionstamp(&1);
    }

    #[test]
    #[should_panic]
    fn subspace_cannot_pack_multiple_incomplete_versionstamps() {
        let subspace: Subspace = Versionstamp::incomplete(0).into();
        subspace.pack_with_versionstamp(&Versionstamp::incomplete(0));
    }

    #[test]
    fn is_start_of() {
        let ss0: Subspace = 1.into();
        let ss1: Subspace = 2.into();
        let tup = (2, 3);

        assert!(ss0.is_start_of(&ss0.pack(&tup)));
        assert!(!ss1.is_start_of(&ss0.pack(&tup)));
        assert!(Subspace::from("start").is_start_of(&pack(&"start")));
        assert!(Subspace::from("start").is_start_of(&pack(&"start".to_string())));
        assert!(!Subspace::from("start").is_start_of(&pack(&"starting")));
        assert!(Subspace::from("start").is_start_of(&pack(&("start", "end"))));
        assert!(Subspace::from(("start", 42)).is_start_of(&pack(&("start", 42, "end"))));
    }

    #[test]
    fn range() {
        let ss: Subspace = 1.into();
        let tup = (2, 3);
        let packed = ss.pack(&tup);

        let (begin, end) = ss.range();
        assert!(packed >= begin && packed <= end);
    }

    #[test]
    fn equality() {
        let sub1 = Subspace::all().subspace(&"test");
        let sub2 = Subspace::all().subspace(&"test");
        let sub3 = Subspace::all().subspace(&"test2");
        assert_eq!(sub1, sub2);
        assert_ne!(sub1, sub3);
    }

    #[test]
    fn hash() {
        let sub1 = Subspace::all().subspace(&"test");
        let sub2 = Subspace::all().subspace(&"test2");

        let map: HashMap<Subspace, u8> = HashMap::from([(sub1, 1), (sub2, 2)]);

        assert_eq!(map.get(&Subspace::all().subspace(&"test")).unwrap(), &1);
        assert_eq!(map.get(&Subspace::all().subspace(&"test2")).unwrap(), &2);
    }

    #[test]
    fn display() {
        let sub1 = Subspace::all().subspace(&"test");
        let sub2 = Subspace::all().subspace(&"test2").subspace(&"path1");
        let sub3 = Subspace::all().subspace(&"test3").subspace(&"__커피__");
        let sub4 = Subspace::all().subspace(&"test4\\path1\\path2");

        assert_eq!(format!("{sub1}"), r#"\x02test\x00"#);
        assert_eq!(format!("{sub2}"), r#"\x02test2\x00\x02path1\x00"#);
        assert_eq!(
            format!("{sub3}"),
            r#"\x02test3\x00\x02__\xec\xbb\xa4\xed\x94\xbc__\x00"#
        );
        assert_eq!(format!("{sub4}"), r#"\x02test4\\path1\\path2\x00"#);
    }
}
