// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! A `KeySelector` identifies a particular key in the database.

use crate::tuple::Bytes;
use std::borrow::Cow;

/// A `KeySelector` identifies a particular key in the database.
///
/// FoundationDB's lexicographically ordered data model permits finding keys based on their order
/// (for example, finding the first key in the database greater than a given key). Key selectors
/// represent a description of a key in the database that could be resolved to an actual key by
/// `Transaction::get_key` or used directly as the beginning or end of a range in
/// `Transaction::getRange`.
///
/// Note that the way the key selectors are resolved is somewhat non-intuitive, so users who wish
/// to use a key selector other than the default ones described below should probably consult that
/// documentation before proceeding.
///
/// Generally one of the following static methods should be used to construct a KeySelector:
///
/// - `last_less_than`
/// - `last_less_or_equal`
/// - `first_greater_than`
/// - `first_greater_or_equal`
///
/// A dedicated [example](https://github.com/foundationdb-rs/foundationdb-rs/blob/main/foundationdb/examples/key_selectors.rs) is available on Github.
#[derive(Clone, Debug)]
pub struct KeySelector<'a> {
    key: Bytes<'a>,
    or_equal: bool,
    offset: i32,
}

impl<'a> KeySelector<'a> {
    /// Constructs a new KeySelector from the given parameters.
    pub const fn new(key: Cow<'a, [u8]>, or_equal: bool, offset: i32) -> Self {
        Self {
            key: Bytes(key),
            or_equal,
            offset,
        }
    }

    /// Returns a the key that serves as the anchor for this `KeySelector`
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    /// True if this is an `or_equal` `KeySelector`
    pub fn or_equal(&self) -> bool {
        self.or_equal
    }

    /// Returns the key offset parameter for this `KeySelector`
    pub fn offset(&self) -> i32 {
        self.offset
    }

    /// Creates a `KeySelector` that picks the last key less than the parameter
    pub fn last_less_than<K: Into<Cow<'a, [u8]>>>(key: K) -> Self {
        Self::new(key.into(), false, 0)
    }

    /// Creates a `KeySelector` that picks the last key less than or equal to the parameter
    pub fn last_less_or_equal<K: Into<Cow<'a, [u8]>>>(key: K) -> Self {
        Self::new(key.into(), true, 0)
    }

    /// Creates a `KeySelector` that picks the first key greater than the parameter
    pub fn first_greater_than<K: Into<Cow<'a, [u8]>>>(key: K) -> Self {
        Self::new(key.into(), true, 1)
    }

    /// Creates a `KeySelector` that picks the first key greater than or equal to the parameter
    pub fn first_greater_or_equal<K: Into<Cow<'a, [u8]>>>(key: K) -> Self {
        Self::new(key.into(), false, 1)
    }

    fn make_key(&mut self, key: &[u8]) {
        match &mut self.key {
            Bytes(Cow::Borrowed(..)) => self.key = Bytes::from(key.to_owned()),
            Bytes(Cow::Owned(vec)) => {
                vec.clear();
                vec.extend_from_slice(key);
            }
        };
    }

    pub(crate) fn make_first_greater_or_equal(&mut self, key: &[u8]) {
        self.make_key(key);
        self.or_equal = false;
        self.offset = 1;
    }

    pub(crate) fn make_first_greater_than(&mut self, key: &[u8]) {
        self.make_key(key);
        self.or_equal = true;
        self.offset = 1;
    }
}
