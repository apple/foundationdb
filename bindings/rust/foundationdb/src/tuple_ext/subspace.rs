use crate::{KeySelector, RangeOption, Transaction};
use foundationdb_tuple::Subspace;
use std::borrow::Cow;

impl From<&Subspace> for RangeOption<'static> {
    fn from(subspace: &Subspace) -> Self {
        let (begin, end) = subspace.range();

        Self {
            begin: KeySelector::first_greater_or_equal(Cow::Owned(begin)),
            end: KeySelector::first_greater_or_equal(Cow::Owned(end)),
            ..Self::default()
        }
    }
}

impl Transaction {
    pub fn clear_subspace_range(&self, subspace: &Subspace) {
        let (begin, end) = subspace.range();
        self.clear_range(&begin, &end)
    }
}
