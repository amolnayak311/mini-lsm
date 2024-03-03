#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};
use bytes::Bytes;
use std::collections::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    SstConcatIterator,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(inner: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            inner,
            end_bound,
            is_valid: true,
        };
        iter.skip_deletes()?;
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid {
            self.inner.next()?;
            self.skip_deletes()?;
            if self.inner.is_valid() {
                match self.end_bound {
                    Bound::Included(ref key) => {
                        self.is_valid = self.inner.key().raw_ref() <= key.as_ref();
                    }
                    Bound::Excluded(ref key) => {
                        self.is_valid = self.inner.key().raw_ref() < key.as_ref();
                    }
                    Bound::Unbounded => {}
                }
            } else {
                self.is_valid = false;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn value(&self) -> &[u8] {
        if self.has_errored {
            panic!("Nothing more to iterate")
        } else {
            self.iter.value()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored {
            panic!("Iterator has errored out")
        } else {
            self.iter.key()
        }
    }

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Nothing more to iterate")
        } else if self.iter.is_valid() {
            match self.iter.next() {
                Ok(_) => Ok(()),
                Err(e) => {
                    self.has_errored = true;
                    Err(e)
                }
            }
        } else {
            Ok(())
        }
    }
    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
