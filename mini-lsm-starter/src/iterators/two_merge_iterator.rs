#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    pick_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, pick_a: true };
        iter.advance_b_if_needed()?;
        iter.pick_a = iter.should_pick_a();
        Ok(iter)
    }

    fn should_pick_a(&self) -> bool {
        if !self.a.is_valid() {
            false
        } else if !self.b.is_valid() {
            true
        } else {
            self.a.key() < self.b.key()
        }
    }

    fn advance_b_if_needed(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn value(&self) -> &[u8] {
        if self.pick_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.pick_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn is_valid(&self) -> bool {
        if self.pick_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.pick_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.advance_b_if_needed()?;
        self.pick_a = self.should_pick_a();
        Ok(())
    }
}
