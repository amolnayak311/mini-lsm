#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let valid_iterators = iters
            .into_iter()
            .filter(|x| x.is_valid())
            .collect::<Vec<Box<I>>>();
        if valid_iterators.is_empty() {
            Self {
                iters: BinaryHeap::new(),
                current: None,
            }
        } else {
            let mut iters = BinaryHeap::new();
            let len = valid_iterators.len();
            valid_iterators
                .into_iter()
                .zip(0..len)
                .for_each(|(iter, idx)| iters.push(HeapWrapper(idx, iter)));

            let current = iters.pop();
            Self { iters, current }
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.take().unwrap();
        // Is there a better way to not clone and use the KeySlice?
        let key_vec = current.1.key().to_key_vec();
        let current_key = key_vec.as_key_slice();
        self.iters.push(current);
        // We first need to offer current back to heap as it was popped out in last ``next`` call
        // Subsequent while loop will ensure its popped off
        // Pop out all items whose keys are same as the one in current or empty
        while let Some(mut top) = self.iters.pop() {
            if top.1.is_valid() && top.1.key() > current_key {
                // We now have a key that is greater that current
                self.current = Some(top);
                return Ok(());
            } else {
                // The key is either empty or same as the current
                match top.1.next() {
                    Ok(_) => {
                        if top.1.is_valid() {
                            // Offer this to heap again as the iterator is still yielding
                            // valid values
                            self.iters.push(HeapWrapper(top.0, top.1))
                        }
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }
        }
        // Since we reached here, we haven't found the next key to return
        self.current.take();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let num_active_iters_on_heap = self
            .iters
            .iter()
            .map(|item| item.1.num_active_iterators())
            .sum::<usize>();

        num_active_iters_on_heap
            + self
                .current
                .as_ref()
                .map(|head| head.1.num_active_iterators())
                .unwrap_or(0)
    }
}
