#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{anyhow, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn check_prerequisites(tables: &[Arc<SsTable>]) -> Result<()> {
        if tables
            .iter()
            .all(|table| table.first_key() <= table.last_key())
        {
            if (1..tables.len()).all(|idx| tables[idx].first_key() > tables[idx - 1].last_key()) {
                Ok(())
            } else {
                Err(anyhow!("Provided tables are not a sorted run"))
            }
        } else {
            Err(anyhow!("First key is greater than last key in sstable"))
        }
    }

    fn iteratate_to_valid_or_end(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            } else {
                // Move to next index if possible
                if self.next_sst_idx == self.sstables.len() {
                    // We have exhausted all sstables
                    self.current = None
                } else {
                    self.current = Some(SsTableIterator::create_and_seek_to_first(
                        self.sstables[self.next_sst_idx].clone(),
                    )?);
                    self.next_sst_idx += 1;
                }
            }
        }
        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        SstConcatIterator::check_prerequisites(&sstables)?;
        let mut iter = Self {
            current: if !sstables.is_empty() {
                Some(SsTableIterator::create_and_seek_to_first(
                    sstables[0].clone(),
                )?)
            } else {
                None
            },
            next_sst_idx: 1,
            sstables,
        };
        iter.iteratate_to_valid_or_end()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        SstConcatIterator::check_prerequisites(&sstables)?;
        // Step 1: Find the sstable which may have the key of our interest
        let key_raw_bytes = key.raw_ref();
        let table_option = sstables
            .iter()
            .position(|table| table.last_key().raw_ref() >= key_raw_bytes);

        // Step 2: If the idx is defined, we use that sstable to start our search
        if let Some(idx) = table_option {
            let mut iter = Self {
                current: Some(SsTableIterator::create_and_seek_to_key(
                    sstables[idx].clone(),
                    key,
                )?),
                next_sst_idx: idx + 1,
                sstables,
            };
            iter.iteratate_to_valid_or_end()?;
            Ok(iter)
        } else {
            // If no idx found, the key is beyond the range of thus sorted run
            Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            })
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = self.current.as_ref() {
            iter.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.iteratate_to_valid_or_end()
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
