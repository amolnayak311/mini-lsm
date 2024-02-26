use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        // Skip binary search for now for simplicity
        let (blk_idx, blk_iter) = SsTableIterator::create_block_iterator(table.clone(), key)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    fn create_block_iterator(table: Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let (blk_idx, blk_iterator) = match table
            .block_meta
            .iter()
            .position(|meta| meta.last_key.as_key_slice() >= key)
        {
            Some(block_idx) => {
                // We may find the key of our interest in this block
                let block = table.read_block_cached(block_idx)?;
                (block_idx, BlockIterator::create_and_seek_to_key(block, key))
            }
            None => {
                // This means our key is way above the range of our SS table's last_key,
                // simply return and empty SsTableIterator iterator without loading any blocks
                (0, BlockIterator::empty())
            }
        };
        Ok((blk_idx, blk_iterator))
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = SsTableIterator::create_block_iterator(self.table.clone(), key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.blk_idx += 1;
            self.blk_iter =
                BlockIterator::create_and_seek_to_first(self.table.read_block_cached(self.blk_idx)?)
        }
        Ok(())
    }
}
