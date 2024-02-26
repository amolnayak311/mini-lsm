#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn expected_block_size(&self) -> usize {
        // The size of data
        self.data.len()
        // size of offsets, each taking a u16 (2 bytes)
        + self.offsets.len() * 2
        // the number of elements stored at the end as u16 (2 bytes)
        + 2
    }
    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // the number 6 is for 3 2 byte values needed for storing the key len, value len and
        // the offset
        assert!(!key.is_empty(), "Key must not be empty");
        let bytes_needed_this_entry = key.len() + value.len() + 6;

        if self.expected_block_size() + bytes_needed_this_entry > self.block_size
            && !self.offsets.is_empty()
        {
            // if adding this key exceeds the block size and the block is not empty, don't
            // allow adding this ket value pair
            false
        } else {
            // This means we either are adding the first kv pair or even after adding this kv pair
            // we don't exceed the target block size
            // Push the offset
            self.offsets.push(self.data.len() as u16);
            // Push the key len
            self.data.put_u16(key.len() as u16);
            // push the key itself
            self.data.put(key.raw_ref());
            // push the value length
            self.data.put_u16(value.len() as u16);
            self.data.put(value);
            if self.first_key.is_empty() {
                self.first_key = key.to_key_vec();
            }
            true
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
