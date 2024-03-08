#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::Buf;
use std::sync::Arc;

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    pub fn empty() -> Self {
        BlockIterator::new(Arc::new(Block {
            data: vec![],
            offsets: vec![],
        }))
    }
    fn init(&mut self) {
        let key_len = (&self.block.data[2..4]).get_u16() as usize;
        let key = &self.block.data[4..4 + key_len];
        // next 8 bytes are timestamp
        let ts = (&self.block.data[key_len..key_len + 8]).get_u64();
        // last
        self.first_key = KeyVec::from_vec_with_ts(key.to_vec(), ts);
        self.seek_to_offset(0);
    }

    fn seek_to_offset(&mut self, idx: usize) {
        // seeks to the given offset
        if idx == self.block.offsets.len() {
            // we reached the end and cannot iterate any further
            self.key = KeyVec::new();
        } else {
            self.idx = idx;
            self.key = self.key_at_offset(idx);
        }
    }

    fn key_at_offset(&self, offset: usize) -> KeyVec {
        debug_assert!(
            offset < self.block.offsets.len(),
            "provided offset {} is out of bounds",
            offset
        );
        self.key_at_raw_offset(self.block.offsets[offset] as usize)
    }

    fn value_at_offset(&self, offset: usize) -> &[u8] {
        debug_assert!(
            offset < self.block.offsets.len(),
            "provided offset {} is out of bounds",
            offset
        );
        let this_kv_start_raw_idx = self.block.offsets[offset] as usize;

        let key_length = (&self.block.data[this_kv_start_raw_idx + 2..this_kv_start_raw_idx + 4])
            .get_u16() as usize;
        // start offset of value is length of the key + 2 (length of the key prefix) + 2(length of key suffix) + 8 byte timestamp
        self.value_at_raw_offset(this_kv_start_raw_idx + key_length + 12)
    }

    fn value_at_raw_offset(&self, raw_offset: usize) -> &[u8] {
        debug_assert!(
            raw_offset < self.block.data.len(),
            "provided raw_offset {} is out of bounds",
            raw_offset
        );
        let value_length = (&self.block.data[raw_offset..raw_offset + 2]).get_u16() as usize;
        let value_start_idx = raw_offset + 2;
        &self.block.data[value_start_idx..value_start_idx + value_length]
    }

    fn key_at_raw_offset(&self, raw_offset: usize) -> KeyVec {
        // gets the key at offset. The offset is the raw offset in the data byte array
        debug_assert!(
            raw_offset < self.block.data.len(),
            "provided raw_offset {} is out of bounds",
            raw_offset
        );

        let key_prefix_len = (&self.block.data[raw_offset..raw_offset + 2]).get_u16() as usize;
        let remaining_key_length =
            (&self.block.data[raw_offset + 2..raw_offset + 4]).get_u16() as usize;
        let key_start_idx = raw_offset + 4;
        let key_suffix = &self.block.data[key_start_idx..key_start_idx + remaining_key_length];
        // read u64 after this suffix
        let key_ts_index = raw_offset + 4 + remaining_key_length;
        let ts = (&self.block.data[key_ts_index..key_ts_index + 8]).get_u64();
        let mut key_vec = Vec::with_capacity(key_prefix_len + key_suffix.len());
        key_vec.extend(&self.first_key.key_ref()[..key_prefix_len]);
        key_vec.extend(key_suffix);
        Key::from_vec_with_ts(key_vec, ts)
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        iter.init();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        iter.init();
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        assert!(self.is_valid(), "Invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "Invalid iterator");
        self.value_at_offset(self.idx)
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_offset(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        assert!(self.is_valid(), "Invalid iterator");
        self.idx += 1;
        self.seek_to_offset(self.idx)
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut low, mut high) = (0, self.block.offsets.len());
        while low < high {
            let mid = (low + high) / 2;
            let key_vec_at_offset = self.key_at_offset(mid);
            self.seek_to_offset(mid);
            let key_ = self.key();
            match key.cmp(&key_) {
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => low = mid + 1,
            }
        }
        self.seek_to_offset(low);
    }
}
