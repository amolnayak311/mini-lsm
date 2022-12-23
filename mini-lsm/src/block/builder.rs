use bytes::BufMut;

use super::{Block, SIZEOF_U16};

/// Builds a block
pub struct BlockBuilder {
    offsets: Vec<u16>,
    data: Vec<u8>,
    target_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder
    pub fn new(target_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            target_size,
        }
    }

    fn estimated_size(&self) -> usize {
        self.offsets.len() * SIZEOF_U16 + self.data.len() + SIZEOF_U16
    }

    /// Adds a key-value pair to the block
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.target_size
            && !self.is_empty()
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Builds a block
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}