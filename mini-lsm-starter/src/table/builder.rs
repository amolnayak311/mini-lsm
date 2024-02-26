#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        debug_assert!(!key.is_empty(), "Key is empty");
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        if self.builder.add(key, value) {
            // We were able to key value pair to existing memtable
            self.last_key = key.to_key_vec().into_inner();
        } else {
            self.freeze_block();
            assert!(self.builder.add(key, value));
            self.first_key = key.to_key_vec().into_inner();
            self.last_key = key.to_key_vec().into_inner();
        }
    }

    fn freeze_block(&mut self) {
        // We are unable to add the key to block, lets create a new block and add the
        // old block to metadata
        let old_block_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let old_block = old_block_builder.build();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.first_key))),
            last_key: KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.last_key))),
        });
        self.data.extend(old_block.encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.freeze_block();
        let first_key = if !self.meta.is_empty() {
            self.meta.first().unwrap().first_key.clone()
        } else {
            KeyBytes::from_bytes(Bytes::from(self.first_key))
        };
        let last_key = if !self.meta.is_empty() {
            self.meta.last().unwrap().last_key.clone()
        } else {
            KeyBytes::from_bytes(Bytes::from(self.last_key))
        };
        let block_meta_offset = self.data.len();
        let mut buff = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buff);
        buff.put_u32(block_meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buff)?;
        Ok(SsTable {
            file,
            // The meta blocks that hold info for data blocks.
            block_meta: self.meta,
            // The offset that indicates the start point of meta blocks in `file`.
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            // No bloom filter for now
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
