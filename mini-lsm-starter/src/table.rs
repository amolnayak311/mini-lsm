#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;
use nom::AsBytes;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // compute required additional size of the for the metadata to be added to buffer
        let total_extra_size = block_meta
            .iter()
            .map(|meta| {
                // offset in meta
                std::mem::size_of::<u32>() +
            // size of the first_key
            std::mem::size_of::<u16>() +
            // actual num bytes of the first_key
            meta.first_key.len() +
            // size of the last_key
            std::mem::size_of::<u16>() +
            // actual num bytes of the last_key
            meta.last_key.len()
            })
            .sum::<usize>()
            + std::mem::size_of::<u32>();
        // additional length for storing the number metadata entries

        buf.reserve(total_extra_size);

        buf.put_u32(block_meta.len() as u32);
        block_meta.iter().for_each(|block_meta| {
            buf.put_u32(block_meta.offset as u32);
            buf.put_u16(block_meta.first_key.len() as u16);
            buf.extend(block_meta.first_key.raw_ref());
            buf.put_u16(block_meta.last_key.len() as u16);
            buf.extend(block_meta.last_key.raw_ref());
        });
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        // Read the number of block meta entries to follow
        let num_blocks = buf.get_u32();
        (0..num_blocks)
            .map(|_| {
                let offset = buf.get_u32() as usize;
                let first_key_size = buf.get_u16() as usize;
                let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_size));
                let last_key_size = buf.get_u16() as usize;
                let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_size));
                BlockMeta {
                    offset,
                    first_key,
                    last_key,
                }
            })
            .collect()
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // Read bloom filter
        let mut end_offset = file.size();
        let bloom_offset_vec = file.read(end_offset - 4, 4)?;
        let bloom_offset = (&bloom_offset_vec[..]).get_u32() as u64;
        let bloom_meta_bytes = file.read(bloom_offset, end_offset - bloom_offset - 4)?;
        let bloom = Some(Bloom::decode(bloom_meta_bytes.as_bytes())?);
        end_offset = bloom_offset;

        // read metadata
        let block_meta_offset_vec = file.read(end_offset - 4, 4)?;
        let block_meta_offset = (&block_meta_offset_vec[..]).get_u32() as u64;

        let block_meta_bytes = file.read(block_meta_offset, end_offset - block_meta_offset - 4)?;
        let block_meta = BlockMeta::decode_block_meta(&block_meta_bytes[..]);
        let first_key = if !block_meta.is_empty() {
            block_meta.first().unwrap().first_key.clone()
        } else {
            KeyBytes::from_bytes(Bytes::new())
        };

        let last_key = if !block_meta.is_empty() {
            block_meta.last().unwrap().last_key.clone()
        } else {
            KeyBytes::from_bytes(Bytes::new())
        };

        Ok(SsTable {
            file,
            // The meta blocks that hold info for data blocks.
            block_meta,
            // The offset that indicates the start point of meta blocks in `file`.
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts: 0,
        })
    }

    // Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(
            block_idx < self.block_meta.len(),
            "Invalid block_idx provided"
        );
        let start_block_offset = self.block_meta[block_idx].offset as u64;
        // End is start of next block or start of the metadata in sstable if this is the last block
        let end_block_offset =
            self.block_meta
                .get(block_idx + 1)
                .map_or(self.block_meta_offset, |meta| meta.offset) as u64;
        let block_data = self
            .file
            .read(start_block_offset, end_block_offset - start_block_offset)?;
        Ok(Arc::new(Block::decode(&block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match self.block_cache {
            Some(ref cache) => {
                let cached_block = cache
                    .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                    .map_err(|e| anyhow!("{}", e))?;
                Ok(cached_block)
            }
            None =>
            // Caching not enabled
            {
                self.read_block(block_idx)
            }
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        unimplemented!()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
