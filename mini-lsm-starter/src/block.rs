#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = self.data.clone();
        for offset in &self.offsets {
            bytes.put_u16(*offset);
        }
        bytes.put_u16(self.offsets.len() as u16);
        Bytes::from(bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // last two bytes are the number of entries
        let offset_end_exclusive = data.len() - 2;
        let num_entries = (&data[offset_end_exclusive..]).get_u16() as usize;
        // num_entries * 2 bytes before the offset_end_exclusive are the offsets
        let data_end_exclusive = offset_end_exclusive - num_entries * 2;
        let offset_data = &data[data_end_exclusive..offset_end_exclusive];
        let offsets = offset_data
            .chunks(2)
            .map(|mut two_bytes| two_bytes.get_u16())
            .collect::<Vec<u16>>();
        Self {
            data: Vec::from(&data[..data_end_exclusive]),
            offsets,
        }
    }
}
