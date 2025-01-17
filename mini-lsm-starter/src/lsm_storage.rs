#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_RANGE_BEGIN};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Send a message to stop the flush thread
        self.flush_notifier.send(())?;
        // Wait for the flush thread to stop
        let mut flush_thread = self.flush_thread.lock();
        if let Some(join_handler) = flush_thread.take() {
            join_handler.join().map_err(|err| anyhow!("{:?}", err))?;
        }
        // Now that the flush thread stopped, check in memory immutable MemTables are empty
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        // Now that all ,mem tables are flushed, sync to disk
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn cleanup(&self, sst_ids: Vec<usize>) -> Result<()> {
        for sst_id in sst_ids {
            std::fs::remove_file(&self.path.join(format!("{:05}.sst", sst_id)))?;
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let snapshot = self.state.read();
            Arc::clone(&snapshot)
        };
        let memtable = snapshot.memtable.clone();
        for mem_table in std::iter::once(&memtable).chain(snapshot.imm_memtables.iter()) {
            if let Some(value) = mem_table.get(key) {
                return if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value))
                };
            }
        }

        // Get the sstable where we potentially may have this key
        let table_option = snapshot
            .l0_sstables
            .iter()
            .map(|id| snapshot.sstables[id].clone())
            .find(|table| key >= table.first_key().key_ref() && key <= table.last_key().key_ref());

        if let Some(table) = table_option {
            // This table can possibly have that key, lets try to seek it
            let iter = SsTableIterator::create_and_seek_to_key(
                table,
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            )?;
            if iter.is_valid() && iter.key().key_ref() == key {
                let value = iter.value();
                return if value.is_empty() {
                    // Value is deleted
                    Ok(None)
                } else {
                    Ok(Some(Bytes::copy_from_slice(value)))
                };
            }
        }
        // We still haven't found the key, lets look for it in the level 1 sstables

        let sst_concat_iter = SstConcatIterator::create_and_seek_to_key(
            snapshot.levels[0]
                .1
                .iter()
                .map(|id| snapshot.sstables[id].clone())
                .collect(),
            KeySlice::from_slice(key, TS_RANGE_BEGIN),
        )?;
        if sst_concat_iter.is_valid() && sst_concat_iter.key().key_ref() == key {
            Ok(Some(Bytes::copy_from_slice(sst_concat_iter.value())))
        } else {
            Ok(None)
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Even if its put we will use read(). The reason being memtable uses SkipMap whose
        // put can takes immutable reference and is still thread safe.
        // We will use write lock on the memtable only when we want to freeze any access to memtable
        // itself
        let size = {
            let state_locked = self.state.read();
            state_locked.memtable.put(key, value)?;
            state_locked.memtable.approximate_size()
        };
        // The above code is put in block as we want the read lock to be released before
        // we call try_freeze which potentially can take a write lock.
        self.try_freeze(size)?;
        Ok(())
    }

    fn try_freeze(&self, approximate_size: usize) -> Result<()> {
        if approximate_size > self.options.target_sst_size {
            // Double guard to check if we really need to freeze or some other thread already did it
            let mutex = self.state_lock.lock();
            let state_read_locked = self.state.read();
            if state_read_locked.memtable.approximate_size() > self.options.target_sst_size {
                // No other thread has made the current mutable thread immutable
                // Don't hold the state locked more than needed
                drop(state_read_locked);
                self.force_freeze_memtable(&mutex)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, b"")?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // Step 1: Create new id
        let sst_id = self.next_sst_id();
        // Step 2:  Create new memtable
        let new_mem_table = Arc::new(MemTable::create(sst_id));

        // Step 3: Get a write lock on the state
        let mut state_writelock_guard = self.state.write();
        let mut new_state = state_writelock_guard.as_ref().clone();

        // TODO: Why not do
        //  new_state.memtable = new_mem_table
        //  and use the existing memtable instead of replace as two steps?
        // Step 4: Replace with new memtable in the cloned state
        let old_mem_table = std::mem::replace(&mut new_state.memtable, new_mem_table);

        // Step 5: Put the old_mem_table at index 0 as its the latest immutable one
        new_state.imm_memtables.insert(0, old_mem_table);

        // Step 6: Replace the sstable's state with the new one
        *state_writelock_guard = Arc::new(new_state);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        // Get the Current memtable to flush
        let mem_table_to_flush = {
            let state = self.state.read();
            state
                .imm_memtables
                .last()
                .expect("Expected to see a memtable")
                .clone()
        };

        // Build the sstable, this will be an expensive operation as it needs to do IO
        let mut builder = SsTableBuilder::new(self.options.block_size);
        mem_table_to_flush.flush(&mut builder)?;
        let sst_id = mem_table_to_flush.id();
        let table = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path.join(format!("{:05}.sst", sst_id)),
        )?;

        // Now that we have the table, get a write lock on the state, pop the immutable memtable
        // and insert the created sstable at index 0

        let mut guard = self.state.write();
        let mut state = guard.as_ref().clone();
        state.imm_memtables.pop().unwrap();
        state.l0_sstables.insert(0, sst_id);
        state.sstables.insert(sst_id, Arc::new(table));
        *guard = Arc::new(state);
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn should_scan(
        &self,
        start_bound: Bound<&[u8]>,
        end_bound: Bound<&[u8]>,
        table_begin: KeyBytes,
        table_end: KeyBytes,
    ) -> bool {
        let end_bound_matched = match end_bound {
            Bound::Excluded(key) if key <= table_begin.key_ref() => false,
            Bound::Included(key) if key < table_begin.key_ref() => false,
            _ => true,
        };
        end_bound_matched
            && match start_bound {
                Bound::Excluded(key) if key >= table_end.key_ref() => false,
                Bound::Included(key) if key > table_end.key_ref() => false,
                _ => true,
            }
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mi = MergeIterator::create(
            std::iter::once(&snapshot.memtable)
                .chain(snapshot.imm_memtables.iter())
                .map(|map| Box::new(map.scan(lower, upper)))
                .collect(),
        );

        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[sst_id].clone();
            let (first_key, last_key) = (table.first_key(), table.last_key());
            if self.should_scan(lower, upper, first_key.clone(), last_key.clone()) {
                let iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        if iter.is_valid() && iter.key().key_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                if iter.is_valid() {
                    sst_iters.push(Box::new(iter));
                }
            }
        }

        let l1_sstables_to_scan = snapshot.levels[0]
            .1
            .iter()
            .map(|id| snapshot.sstables[id].clone())
            .collect::<Vec<Arc<SsTable>>>();
        let sst_concat_iter = match lower {
            Bound::Included(included) => SstConcatIterator::create_and_seek_to_key(
                l1_sstables_to_scan,
                KeySlice::from_slice(included, TS_RANGE_BEGIN),
            )?,
            Bound::Excluded(excluded) => {
                let mut iter = SstConcatIterator::create_and_seek_to_key(
                    l1_sstables_to_scan,
                    KeySlice::from_slice(excluded, TS_RANGE_BEGIN),
                )?;
                if iter.is_valid() && iter.key().key_ref() == excluded {
                    iter.next()?;
                }
                iter
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(l1_sstables_to_scan)?,
        };

        let mem_and_l0_iterator = TwoMergeIterator::create(mi, MergeIterator::create(sst_iters))?;
        let iter = TwoMergeIterator::create(mem_and_l0_iterator, sst_concat_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
