#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn build_ssts_from_iterator(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut sstable_builder = None;
        let mut ss_tables = vec![];
        while iter.is_valid() {
            if sstable_builder.is_none() {
                sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder = sstable_builder.as_mut().unwrap();
            let (key, value) = (iter.key(), iter.value());
            if !value.is_empty() {
                builder.add(key, value);
            }
            iter.next()?;

            if builder.estimated_size() >= self.options.target_sst_size {
                // Build the sstable
                let sst_id = self.next_sst_id();
                ss_tables.push(Arc::new(sstable_builder.take().unwrap().build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
            }
        }
        if let Some(builder) = sstable_builder {
            let sst_id = self.next_sst_id();
            ss_tables.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }
        Ok(ss_tables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = { self.state.read().clone() };
        match task {
            CompactionTask::Leveled(_) => {
                unimplemented!()
            }
            CompactionTask::Tiered(_) => {
                unimplemented!()
            }
            CompactionTask::Simple(_) => {
                unimplemented!()
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.run_full_compaction(snapshot, l0_sstables, l1_sstables),
        }
    }

    fn run_full_compaction(
        &self,
        snapshot: Arc<LsmStorageState>,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        // Step 1: Create a merge iterator for level0 sstables as the keys are not guaranteed to
        // be non overlapping multiple sstables are not guaranteed to be sorted strictly by
        // keys. One sstable in isolation is sorted
        let mut lo_tables = Vec::with_capacity(l0_sstables.len());
        for id in l0_sstables {
            lo_tables.push(Box::new(SsTableIterator::create_and_seek_to_first(
                snapshot.sstables[id].clone(),
            )?));
        }
        let l0_merge_iterator = MergeIterator::create(lo_tables);

        // Step 2: Create SSTConcatIterator. This is more efficient than merge iterator
        // because we know L1 SSTs are ordered by keys and there is no overlap of keys between
        // these. In other words, L1 is a sorted run while L0 is not

        let concat_iterator = SstConcatIterator::create_and_seek_to_first(
            l1_sstables
                .iter()
                .map(|sst_id| snapshot.sstables[sst_id].clone())
                .collect(),
        )?;

        let iter = TwoMergeIterator::create(l0_merge_iterator, concat_iterator)?;

        self.build_ssts_from_iterator(iter)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // Step 1: Get the snapshot of the current level0 and level 1 sstables.
        let (l0_tables, l1_tables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        // Step 2: Take these l0 and l1 tables and provide them to compaction to get new set
        // of l1 tables, compaction is expensive and time consuming operation
        let compacted_tables = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_tables.clone(),
            l1_sstables: l1_tables.clone(),
        })?;

        if !compacted_tables.is_empty() {
            // Step 3: Now take a state_lock and check if no one else performed the compaction
            // by checking l1 ssts are same as when we took a snapshot

            let _state_lock = self.state_lock.lock();

            let mut new_state = self.state.read().as_ref().clone();
            if new_state.levels[0].1 != l1_tables {
                // This means some other thread has already compacted, we should stop performing
                // any more changes, clean up the newly merged sstables
                self.cleanup(
                    compacted_tables
                        .iter()
                        .map(|table| table.sst_id())
                        .collect(),
                )?;
                Ok(())
            } else {
                // Since we are here, there is no other compaction that happened while
                // we compacted the l0 and l1 files

                // Step 4: Remove references to l0_tables and l1_tables (old sstables) from the state
                for ids in l0_tables.iter().chain(&l1_tables) {
                    let res = new_state.sstables.remove(ids);
                    // Removal should be successful
                    debug_assert!(res.is_some());
                }

                // Step 5: Add the new sstables to the map
                for table in &compacted_tables {
                    let res = new_state.sstables.insert(table.sst_id(), table.clone());
                    // There should be no sstable with that id
                    debug_assert!(res.is_none());
                }

                //Step 6: Set the new Level 1 tables as the newly compacted ones
                new_state.levels[0].1 = compacted_tables.iter().map(|t| t.sst_id()).collect();

                // Step 7: remove the already compacted l0 tables
                let mut id_set: HashSet<usize> = l0_tables.into_iter().collect();
                // Retain only those ids which were not present in the initial snapshot of
                // ids we took before compaction.
                new_state.l0_sstables.retain(|id| !id_set.remove(id));
                // this id_set should be empty
                debug_assert!(id_set.is_empty());
                // Step 8: Take write lock and update the state
                // TODO: Validate we have the state_lock each time we take state.write()
                *self.state.write() = Arc::new(new_state);
                self.sync_dir()?;
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = {
            let state_locked = self.state.read();
            state_locked.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if should_flush {
            self.force_flush_next_imm_memtable()
        } else {
            Ok(())
        }
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
