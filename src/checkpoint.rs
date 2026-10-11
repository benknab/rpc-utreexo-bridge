//! Two durable forest checkpoints plus the chain view's bounded delta journal.
//! Forest snapshots never contain Bitcoin blocks. The journal is discarded only
//! after the replacement checkpoint and its directory entry are durable.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use bitcoin::consensus::{deserialize, serialize};
use rustreexo::accumulator::mem_forest::MemForest;

use crate::chainview::{ChainView, Tip};
use crate::prover::AccumulatorHash;
use crate::udata::LeafData;

const MAGIC: &[u8; 8] = b"BRDGACC2";

pub struct Checkpoints {
    directory: PathBuf,
}

impl Checkpoints {
    pub fn new(directory: PathBuf) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        // A crash can leave an unpublished full-size snapshot. Reclaim it on
        // startup rather than retaining one large temporary file per crash.
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                fs::remove_file(path)?;
            }
        }
        File::open(&directory)?.sync_all()?;
        Ok(Self { directory })
    }

    fn entries(&self) -> Result<Vec<(u32, PathBuf)>> {
        let mut entries = Vec::new();
        for file in fs::read_dir(&self.directory)? {
            let path = file?.path();
            if path.extension().is_some_and(|ext| ext == "acc") {
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("invalid checkpoint name")?;
                let height = name
                    .split('-')
                    .next()
                    .context("missing checkpoint height")?
                    .parse()?;
                entries.push((height, path));
            }
        }
        entries.sort_by_key(|(height, _)| *height);
        Ok(entries)
    }

    fn read_tip(reader: &mut impl Read) -> Result<Tip> {
        let mut magic = [0; 8];
        reader.read_exact(&mut magic)?;
        ensure!(&magic == MAGIC, "unsupported forest checkpoint");
        let mut height = [0; 4];
        let mut hash = [0; 32];
        reader.read_exact(&mut height)?;
        reader.read_exact(&mut hash)?;
        Ok(Tip {
            height: u32::from_le_bytes(height),
            block_hash: deserialize(&hash)?,
        })
    }

    /// Reconstruct the forest at a committed ancestor. In particular this also
    /// works after an unclean exit between a DB commit and a snapshot.
    pub fn restore(&self, view: &ChainView, target: Tip) -> Result<MemForest<AccumulatorHash>> {
        let mut forest = MemForest::new_with_hash();
        let mut start = 0;
        for (height, path) in self.entries()?.into_iter().rev() {
            if height > target.height {
                continue;
            }
            let mut reader = BufReader::new(File::open(&path)?);
            let tip = Self::read_tip(&mut reader)?;
            ensure!(tip.height == height, "checkpoint height mismatch");
            if view.get_block_hash(height)? != Some(tip.block_hash) {
                continue;
            }
            forest = MemForest::deserialize(reader).context("invalid forest checkpoint")?;
            ensure!(
                acc_bytes(&forest)
                    == view
                        .get_acc(tip.block_hash)?
                        .context("missing checkpoint roots")?,
                "checkpoint roots differ from committed state"
            );
            start = height;
            break;
        }
        let mut parent = view
            .get_block_hash(start)?
            .context("missing replay origin")?;
        for height in start + 1..=target.height {
            let delta = view
                .delta(height)?
                .context("recovery exceeds retained checkpoints; rebuild with a fresh DATA_DIR")?;
            ensure!(
                delta.parent == parent
                    && delta.tip.height == height
                    && view.get_block_hash(height)? == Some(delta.tip.block_hash),
                "recovery journal chain mismatch"
            );
            let adds = delta
                .adds
                .iter()
                .map(LeafData::get_leaf_hashes)
                .collect::<Vec<_>>();
            let dels = delta
                .dels
                .iter()
                .map(LeafData::get_leaf_hashes)
                .collect::<Vec<_>>();
            forest.modify(&adds, &dels).map_err(anyhow::Error::msg)?;
            ensure!(
                acc_bytes(&forest)
                    == view
                        .get_acc(delta.tip.block_hash)?
                        .context("missing replay roots")?,
                "replayed accumulator differs from committed roots"
            );
            parent = delta.tip.block_hash;
        }
        ensure!(parent == target.block_hash, "recovery target mismatch");
        Ok(forest)
    }

    pub fn save(
        &self,
        view: &ChainView,
        tip: Tip,
        forest: &MemForest<AccumulatorHash>,
    ) -> Result<()> {
        // Make the journal/leaf state durable before publishing a snapshot of it.
        view.flush()?;
        let path = self
            .directory
            .join(format!("{:010}-{}.acc", tip.height, tip.block_hash));
        atomic_write(&path, |writer| {
            writer.write_all(MAGIC)?;
            writer.write_all(&tip.height.to_le_bytes())?;
            writer.write_all(&serialize(&tip.block_hash))?;
            forest.serialize(writer)?;
            Ok(())
        })?;
        self.discard_after(view, tip.height)?;
        let entries = self.entries()?;
        let remove = entries.len().saturating_sub(2);
        for (_, path) in &entries[..remove] {
            fs::remove_file(path)?;
        }
        File::open(&self.directory)?.sync_all()?;
        if let Some((oldest, _)) = entries.get(remove) {
            view.prune_journal(*oldest)?;
            view.flush()?;
        }
        Ok(())
    }

    pub fn discard_after(&self, view: &ChainView, height: u32) -> Result<()> {
        for (entry_height, path) in self.entries()? {
            let tip = Self::read_tip(&mut BufReader::new(File::open(&path)?))?;
            if entry_height > height || view.get_block_hash(entry_height)? != Some(tip.block_hash) {
                fs::remove_file(path)?;
            }
        }
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

pub fn acc_bytes(forest: &MemForest<AccumulatorHash>) -> Vec<u8> {
    use bitcoin::consensus::Encodable;
    let mut bytes = forest.leaves.to_le_bytes().to_vec();
    for root in forest.get_roots() {
        root.get_data()
            .consensus_encode(&mut bytes)
            .expect("write roots to memory");
    }
    bytes
}

pub fn atomic_write(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
) -> Result<()> {
    let temp = path.with_extension("tmp");
    let mut writer = BufWriter::new(File::create(&temp)?);
    write(&mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(temp, path)?;
    File::open(path.parent().context("file has no parent")?)?.sync_all()?;
    Ok(())
}
