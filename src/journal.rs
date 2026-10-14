//! Recovery payloads live in files, never in Sled's blob/page cache.
//! Publish a durable file before committing its small reference with the tip.

use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use bitcoin::hashes::{sha256, Hash, HashEngine};
use bitcoin::BlockHash;

use crate::chainview::{Delta, Tip};
use crate::checkpoint::atomic_write;

const MAGIC: &[u8; 8] = b"BRDGJNL1";

pub struct Journal {
    directory: PathBuf,
}

impl Journal {
    pub fn new(directory: PathBuf) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        // Persist the directory itself before a DB reference can point into it.
        File::open(directory.parent().context("journal has no parent")?)?.sync_all()?;
        Ok(Self { directory })
    }

    fn path(&self, tip: Tip) -> PathBuf {
        self.directory
            .join(format!("{:010}-{}.json", tip.height, tip.block_hash))
    }

    pub fn reference_hash(reference: &[u8]) -> Result<BlockHash> {
        ensure!(
            reference.len() == 72 && reference.starts_with(MAGIC),
            "invalid journal reference"
        );
        Ok(BlockHash::from_slice(&reference[8..40])?)
    }

    pub fn is_reference(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }

    pub fn write(&self, delta: &Delta) -> Result<Vec<u8>> {
        let mut engine = sha256::Hash::engine();
        atomic_write(&self.path(delta.tip), |writer| {
            serde_json::to_writer(
                CheckedIo {
                    inner: writer,
                    engine: &mut engine,
                },
                delta,
            )?;
            Ok(())
        })?;
        let mut reference = MAGIC.to_vec();
        reference.extend_from_slice(delta.tip.block_hash.as_byte_array());
        reference.extend_from_slice(sha256::Hash::from_engine(engine).as_byte_array());
        Ok(reference)
    }

    pub fn read(&self, height: u32, reference: &[u8]) -> Result<Delta> {
        let tip = Tip {
            height,
            block_hash: Self::reference_hash(reference)?,
        };
        let file = File::open(self.path(tip)).context("missing committed recovery file")?;
        let mut engine = sha256::Hash::engine();
        let delta: Delta = serde_json::from_reader(BufReader::new(CheckedIo {
            inner: file,
            engine: &mut engine,
        }))?;
        ensure!(
            sha256::Hash::from_engine(engine).as_byte_array() == &reference[40..],
            "recovery file checksum mismatch"
        );
        ensure!(delta.tip == tip, "recovery file identity mismatch");
        Ok(delta)
    }

    pub fn remove(&self, tip: Tip) -> Result<()> {
        match fs::remove_file(self.path(tip)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn sync(&self) -> Result<()> {
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    /// A crash can leave an unpublished file, an old fork, or half-done pruning.
    /// Inspect names and small DB references only, not the recovery payloads.
    pub fn clean_orphans(&self, mut keep: impl FnMut(Tip) -> Result<bool>) -> Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                fs::remove_file(path)?;
            } else if path.extension().is_some_and(|ext| ext == "json") {
                let tip = file_tip(&path)?;
                if !keep(tip)? {
                    fs::remove_file(path)?;
                }
            }
        }
        self.sync()
    }
}

fn file_tip(path: &Path) -> Result<Tip> {
    let (height, hash) = path
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.split_once('-'))
        .context("invalid journal filename")?;
    Ok(Tip {
        height: height.parse()?,
        block_hash: hash.parse()?,
    })
}

struct CheckedIo<'a, T> {
    inner: T,
    engine: &'a mut sha256::HashEngine,
}

impl<T: Write> Write for CheckedIo<'_, T> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(bytes)?;
        self.engine.input(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Read> Read for CheckedIo<'_, T> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(bytes)?;
        self.engine.input(&bytes[..n]);
        Ok(n)
    }
}
