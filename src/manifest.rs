use crate::{
    constants::BUF_SIZE, error::{KeplerErr, KeplerResult}, sst_writer::FlushResult, types::WorkerSignal, version::{SSTInfo, Version}
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::{
        Arc,
        mpsc::{Receiver, Sender, SyncSender, sync_channel},
    },
    thread,
};

pub(crate) struct Manifest {
    pub sender: SyncSender<FlushResult>,
}

impl Manifest {
    pub fn new(path: &Path, err_tx: Sender<WorkerSignal>) -> KeplerResult<(Arc<Self>, Version)> {
        let manifest_path = path.join("manifest");
        let (manifest_tx, manifest_rx) = sync_channel::<FlushResult>(8);
        let version = restore_sst_list(&manifest_path)?;
        start_manifest_thread(&manifest_path, err_tx, manifest_rx)?;

        Ok((
            Arc::new(Self {
                sender: manifest_tx,
            }),
            version,
        ))
    }

    pub fn send(&self, result: FlushResult) -> KeplerResult<()> {
        self.sender
            .send(result)
            .map_err(|_| KeplerErr::ManifestCorrupted(0))?;
        Ok(())
    }
}

fn start_manifest_thread(
    manifest_path: &Path,
    err_tx: Sender<WorkerSignal>,
    manifest_rx: Receiver<FlushResult>,
) -> KeplerResult<()> {
    let manifest = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(manifest_path)?;

    thread::spawn(move || {
        let mut buf = BufWriter::new(manifest);
        // type(1) + sstno(8) + max_seqno(8) + min_seqno(8)
        let mut process = || -> Result<(), std::io::Error> {
            while let Ok(result) = manifest_rx.recv() {
                buf.write_all(&[result.t])?;
                buf.write_all(&result.sstno.to_le_bytes())?;
                buf.write_all(&result.max_seqno.to_le_bytes())?;
                buf.write_all(&result.min_seqno.to_le_bytes())?;
                buf.flush()?;
                buf.get_mut().sync_all()?;
            }
            Ok(())
        };

        if let Err(e) = process() {
            let _ = err_tx.send(WorkerSignal::Panic(KeplerErr::Io(e)));
        }
    });

    Ok(())
}

fn restore_sst_list(manifest_path: &Path) -> KeplerResult<Version> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(manifest_path)?;
    let mut reader = BufReader::with_capacity(BUF_SIZE, file);
    let mut sst_list: BTreeMap<u64, SSTInfo> = BTreeMap::new();
    let mut max_seqno = 0;
    let mut max_sstno = 0;

    loop {
        let mut form = [0u8; 25];

        match reader.read_exact(&mut form) {
            Ok(_) => {
                let t = form[0];
                let sstno = u64::from_le_bytes(form[1..9].try_into().unwrap());
                let maxno = u64::from_le_bytes(form[9..17].try_into().unwrap());

                match t {
                    0 => {
                        max_sstno = max_sstno.max(sstno);
                        max_seqno = max_seqno.max(maxno);
                        sst_list.insert(sstno, SSTInfo::new(sstno));
                    }
                    1 => {
                        sst_list.remove(&sstno);
                    }
                    _ => return Err(KeplerErr::ManifestCorrupted(sstno as usize)),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(KeplerErr::Io(e)),
        }
    }
    Ok(Version::new(sst_list, max_seqno + 1, max_sstno + 1))
}
