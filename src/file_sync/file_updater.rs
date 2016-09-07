use crdt_fileset;
use optra::TransactionSequence;
use super::SyncData;
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use std::io;
use std::fs;
use std::fmt;


pub struct FileUpdater {
    data: Arc<Mutex<SyncData>>,
    path: PathBuf,
    builder: fs::DirBuilder
}

impl FileUpdater {

    pub fn new(data: Arc<Mutex<SyncData>>) -> io::Result<FileUpdater> {
        let path = data.lock().unwrap().base_path.clone();
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        Ok(FileUpdater {
            data: data,
            path: path,
            builder: builder
        })
    }

}

impl fmt::Debug for FileUpdater {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "File Updater for {:?}", self.path)
    }
}

impl crdt_fileset::FileUpdater for FileUpdater
    {
        type FileTransaction = TransactionSequence;
        fn create_file<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
            let path = path.as_ref();
            trace!("Creating file at {:?}", path);
            let data = self.data.lock().unwrap();
            data.store.create_entry(path, data.site_id, data.block_size);
            let path = data.base_path.join(path);
            let mut dir = path.clone();
            dir.pop();
            try!(self.builder.create(dir));
            fs::File::create(path).map(|_| ())
        }
        fn remove_file<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
            let path = path.as_ref();
            let data = self.data.lock().unwrap();
            data.store.remove_entry(path);
            fs::remove_file(data.base_path.join(path))
        }
        fn update_file<P: AsRef<Path>>(&mut self, path: P, timestamp_lookup: &BTreeMap<u32, (u32, u32)>, transaction: &mut TransactionSequence) -> io::Result<()> {
            let path = path.as_ref();
            let mut data = self.data.lock().unwrap();
            trace!("Applying transaction {:?}", transaction);
            for (_, &(site_id, timestamp)) in timestamp_lookup.iter() {
                data.stamper.stamp_remote(site_id, timestamp);
            }
            data.apply_transaction(transaction, timestamp_lookup, path);
            Ok(())
        }
        fn move_file<P: AsRef<Path>>(&mut self, old_path: P, new_path: P) -> io::Result<()> {
            let old_path = old_path.as_ref();
            let new_path = new_path.as_ref();
            let data = self.data.lock().unwrap();
            data.store.rename_entry(old_path, new_path);
            let path = data.base_path.join(new_path);
            let mut dir = path.clone();
            dir.pop();
            try!(self.builder.create(dir));
            fs::rename(data.base_path.join(old_path), path)
        }
        fn get_local_changes<P: AsRef<Path>>(&mut self, path: P) -> io::Result<(TransactionSequence, BTreeMap<u32, (u32, u32)>)> {
            let path = path.as_ref();
            let mut data = self.data.lock().unwrap();
            let (mut engine, mut hashes) = try!(data.store.load(path, data.site_id, data.block_size));

            let file = try!(fs::File::open(data.base_path.join(path)));
            let diff = try!(hashes.diff_and_update(file));
            let (transaction, lookup) = engine.process_diffs(diff, &mut data.stamper);
            data.store.store(path, &engine, &hashes);
            Ok((transaction, lookup))
        }
        fn get_changes_since<P: AsRef<Path>>(&self, path: P, last_timestamp: Option<(u32, u32)>) -> TransactionSequence {
            let path = path.as_ref();
            let mut data = self.data.lock().unwrap();
            // TODO better error handling
            let (engine, _) = data.store.load(path, data.site_id, data.block_size).unwrap();
            engine.get_operations_since(last_timestamp, &mut data.stamper).unwrap()
        }
        fn get_base_path(&self) -> &Path {
            self.path.as_path()
        }

}
