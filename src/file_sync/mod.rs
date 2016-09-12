pub mod file_updater;

use self::file_updater::FileUpdater;
use optra::{Engine, TransactionSequence, TimeStamper};
use crdt_fileset::FileSet;
use rdiff::BlockHashes;
use notify::{self, RecommendedWatcher, Watcher, RecursiveMode};
use notify::op;
use std::path::{PathBuf, Path};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::{self, JoinHandle, current, park};
use std::io::{self, Seek, SeekFrom};
use std::fs::{OpenOptions, File};
use std::collections::{HashMap, VecDeque, LinkedList, BTreeMap};
use std::mem;

use history_store::Store;
use communication::Client;


// Step 1: Create data object
// Step 2: Create FileUpdater
// Step 3: Create CRDT
// Step 4: Create serializer/deserializer
// Step 5: Create file watcher
// Step 6: Initalize the data from the network

// TODO: Update file watcher thread with fileset activities
// TODO: Update the store with fileset stuff
// TODO: Profit?

pub struct FileWatcher {
    watcher_thread: Arc<JoinHandle<()>>,
    watcher: RecommendedWatcher
}

pub struct SyncData {
    delayed_transactions: LinkedList<(TransactionSequence, BTreeMap<u32, (u32, u32)>, PathBuf)>,
    pub site_id: u32,
    pub block_size: usize,
    pub store: Store,
    pub base_path: PathBuf,
    pub stamper: TimeStamper
}



impl FileWatcher {

    pub fn new(data: Arc<Mutex<SyncData>>, mut sender: Client, fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> io::Result<FileWatcher> {
        let path = data.lock().unwrap().base_path.clone();
        let (watcher_thread, watcher) = create_file_watcher_thread(data.clone(), path.as_path(), sender, fileset).unwrap();
        Ok(FileWatcher {
            watcher: watcher,
            watcher_thread: Arc::new(watcher_thread),
        })
    }

}

impl SyncData {

    pub fn new<P: AsRef<Path> + Clone>(path: P, site_id: u32) -> io::Result<SyncData> {
        let path = path.as_ref();
        trace!("Creating Store");
        let store = try!(Store::new(path));
        trace!("Creating Time Stamper");
        let stamper = try!(store.load_stamper());
        Ok(SyncData {
            site_id: site_id,
            block_size: 8,
            delayed_transactions: LinkedList::new(),
            store: store,
            base_path: path.to_path_buf(),
            stamper: stamper
        })
    }



    fn apply_transaction(&mut self, transaction: &mut TransactionSequence, timestamp_lookup: &BTreeMap<u32, (u32, u32)>, path: &Path){
        let actual_path = self.base_path.clone().join(Path::new(&path).to_path_buf());
        let (mut engine, mut hashes) = self.store.load(&path, self.site_id, self.block_size).unwrap();
        trace!("Opening file {:?}", actual_path);
        let mut file = OpenOptions::new()
                       .read(true)
                       .write(true)
                       .open(actual_path.as_path());
        let mut file = match file {
            Ok(file) => file,
            Err(e) =>  {
                trace!("Delaying transaction due to error: {:?}", e);
                self.delayed_transactions.push_back((transaction.clone(), timestamp_lookup.clone(), path.to_path_buf()));
                return
            }
        };
        trace!("File opened");
        match hashes.verify_unchanged(&mut file) {
           Ok(true) => {},
           _ => {
               trace!("File does not match stored hashes, delaying");
               self.delayed_transactions.push_back((transaction.clone(), timestamp_lookup.clone(), path.to_path_buf()));
               return
           }
        }
        trace!("Integrating transaction");
        if let Ok(_) = engine.integrate_remote(transaction,timestamp_lookup, &mut self.stamper){
            file.seek(SeekFrom::Start(0)).unwrap();
            trace!("Applying integrated transaction: {:?}", transaction);
            transaction.apply(&mut file);
            file.seek(SeekFrom::Start(0)).unwrap();
            hashes.diff_and_update(&mut file).unwrap();
            self.store.store(&path, &engine, &hashes).unwrap()
        }
    }

    pub fn process_delayed(&mut self) {
        if !self.delayed_transactions.is_empty() {
            for (mut transaction, timestamp_lookup, path) in mem::replace(&mut self.delayed_transactions, LinkedList::new()) {
                self.apply_transaction(&mut transaction, &timestamp_lookup, path.as_path());
            }
        }
    }
}


//
// fn create_sync_thread( data: Arc<Mutex<SyncData>>, transaction_reciever: Receiver<TransactionMessage>) -> JoinHandle<()> {
//     thread::spawn(move || {
//         while let Ok(message) = transaction_reciever.recv() {
//             trace!("Attempting to lock data");
//             let mut data = data.lock().unwrap();
//             trace!("Data locked");
//             match message {
//                 TransactionMessage::UpdateTransaction(mut transaction, path) => {
//                     trace!("Applying transaction {:?}", transaction);
//                     data.apply_transaction(transaction, path);
//                 },
//                 _ => {}
//             }
//
//         }
//     })
// }

fn create_file_watcher_thread(data: Arc<Mutex<SyncData>>, path: &Path, mut sender: Client, fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> Result<(JoinHandle<()>, RecommendedWatcher), notify::Error> {
    let (tx, rx) = channel();
    let mut watcher:RecommendedWatcher = try!(Watcher::new(tx));
    trace!("Watching path {:?}", path);
    try!(watcher.watch(path, RecursiveMode::Recursive));
    // TODO add checks for if the file was already added/removed
    Ok((thread::spawn(move ||{
        let mut rename_cookies:HashMap<_, PathBuf> = HashMap::new();
        loop {
            let event = rx.recv().unwrap();
            let operation = event.op.unwrap();
            let mut fileset = fileset.lock().unwrap();
            let mut actual_path = event.path.unwrap();
            let path_string = actual_path.to_str().unwrap().to_string();
            trace!("path string: {}", path_string);
            actual_path = Path::new(path_string.trim_left_matches("\\\\?\\")).to_path_buf();

            let mut data = data.lock().unwrap();

            trace!("Watcher data locked");
            let path = actual_path.strip_prefix(data.base_path.as_path()).unwrap().to_path_buf();
            if path.starts_with(".kitchen") {
                continue;
            }
            if operation.contains(op::CREATE) {
                if actual_path.is_dir() {
                    continue;
                }
                trace!("Recieved a create notification for {:?}", actual_path);
                trace!("Watcher locking data");
                if fileset.has_path(&path) {
                    continue;
                }
                let operation = fileset.process_create(path.as_path());
                sender.send_transaction(operation);
            }
            if operation.contains(op::RENAME) {
                if actual_path.is_dir() {
                    continue;
                }
                trace!("Recieved a rename notification for {:?}", actual_path);
                match rename_cookies.remove(&event.cookie) {
                    Some(old_path) => {
                        if fileset.has_path(&path) {
                            continue;
                        }
                        let operation = fileset.process_file_move(old_path.as_path(), path.as_path());
                        data.store.rename_entry(old_path, path);
                        sender.send_transaction(operation);
                    }, None => {
                        rename_cookies.insert(event.cookie, path);
                    }
                }
            }
            else if operation.contains(op::REMOVE) {
                trace!("Recieved remove notification for {:?}", actual_path);

                if !fileset.has_path(&path) {
                    let operations = fileset.process_remove_folder(path.as_path());
                    trace!("Got remove operations: {:?}", operations);
                    if operations.len() > 0 {
                        data.store.remove_entry_folder(path.as_path());
                        for operation in operations {
                            sender.send_transaction(operation);
                        }
                    }
                } else {
                    let operation = fileset.process_remove(path.as_path());
                    data.store.remove_entry(path.as_path());
                    sender.send_transaction(operation);
                }

            }
            else if operation.contains(op::WRITE) || operation.contains(op::CREATE) {
                trace!("Recieved a write notification for {:?}", actual_path);
                if actual_path.is_dir() {
                    continue;
                }
                {


                    trace!("Notified that {:?} was updated", actual_path);
                    let mut file = File::open(actual_path.as_path()).unwrap();

                    let (mut engine, mut hashes) = data.store.load(&path, data.site_id, data.block_size).unwrap();
                    let diffs = hashes.diff_and_update(&mut file).unwrap();
                    if diffs.is_empty() {
                        trace!("No changes made to file, skipping");
                        continue;
                    }
                    let (transaction, lookups) = engine.process_diffs(diffs, &mut data.stamper);
                    data.store.store(&path, &engine, &hashes).unwrap();
                    data.store.save_stamper(&data.stamper).unwrap();
                    trace!("Sending transaction {:?} for {:?}", transaction, path);
                    let operation = fileset.process_update(&path, transaction, lookups);
                    sender.send_transaction(operation);

                }
                data.process_delayed();
            }
        }
    }), watcher))
}

// Operations come in front network
// Network acquires lock on remote queue
// Operations are added to remote queue
// Network releases lock on remote queue
// Sync thread is notified

// Sync thread is notified
// Sync thread acquires write lock on File (if fail, then park)
// Sync thread acquires lock on data (If none then park)
// Sync thread acquires lock on Engine
// Sync thread processes Operations
// Sync thread releases lock on Engine
// Sync thread releases lock on data
// Sync thread releases file lock

// File watcher is notified of a file change
// File watcher acquires read lock on file
// File watcher aquires lock on data
// File watcher acquires lock on Engine
// File watcher finds diffs
// File watcher uses the engine to process the diffs
// File watcher releases all locks
// File watcher sends transaction
// File watcher notifies sync thread
