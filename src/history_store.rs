use optra::{Engine, TimeStamper};
use rdiff::BlockHashes;
use std::path::{Path, PathBuf};
use std::fs::{File, DirBuilder, OpenOptions, read_dir, remove_file, rename, remove_dir_all};
use std::io::{self, Seek, SeekFrom};
use std::fmt::Debug;
use eventual::{Future, Async};
use communication;


pub struct Store {
    root: PathBuf,
    builder: DirBuilder
}

impl Store {

    pub fn new(dir: &Path) -> Result<Store, io::Error> {
        let mut root = dir.to_path_buf();
        root.push(".kitchen");
        root.push("abst");
        let mut builder = DirBuilder::new();
        builder.recursive(true);
        if !root.exists() {
            try!(builder.create(root.as_path()));
        }
        Ok(Store{
            root: root,
            builder: builder
        })

    }
    pub fn store<P: AsRef<Path> + Debug + Clone>(&self, path: P, engine: &Engine, hashes: &BlockHashes) -> io::Result<()> {
        let mut store_path = self.root.join(path.as_ref());
        let mut dir = store_path.clone();
        dir.pop();
        try!(self.builder.create(dir));
        trace!("Saving data on {:?} in {:?}", path, store_path);
        let mut store_file = try!(File::create(store_path));
        try!(engine.compress_to(&mut store_file));
        try!(hashes.compress_to(&mut store_file));
        Ok(())
    }

    pub fn load<P: AsRef<Path> + Debug + Clone>(&self, path: P, site_id: u32, default_blocksize: usize) -> io::Result<(Engine, BlockHashes)> {
        let mut store_path = self.root.join(path.as_ref());
        trace!("Loading data on {:?} in {:?}", path, store_path);
        match File::open(store_path) {
            Ok(mut store_file) => {
                trace!("Loading Engine");
                let engine = try!(Engine::expand_from(&mut store_file, site_id));
                trace!("Engine loaded. Loading hashes");
                let hashes = try!(BlockHashes::expand_from(&mut store_file));
                trace!("Hashes loaded");
                Ok((engine, hashes))
            },
            Err(e) => {
                trace!("Failed to load data: {:?}", e);
                trace!("Loading fresh copies of the engine and hashes");
                Ok((Engine::new(site_id), BlockHashes::empty(default_blocksize)))
            }
        }
    }

    pub fn create_entry<P: AsRef<Path> + Debug + Clone>(&self, path: P, site_id: u32, default_blocksize: usize) -> io::Result<()> {
        self.store(path, &Engine::new(site_id), &BlockHashes::empty(default_blocksize))
    }

    pub fn remove_entry<P: AsRef<Path> + Debug + Clone>(&self, path: P) -> io::Result<()> {
        let mut store_path = self.root.join(path.as_ref());
        remove_file(store_path.as_path())
    }

    pub fn remove_entry_folder<P: AsRef<Path> + Debug + Clone>(&self, path: P) -> io::Result<()> {
        let mut store_path = self.root.join(path.as_ref());
        remove_dir_all(store_path.as_path())
    }

    pub fn rename_entry<P: AsRef<Path> + Debug + Clone>(&self, old_path: P, new_path: P) -> io::Result<()> {
        let mut old_store_path = self.root.join(old_path.as_ref());
        let mut new_store_path = self.root.join(new_path.as_ref());
        rename(old_store_path.as_path(), new_store_path.as_path())
    }

    pub fn save_stamper(&self, stamper: &TimeStamper) -> io::Result<()> {
        let mut store_path = self.root.clone();
        store_path.pop();
        store_path.push("timestamps");
        let mut file = try!(File::create(store_path));
        stamper.compress_to(&mut file)
    }

    pub fn load_stamper(&self) -> io::Result<TimeStamper> {
        let mut store_path = self.root.clone();
        store_path.pop();
        store_path.push("timestamps");
        match File::open(store_path.as_path()) {
            Ok(mut file) => {
                trace!("Loading time stamper from {:?}", store_path);
                TimeStamper::expand_from(&mut file)
            } Err(_) => {
                trace!("Could not load time stamper, creating new one");
                Ok(TimeStamper::new())
            }
        }
    }

    // pub fn initialize(&self, sender: &mut S, site_id: u32) -> io::Result<()> {
    //     self.visit_dir(self.root.parent().unwrap(), self.root.parent().unwrap(), sender, site_id)
    // }

    // fn visit_dir(&self, base_path: &Path, path: &Path, sender: &mut S, site_id: u32) -> io::Result<()> {
    //     trace!("Visiting {:?}", path);
    //     if let Some(file_name) = path.file_name() {
    //         if file_name == ".kitchen" {
    //             return Ok(())
    //         }
    //     }
    //     for entry in try!(read_dir(path)) {
    //         let entry = try!(entry);
    //         let path = entry.path();
    //         if path.is_dir() {
    //             try!(self.visit_dir(base_path, path.as_path(), sender, site_id))
    //         } else {
    //             try!(self.init_file(path.clone().as_path(), path.as_path().strip_prefix(base_path).unwrap(), sender, site_id))
    //         }
    //     }
    //     Ok(())
    // }
    //
    // fn init_file<S: 'static + communication::TransactionSender>(&self, full_path: &Path, path: &Path, sender: &mut S, site_id: u32) -> io::Result<()> {
    //     trace!("Initializing file {:?}", path);
    //     let (mut engine, mut hashes) = try!(self.load(path, site_id, 8));
    //     trace!("Loaded engine and hashes");
    //     let mut file = OpenOptions::new()
    //                    .read(true)
    //                    .write(true)
    //                    .open(full_path)
    //                    .unwrap();
    //     let local_diff = try!(hashes.diff_and_update(file.try_clone().unwrap()));
    //
    //     trace!("Getting transactions");
    //     let mut historical = sender.get_transactions_since(path, engine.get_last_state()).await().unwrap();
    //     let local_transaction = engine.process_diffs(local_diff);
    //     trace!("Notifying server of changes: {:?}", local_transaction);
    //     sender.send_transaction(path, local_transaction);
    //     trace!("Integrating transactions");
    //     engine.integrate_remote(&mut historical);
    //     file.seek(SeekFrom::Start(0));
    //     trace!("Applying transaction {:?}", historical);
    //     historical.apply(&mut file.try_clone().unwrap());
    //     file.seek(SeekFrom::Start(0));
    //     trace!("Calculating hashes");
    //     hashes.diff_and_update(file);
    //     self.store(path, &engine, &hashes);
    //     trace!("Completed file {:?}", path);
    //     Ok(())
    // }

}
