extern crate optra;
extern crate rdiff;
extern crate crdt_fileset;
extern crate notify;
extern crate eventual;
extern crate wamp;
extern crate base64;
extern crate getopts;
extern crate byteorder;
#[macro_use]
extern crate log;
extern crate env_logger;

mod file_sync;
mod history_store;
mod communication;

use std::thread::{park, sleep};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::env;
use std::path::Path;
use std::sync::mpsc::channel;
use getopts::Options;
use wamp::router::Router;

fn print_usage(opts: Options) {
    let brief = format!("Usage: kitchen <directory> <site_id> [options]");
    print!("{}", opts.usage(&brief));
}

fn main() {
    env_logger::init().unwrap();
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();
    opts.optopt("u", "url", "The url of the router", "URL");
    opts.optflag("h", "help", "print this help menu");
    let matches = opts.parse(&args[1..]).unwrap();
    if matches.opt_present("h") {
        print_usage(opts);
        return;
    }
    let url = match matches.opt_str("u") {
        Some(url) => url,
        None => "ws://127.0.0.1:8090".to_string()
    };
    if matches.free.len() != 2 {
        print_usage(opts);
        return
    }
    let path = Path::new(&matches.free[0]);
    if !path.is_dir() {
        println!("Please specify an existing directory");
        return
    }
    let site_id = match str::parse::<u32>(&matches.free[1]) {
        Ok(site_id) => site_id,
        Err(_) => {
            println!("Site ID must be a 32 bit unsigned integer, got: {}", matches.free[1]);
            return
        }
    };
    let mut router = Router::new();
    router.add_realm("kitchen_realm");
    let child = router.listen("127.0.0.1:8090");
    sleep(Duration::from_millis(1000));
    trace!("Loading data");
    let data = Arc::new(Mutex::new(file_sync::SyncData::new(&path, site_id).unwrap()));
    trace!("Starting file updater");
    let updater = file_sync::file_updater::FileUpdater::new(data.clone()).unwrap();
    let mut path = path.to_path_buf();
    path.push(".kitchen");
    trace!("Loading fileset");
    let mut fileset = Arc::new(Mutex::new(crdt_fileset::FileSet::new(updater, site_id, path.as_path()).unwrap()));
    trace!("Setting up comms");
    let mut comms = communication::Client::new_cannonical(&url, data.clone(), fileset.clone()).unwrap();
    trace!("Launching file watcher");
    let watcher = file_sync::FileWatcher::new(data, comms, fileset).unwrap();
    println!("Server running");
    park();
}
