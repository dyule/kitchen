use optra::{TransactionSequence, DeleteOperation, InsertOperation};
use rdiff::{BlockHashes, Diff};
use crdt_fileset::{FileSet, FileHistory, FileSetOperation, CreateOperation, RemoveOperation, UpdateOperation, UpdateMetadata, MetadataTransaction, State};
use std::collections::{LinkedList, HashMap, BTreeMap};
use std::path::{PathBuf, Path};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::io::{self, Cursor, Read, Write};
use std::fs::{File};
use eventual::{Future, Async};
use wamp::{Client as WampClient, WampResult, URI, Connection, List, Dict, ArgDict, Value, CallError, CallResult};
use base64;
use file_sync::SyncData;
use file_sync::file_updater::FileUpdater;
use history_store::Store;
use byteorder::{NetworkEndian, ByteOrder};

pub struct Client {
    client: WampClient
}


fn create_update_callback(mut fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> Box<FnMut(List, Dict)> {
    let update_callback = move |_args: List, kwargs: Dict| {
        let mut fileset = fileset.lock().unwrap();
        // TODO better error handling
        match kwargs.get_string("type").unwrap().unwrap() {
            "create" => process_create(&kwargs, &mut fileset),
            "remove" => process_remove(&kwargs, &mut fileset),
            "update" => process_update(&kwargs, &mut fileset),
            "update_metadata" => process_update_metadata(&kwargs, &mut fileset),
            _ => ()
        };
    };
    Box::new(update_callback)
}

fn process_create(kwargs: &Dict, fileset: &mut FileSet<FileUpdater>) {

    let site_id = kwargs.get_int("site_id").unwrap().unwrap() as u32;
    let id = kwargs.get_int("id").unwrap().unwrap() as u32;
    let site_state_id = kwargs.get_int("state_site_id").unwrap().unwrap() as u32;
    let timestamp = kwargs.get_int("timestamp").unwrap().unwrap() as u32;
    let filename = if let &Value::List(ref filename) = kwargs.get("filename").unwrap() {
        filename.into_iter().map(|component|{
            if let &Value::String(ref component) = component {
                component.clone()
            } else {
                String::new()
            }
        }).collect()
    } else {
        return
    };
    fileset.integrate_remote(FileSetOperation::Create(CreateOperation{
        state: State {
            site_id: site_state_id,
            time_stamp: timestamp
        },
        id: (site_id, id),
        filename: filename
    }));
}

fn process_remove(kwargs: &Dict, fileset: &mut FileSet<FileUpdater>) {
    let site_id = kwargs.get_int("site_id").unwrap().unwrap() as u32;
    let id = kwargs.get_int("id").unwrap().unwrap() as u32;
    fileset.integrate_remote(FileSetOperation::Remove(RemoveOperation{
        id: (site_id, id)
    }));
}

fn process_update(kwargs: &Dict, fileset: &mut FileSet<FileUpdater>) {
    let lookup = if let &Value::List(ref list) = kwargs.get("lookup").unwrap() {
        list_to_lookups(list)
    } else {
        panic!("Epxected a list of lookups");
    };
    let transaction_str = kwargs.get_string("transaction").unwrap().unwrap();
    let transaction_bytes = base64::decode(transaction_str).unwrap();
    let transaction = TransactionSequence::expand_from(&mut Cursor::new(transaction_bytes), Some(&lookup)).unwrap();
    trace!("Read transaction: {:?}", transaction);
    let site_id = kwargs.get_int("site_id").unwrap().unwrap() as u32;
    let id = kwargs.get_int("id").unwrap().unwrap() as u32;
    fileset.integrate_remote(FileSetOperation::Update(UpdateOperation{
        id: (site_id, id),
        data: transaction
    }, lookup));
}

fn process_update_metadata(kwargs: &Dict, fileset: &mut FileSet<FileUpdater>) {
    let site_id = kwargs.get_int("site_id").unwrap().unwrap() as u32;
    let id = kwargs.get_int("id").unwrap().unwrap() as u32;
    let site_state_id = kwargs.get_int("state_site_id").unwrap().unwrap() as u32;
    let timestamp = kwargs.get_int("timestamp").unwrap().unwrap() as u32;
    let key = kwargs.get_string("key").unwrap().unwrap();
    let transaction = if key == "filename" {
        if let &Value::List(ref value) = kwargs.get("value").unwrap() {
            MetadataTransaction::Filename(value.into_iter().map(|component|{
                if let &Value::String(ref component) = component {
                    component.clone()
                } else {
                    String::new()
                }
            }).collect())
        } else {
            return
        }
    } else {
        let value = kwargs.get_string("value").unwrap().unwrap();
        MetadataTransaction::Custom(key.to_string(), value.to_string())
    };
    fileset.integrate_remote(FileSetOperation::UpdateMetadata(UpdateMetadata{
        state: State {
            site_id: site_state_id,
            time_stamp: timestamp
        },
        id: (site_id, id),
        data: transaction
    }));
}

fn send_transaction(client: &mut WampClient, operation: FileSetOperation<FileUpdater>) {
    match operation {
        FileSetOperation::Create(operation) => send_create_operation(client, operation),
        FileSetOperation::Remove(operation) => send_remove_operation(client, operation),
        FileSetOperation::Update(operation, lookup) => send_update_operation(client, operation, lookup),
        FileSetOperation::UpdateMetadata(operation) => send_update_metadata_operation(client, operation),
    }

}


fn send_create_operation(client: &mut WampClient, operation: CreateOperation) {
    trace!("Sending operation {:?}", operation);
    let mut kwargs = HashMap::new();
    kwargs.insert("site_id".to_string(), Value::Integer(operation.id.0 as i64));
    kwargs.insert("id".to_string(), Value::Integer(operation.id.1 as i64));
    kwargs.insert("filename".to_string(), Value::List(operation.filename.into_iter().map(|component| Value::String(component)).collect()));
    kwargs.insert("state_site_id".to_string(), Value::Integer(operation.state.site_id as i64));
    kwargs.insert("timestamp".to_string(), Value::Integer(operation.state.time_stamp as i64));
    kwargs.insert("type".to_string(), Value::String("create".to_string()));
    client.publish(URI::new("ca.kitchen.file_updates"), None, Some(kwargs)).unwrap();
}

fn send_remove_operation(client: &mut WampClient, operation: RemoveOperation) {
    let mut kwargs = HashMap::new();
    kwargs.insert("site_id".to_string(), Value::Integer(operation.id.0 as i64));
    kwargs.insert("id".to_string(), Value::Integer(operation.id.1 as i64));
    kwargs.insert("type".to_string(), Value::String("remove".to_string()));
    client.publish(URI::new("ca.kitchen.file_updates"), None, Some(kwargs)).unwrap();
}

fn send_update_metadata_operation(client: &mut WampClient, operation: UpdateMetadata) {
    let mut kwargs = HashMap::new();
    kwargs.insert("site_id".to_string(), Value::Integer(operation.id.0 as i64));
    kwargs.insert("id".to_string(), Value::Integer(operation.id.1 as i64));
    kwargs.insert("state_site_id".to_string(), Value::Integer(operation.state.site_id as i64));
    kwargs.insert("timestamp".to_string(), Value::Integer(operation.state.time_stamp as i64));
    match operation.data {
        MetadataTransaction::Filename(filename) => {
            kwargs.insert("key".to_string(), Value::String("filename".to_string()));
            kwargs.insert("value".to_string(), Value::List(filename.into_iter().map(|component| Value::String(component)).collect()));
        }, MetadataTransaction::Custom(key, value) => {
            kwargs.insert("key".to_string(), Value::String(key));
            kwargs.insert("value".to_string(), Value::String(value));
        }
    }
    kwargs.insert("type".to_string(), Value::String("update_metadata".to_string()));
    client.publish(URI::new("ca.kitchen.file_updates"), None, Some(kwargs)).unwrap();
}

fn send_update_operation(client: &mut WampClient, operation: UpdateOperation<FileUpdater>, lookup: BTreeMap<u32, (u32, u32)>) {
    let mut transaction_bytes = Cursor::new(Vec::new());
    operation.data.compress_to(&mut transaction_bytes).unwrap();
    let transaction_str = base64::encode(transaction_bytes.into_inner().as_slice());
    let mut kwargs = HashMap::new();
    kwargs.insert("transaction".to_string(), Value::String(transaction_str));
    kwargs.insert("site_id".to_string(), Value::Integer(operation.id.0 as i64));
    kwargs.insert("id".to_string(), Value::Integer(operation.id.1 as i64));
    kwargs.insert("lookup".to_string(), Value::List(lookup_to_list(&lookup)));
    kwargs.insert("type".to_string(), Value::String("update".to_string()));
    client.publish(URI::new("ca.kitchen.file_updates"), None, Some(kwargs)).unwrap();
}

fn get_changes(fileset: &mut FileSet<FileUpdater>, state: Option<(u32, u32)>) -> CallResult<HashMap<(u32, u32), FileHistory<FileUpdater>>> {
    let file_changes = fileset.get_changes_since(state);

    Ok(file_changes)
}

fn compress_file_history_entry<W: Write>(writer: &mut W, id: (u32, u32), history: FileHistory<FileUpdater>) -> io::Result<()> {
    let mut int_buf = [0;4];
    NetworkEndian::write_u32(&mut int_buf, id.0);
    try!(writer.write(&int_buf));
    NetworkEndian::write_u32(&mut int_buf, id.1);
    try!(writer.write(&int_buf));
    NetworkEndian::write_u32(&mut int_buf, history.filename.0);
    try!(writer.write(&int_buf));
    NetworkEndian::write_u32(&mut int_buf, history.filename.1.len() as u32);
    try!(writer.write(&int_buf));
    for filename in history.filename.1.iter() {
        let bytes = filename.as_bytes();
        NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
        try!(writer.write(&int_buf));
        try!(writer.write(bytes));
    }
    NetworkEndian::write_u32(&mut int_buf, history.attributes.len() as u32);
    try!(writer.write(&int_buf));
    for (key, &(time_stamp, ref value)) in history.attributes.iter() {
        let bytes = key.as_bytes();
        NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
        try!(writer.write(&int_buf));
        try!(writer.write(bytes));
        NetworkEndian::write_u32(&mut int_buf, time_stamp);
        try!(writer.write(&int_buf));
        let bytes = value.as_bytes();
        NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
        try!(writer.write(&int_buf));
        try!(writer.write(bytes));
    }
    NetworkEndian::write_u32(&mut int_buf, history.operation_history.inserts.len() as u32);
    try!(writer.write(&mut int_buf));
    for insert in history.operation_history.inserts {
        try!(insert.compress_to(writer, false));
    }
    NetworkEndian::write_u32(&mut int_buf, history.operation_history.deletes.len() as u32);
    try!(writer.write(&mut int_buf));
    for delete in history.operation_history.deletes {
        try!(delete.compress_to(writer));
    }
    Ok(())
    //history.operation_history.compress_to(writer)
}

fn expand_file_history_entry<R: Read>(reader: &mut R, timestamp_lookup: &BTreeMap<u32, (u32, u32)>, last_timestamp: Option<(u32, u32)>) -> io::Result<(u32, u32, FileHistory<FileUpdater>)> {
    let mut int_buf = [0;4];

    try!(reader.read_exact(&mut int_buf));
    let file_site_id = NetworkEndian::read_u32(&int_buf);
    try!(reader.read_exact(&mut int_buf));
    let id = NetworkEndian::read_u32(&int_buf);
    try!(reader.read_exact(&mut int_buf));
    let filename_timestamp = NetworkEndian::read_u32(&int_buf);
    try!(reader.read_exact(&mut int_buf));
    let filename_component_count = NetworkEndian::read_u32(&int_buf) as usize;
    let mut filename = Vec::with_capacity(filename_component_count);
    for _ in 0..filename_component_count {
        filename.push(try!(read_str(reader, &mut int_buf)));
    }
    try!(reader.read_exact(&mut int_buf));
    let attribute_count = NetworkEndian::read_u32(&int_buf) as usize;
    let mut attributes = HashMap::with_capacity(attribute_count);
    for _ in 0..attribute_count {
        let key = try!(read_str(reader, &mut int_buf));
        try!(reader.read_exact(&mut int_buf));
        let attribute_timestamp = NetworkEndian::read_u32(&int_buf);
        let value = try!(read_str(reader, &mut int_buf));
        attributes.insert(key, (attribute_timestamp, value));
    }
    try!(reader.read_exact(&mut int_buf));
    let insert_len = NetworkEndian::read_u32(&int_buf);
    trace!("Insert length was: {}", insert_len);
    let mut inserts = LinkedList::new();
    for _ in 0..insert_len {
        inserts.push_back(try!(InsertOperation::expand_from(reader, Some(timestamp_lookup))))
    }
    trace!("Read inserts");
    trace!("Reading delete length");
    try!(reader.read_exact(&mut int_buf));
    let delete_len = NetworkEndian::read_u32(&int_buf);
    trace!("Delete length was: {}", delete_len);
    let mut deletes = LinkedList::new();
    for _ in 0..delete_len {
        deletes.push_back(try!(DeleteOperation::expand_from(reader)));
    }
    let operations = TransactionSequence::new(last_timestamp, inserts, deletes);
    Ok((file_site_id, id, FileHistory::new(filename_timestamp, filename, attributes, operations)))
}

fn create_get_id_callback() -> Box<FnMut(List, Dict) -> CallResult<(Option<List>, Option<Dict>)>> {
    let last_id = Arc::new(Mutex::new(1));
    Box::new(move |args, kwargs| {
        let mut last_id = last_id.lock().unwrap();
        *last_id += 1;
        Ok((Some(vec![Value::Integer(*last_id)]), None))
    })
}

fn create_check_timestamp_callback(data: Arc<Mutex<SyncData>>) -> Box<FnMut(List, Dict) -> CallResult<(Option<List>, Option<Dict>)>> {
    Box::new(move |args, kwargs| {
        let site_id = try!(kwargs.get_int("site_id")).unwrap() as u32;
        let timestamp = try!(kwargs.get_int("timestamp")).unwrap() as u32;
        let data = data.lock().unwrap();
        if data.stamper.get_local_timestamp_for(site_id, timestamp).is_some() {
            Ok((Some(vec![Value::Integer(1)]), None))
        } else {
            Ok((Some(vec![Value::Integer(0)]), None))
        }
    })
        
}


fn create_changes_callback(data: Arc<Mutex<SyncData>>, fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> Box<FnMut(List, Dict) -> CallResult<(Option<List>, Option<Dict>)>> {
    Box::new(move |args, kwargs| {
        trace!("Calculating changes");
        let state = if kwargs.contains_key("site_id") {
            let site_id = try!(kwargs.get_int("site_id")).unwrap() as u32;
            let timestamp = try!(kwargs.get_int("timestamp")).unwrap() as u32;
            Some((site_id, timestamp))
        } else {
            None
        };
        trace!("Attempting to lock fileset");
        let mut fileset = fileset.lock().unwrap();
        trace!("Fileset locked");
        let changes = try!(get_changes(&mut fileset,  state));

        trace!("Changes found, compressing for transmission");

        let mut change_bytes = Cursor::new(Vec::new());
        // TODO better error handling
        let lookup = data.lock().unwrap().stamper.get_timestamps_since(state).unwrap();
        let mut int_buf = [0;4];
        NetworkEndian::write_u32(&mut int_buf, lookup.len() as u32);
        change_bytes.write(&mut int_buf).unwrap();
        for (&local, &(site_id, remote)) in lookup.iter() {
            NetworkEndian::write_u32(&mut int_buf, local);
            change_bytes.write(&mut int_buf).unwrap();
            NetworkEndian::write_u32(&mut int_buf, site_id);
            change_bytes.write(&mut int_buf).unwrap();
            NetworkEndian::write_u32(&mut int_buf, remote);
            change_bytes.write(&mut int_buf).unwrap();
        }
        NetworkEndian::write_u32(&mut int_buf, changes.len() as u32);
        change_bytes.write(&int_buf).unwrap();
        for (id, history) in changes.into_iter() {
            // TODO better error handling
            compress_file_history_entry(&mut change_bytes, id, history).unwrap();
        }
        let change_str = base64::encode(change_bytes.into_inner().as_slice());
        let mut ret_args = HashMap::new();
        ret_args.insert("changes".to_string(), Value::String(change_str));
        trace!("Transmitting changes back");
        Ok((None, Some(ret_args)))

    })
}

fn create_history_callback(data: Arc<Mutex<SyncData>>, fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> Box<FnMut(List, Dict) -> CallResult<(Option<List>, Option<Dict>)>> {
    Box::new(move |args, kwargs| {
        trace!("Getting file history");
        let site_id = try!(kwargs.get_int("site_id")).unwrap() as u32;
        let id = try!(kwargs.get_int("id")).unwrap() as u32;

        trace!("Attempting to lock fileset");
        let mut fileset = fileset.lock().unwrap();
        trace!("Fileset locked");
        // TODO better error handling
        let history = fileset.get_file_history_for((site_id, id)).unwrap();
        trace!("history found, compressing for transmission");
        let lookup = data.lock().unwrap().stamper.get_timestamps_for(&history);

        let mut history_bytes = Cursor::new(Vec::new());
        // TODO better error handling
        let mut int_buf = [0;4];
        NetworkEndian::write_u32(&mut int_buf, lookup.len() as u32);
        history_bytes.write(&mut int_buf).unwrap();
        for (&local, &(site_id, remote)) in lookup.iter() {
            NetworkEndian::write_u32(&mut int_buf, local);
            history_bytes.write(&mut int_buf).unwrap();
            NetworkEndian::write_u32(&mut int_buf, site_id);
            history_bytes.write(&mut int_buf).unwrap();
            NetworkEndian::write_u32(&mut int_buf, remote);
            history_bytes.write(&mut int_buf).unwrap();
        }
        history.compress_to(&mut history_bytes).unwrap();
        let history_str = base64::encode(history_bytes.into_inner().as_slice());
        let mut ret_args = HashMap::new();
        ret_args.insert("history".to_string(), Value::String(history_str));
        trace!("Transmitting history back");
        Ok((None, Some(ret_args)))

    })
}


fn lookup_to_list(lookup: &BTreeMap<u32, (u32, u32)>) -> List {
    lookup.iter().map(|(&local, &(site_id, remote))| Value::List(vec![Value::Integer(local as i64), Value::Integer(site_id as i64), Value::Integer(remote as i64)])).collect()
}

fn extract_index(list: &List, index: usize) -> u32 {
    let item = list.get(index).unwrap();
    if let &Value::Integer(i) = item {
        i as u32
    } else {
        panic!("Lookup did not contain three integers")
    }
}

fn list_to_lookups(list: &List) -> BTreeMap<u32, (u32, u32)> {
    list.iter().map(|entry| {
        if let &Value::List(ref entry) = entry {
            (extract_index(entry, 0), (extract_index(entry, 1), extract_index(entry, 2)))
        } else {
            panic!("Lookup was not a list")
        }
    }).collect()
}

fn create_file_list_callback(fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> Box<FnMut(List, Dict) -> CallResult<(Option<List>, Option<Dict>)>> {
    Box::new(move |args, kwargs| {
        let fileset = fileset.lock().unwrap();
        let files = fileset.get_all_files();
        let file_list:Vec<_> = files.iter().map(|(&(site_id, id), metadata)| {
            let mut filedata = HashMap::new();
            filedata.insert("site_id".to_string(), Value::Integer(site_id as i64));
            filedata.insert("id".to_string(), Value::Integer(id as i64));
            filedata.insert("timestamp".to_string(), Value::Integer(metadata.get_file_timestamp() as i64));
            filedata.insert("filename".to_string(), Value::List(metadata.get_file_path().iter().map(|component| Value::String(component.clone())).collect()));
            Value::Dict(filedata)
        }).collect();
        let mut the_args = HashMap::new();
        the_args.insert("files".to_string(), Value::List(file_list));
        Ok((None, Some(the_args)))
    })
}

fn get_remote_changes_since(timestamp: Option<(u32, u32)>, client: &mut WampClient) -> (HashMap<(u32, u32), FileHistory<FileUpdater>>, BTreeMap<u32, (u32, u32)>) {
    let mut kwargs = HashMap::with_capacity(2);
    if let Some(timestamp) = timestamp {
        kwargs.insert("site_id".to_string(), Value::Integer(timestamp.0 as i64));
        kwargs.insert("timestamp".to_string(), Value::Integer(timestamp.1 as i64));
    }
    // TODO better error handling
    let (_, rkwargs) = client.call(URI::new("ca.kitchen.get_changes"), None, Some(kwargs)).unwrap().await().unwrap();
    let change_str = rkwargs.get_string("changes").unwrap().unwrap();
    let change_bytes = base64::decode(change_str).unwrap();
    let mut change_bytes = Cursor::new(change_bytes);
    let mut int_buf = [0;4];
    change_bytes.read_exact(&mut int_buf).unwrap();
    trace!("Reading timestamps");
    let lookup_len = NetworkEndian::read_u32(&int_buf);
    trace!("timestamp len: {}", lookup_len);
    let mut timestamp_lookup = BTreeMap::new();
    for _ in 0..lookup_len {
        change_bytes.read_exact(&mut int_buf).unwrap();
        let local_time_stamp = NetworkEndian::read_u32(&int_buf);
        change_bytes.read_exact(&mut int_buf).unwrap();
        let site_id = NetworkEndian::read_u32(&int_buf);
        change_bytes.read_exact(&mut int_buf).unwrap();
        let remote_time_stamp = NetworkEndian::read_u32(&int_buf);
        timestamp_lookup.insert(local_time_stamp, (site_id, remote_time_stamp));
    }
    trace!("Read timestamps: {:?}", timestamp_lookup);
    change_bytes.read_exact(&mut int_buf).unwrap();
    let history_count = NetworkEndian::read_u32(&int_buf) as usize;
    let mut changes = HashMap::with_capacity(history_count);
    for _ in 0..history_count {
        let (site_id, id, history) = expand_file_history_entry(&mut change_bytes, &timestamp_lookup, timestamp).unwrap();
        changes.insert((site_id, id), history);
    }
    (changes, timestamp_lookup)

}


impl Client {
    pub fn new_cannonical(server_address: &str, data: Arc<Mutex<SyncData>>, fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> WampResult<Client> {
        let connection = Connection::new(server_address, "kitchen_realm");
        let mut client = try!(connection.connect());
        {
            let last_timestamp = data.lock().unwrap().stamper.get_last_timestamp().map(|(_local, remote)| remote);
            let mut fileset = fileset.lock().unwrap();
            trace!("Getting local changes");
            let local_changes = fileset.get_changes_since(last_timestamp);
            let local_lookup = data.lock().unwrap().stamper.get_timestamps_since(last_timestamp).unwrap();
            trace!("Local changes found.  Integrating");
            fileset.integrate_remote_file_list(local_changes, local_lookup);
            trace!("Integration complete");
            {
                let data = data.lock().unwrap();
                data.store.save_stamper(&data.stamper).unwrap();
            }
        }
        try!(client.register(URI::new("ca.kitchen.get_id"), create_get_id_callback()));
        try!(client.register(URI::new("ca.kitchen.check_timestamp"), create_check_timestamp_callback(data.clone())));
        try!(client.register(URI::new("ca.kitchen.get_changes"), create_changes_callback(data.clone(), fileset.clone())));
        try!(client.register(URI::new("ca.kitchen.get_all_files"), create_file_list_callback(fileset.clone())));
        try!(client.register(URI::new("ca.kitchen.get_file_history"), create_history_callback(data, fileset.clone())));
        try!(client.subscribe(URI::new("ca.kitchen.file_updates"), create_update_callback(fileset)));

        Ok(Client {
            client: client
        })
    }

    pub fn new_standard(server_address: &str, data: &Arc<Mutex<SyncData>>, fileset: Arc<Mutex<FileSet<FileUpdater>>>) -> WampResult<Client> {
        let connection = Connection::new(server_address, "kitchen_realm");
        trace!("Connecting to server");
        let mut client = try!(connection.connect());
        trace!("Subscribing to changes");
        try!(client.subscribe(URI::new("ca.kitchen.file_updates"), create_update_callback(fileset.clone())));

        trace!("Getting timestamp");
        let last_timestamp = data.lock().unwrap().stamper.get_last_timestamp().map(|(_local, remote)| remote);
        let mut fileset = fileset.lock().unwrap();
        trace!("Getting local changes");
        let local_changes = fileset.get_changes_since(last_timestamp);
        let local_lookup = data.lock().unwrap().stamper.get_timestamps_since(last_timestamp).unwrap();
        trace!("Getting remote changes");
        let (remote_changes, remote_lookup) = get_remote_changes_since(last_timestamp, &mut client);

        trace!("Transmitting local changes");
        // TODO made this more efficient
        for change in local_changes {
            send_transaction(&mut client, FileSetOperation::Update(UpdateOperation{
                id: change.0,
                data: change.1.operation_history
            }, local_lookup.clone()));
        }
        // pre stamp everything in the lookup to make sure the order is maintained
        {
            let mut stamper = &mut data.lock().unwrap().stamper;
            for (_, &(site_id, remote_timestmap)) in remote_lookup.iter() {
                stamper.stamp_remote(site_id, remote_timestmap);
            }
        }
//Something new.  Another new thing
        trace!("Integrating remote changes");
        fileset.integrate_remote_file_list(remote_changes, remote_lookup);
        {
            let data = data.lock().unwrap();
            data.store.save_stamper(&data.stamper).unwrap();
        }
        Ok(Client {
            client: client
        })
    }

    pub fn send_transaction(&mut self, operation: FileSetOperation<FileUpdater>) {
        send_transaction(&mut self.client, operation)
    }
}

fn read_str<R: io::Read>(reader: &mut R, int_buf: &mut [u8;4]) -> io::Result<String> {
    try!(reader.read_exact(int_buf));
    let str_len = NetworkEndian::read_u32(int_buf) as usize;
    let mut str_vec:Vec<u8> = Vec::with_capacity(str_len);
    str_vec.resize(str_len, 0);
    try!(reader.read_exact(&mut str_vec));
    Ok(String::from_utf8_lossy(str_vec.as_slice()).into_owned())
}
