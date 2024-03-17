mod serde;

// use rocksdb::{DB, WriteOptions};
use crate::executor::ExecuteError;




pub trait State {

    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), ExecuteError>;
}


pub struct RocksDBState {
    // db: Arc<DB>
}

impl State for RocksDBState {

    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        // self.db.get(key).ok()?
        None
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), ExecuteError>{
        // self.db.put( key, value)
        //     .map_err(|e| todo!())
        unimplemented!()
    }
}

impl RocksDBState {

    pub fn new(path: &str) -> Self {
        // let db = DB::open_default(
        //     path).unwrap();
        // RocksDBState {
        //     db: Arc::new(db)
        // }
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::state::{RocksDBState, State};

    #[test]
    fn t1() {
        let path = "/Users/luoyuxia/CLionProjects/rustflink/src/state/rocks";
        let state = RocksDBState::new(path);

        let key = "key".as_bytes();
        let val = "val".as_bytes();
        println!("{:?}", state.get(key.to_vec()));

        state.put(key.to_vec(), val.to_vec()).expect("TODO: panic message");

        println!("{:?}",
                 String::from_utf8(state.get(key.to_vec()).expect("xx")));
    }
}