// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Fast and reliable key-value store, under the Mozilla Public License (link as you like, share modifications).
//!
//! # Features
//!
//! - ACID semantics, using a separable transactions module, reusable for other structure.
//!
//! - B-trees with copy-on-write.
//!
//! - Support for referential transparency (interface still missing).
//!
//! - No read locks. The only three cases of locks are (1) on the first transaction of a thread, (2) when starting concurrent writers, or (3) when starting a writer in parallel with a reader started before the last commit on the file.
//!
//!
//! This version is only capable of inserting and retrieving keys in
//! the database, allowing several bindings for the same key (get will
//! retrieve the first one).
//!
//! Implementation details, in particular the file format, are
//! documented in the file.
//!
//! # Todo-list
//!
//! - check that all dereferences are converted to/from little-endian. (easy)
//!
//! - improve error handling
//!
//! - reference counting, and then clone (half-easy)
//!
//! - dynamic loading of pages not in the map, especially on 32-bits platforms ('transaction.rs', half-easy)
//!
//! - documenting the format.
//!
//! # For future versions
//!
//! - implement advertised lock model (right now, committing a writer excludes readers, there's no other lock).
//!
//! - merging pages to rebalance more (delete).
//!
//! - combined "CoW + delete".
//!
//! # Example
//!
//! ```
//! extern crate rand;
//! extern crate tempdir;
//! extern crate sanakirja;
//! use self::sanakirja::Transaction;
//!
//! fn main() {
//!    let mut rng = rand::thread_rng();
//!    let dir = tempdir::TempDir::new("pijul").unwrap();
//!    let env = sanakirja::Env::new(dir.path()).unwrap();
//!    let mut txn = env.mut_txn_begin();
//!    let mut root = txn.root().unwrap_or_else(|| txn.create_db());
//!    txn.put(&mut rng, &mut root, b"test key", b"test value");
//!    txn.set_root(root);
//!    txn.commit().unwrap();
//!
//!    let txn = env.txn_begin();
//!    let root = txn.root().unwrap();
//!    assert!(txn.get(&root, b"test key",None).and_then(|mut x| x.next()) == Some(b"test value"))
//! }
//! ```
//!


extern crate libc;
extern crate rand;
extern crate rustc_serialize;

#[macro_use]
extern crate log;
extern crate fs2;
extern crate memmap;

use rand::Rng;
use std::path::Path;

#[allow(mutable_transmutes)]
mod transaction;

pub use transaction::{Statistics};
use transaction::Commit;
mod txn;
pub use txn::{MutTxn, Txn, Value, Db};
use txn::{P, LoadPage};
mod put_del;

/// Environment, containing in particular a pointer to the memory-mapped file.
pub struct Env {
    env: transaction::Env,
}

pub type Error = transaction::Error;

impl Env {
    /// Creates an environment.
    pub fn new<P: AsRef<Path>>(file: P) -> Result<Env, Error> {
        transaction::Env::new(file, 13 + 10).and_then(|env| Ok(Env { env: env }))
    }

    /// Returns statistics about pages.
    pub fn statistics(&self) -> Statistics {
        self.env.statistics()
    }

    /// Start an immutable transaction.
    pub fn txn_begin<'env>(&'env self) -> Txn<'env> {
        Txn {
            txn: self.env.txn_begin()
        }
    }

    /// Start a mutable transaction.

    pub fn mut_txn_begin<'env>(&'env self) -> MutTxn<'env,()> {
        let mut txn = self.env.mut_txn_begin();
        MutTxn {
            txn: txn,
        }
    }
}


// Insert must return the new root.
// When searching the tree, note whether at least one page had RC >= 2. If so, reallocate + copy all pages on the path.


impl<'env> MutTxn<'env,()> {
    pub fn commit(mut self) -> Result<(), transaction::Error> {
        self.txn.commit()
    }
}

impl<'env,'txn,T> MutTxn<'env,&'txn mut transaction::MutTxn<'env,T>> {
    pub fn commit(mut self) -> Result<(), transaction::Error> {
        self.txn.commit()
    }
}

impl<'env,T> MutTxn<'env,T> {
    pub fn mut_txn_begin<'txn>(&'txn mut self) -> MutTxn<'env,&'txn mut transaction::MutTxn<'env,T>> {
        MutTxn {
            txn: self.txn.mut_txn_begin()
        }
    }
    pub fn create_db(&mut self) -> Db {
        let mut db = self.alloc_page();
        db.init();
        Db { root: db.page_offset() }
    }
    pub fn put_db<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: &[u8], value: Db) {
        let mut val: [u8; 8] = [0; 8];
        unsafe {
            *(val.as_mut_ptr() as *mut u64) = value.root.to_le();
        }
        self.put(rng, db, key, &val);
        self.txn.set_root(db.root)
    }
    pub fn put_u64<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: u64, value: u64) {
        let mut k: [u8; 8] = [0; 8];
        let mut v: [u8; 8] = [0; 8];
        unsafe {
            *(k.as_mut_ptr() as *mut u64) = key.to_le();
            *(v.as_mut_ptr() as *mut u64) = value.to_le();
        }
        self.put(rng, db, &k, &v);
    }
    pub fn replace_u64<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: u64, value: u64) {
        let mut k: [u8; 8] = [0; 8];
        let mut v: [u8; 8] = [0; 8];
        unsafe {
            *(k.as_mut_ptr() as *mut u64) = key.to_le();
            *(v.as_mut_ptr() as *mut u64) = value.to_le();
        }
        self.replace(rng, db, &k, &v);
    }
    pub fn set_root(&mut self, db:Db) {
        self.txn.set_root(db.root)
    }
    pub fn put<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: &[u8]) {
        put_del::put(r, self, db, key, value)
    }
    pub fn replace<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: &[u8]) {
        put_del::replace(r, self, db, key, value)
    }
    pub fn del<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: Option<&[u8]>) {
        put_del::del(r, self, db, key, value)
    }
}

pub trait Transaction:LoadPage {
    fn root(&self) -> Option<Db> {
        self.root_db_()
    }
    fn get<'a>(&'a self, db: &Db, key: &[u8], value:Option<&[u8]>) -> Option<Value<'a,Self>> {
        unsafe {
            let page = self.load_page(db.root);
            let value = value.map(|x| txn::UnsafeValue::S { p:x.as_ptr(), len:x.len() as u32 });
            self.get_(page, key, value).map(|x| Value { txn:self, value:x })
        }
    }
    fn open_db<'a>(&'a self, key: &[u8]) -> Option<Db> {
        self.open_db_(key)
    }

    fn iterate<'a, F: Fn(&'a [u8], Value<'a,Self>) -> bool>(&'a self,
                                                            db: &Db,
                                                            key: &[u8],
                                                            value: Option<&[u8]>,
                                                            f: F) {
        unsafe {
            let page = self.load_page(db.root);
            let value = value.map(|x| txn::UnsafeValue::S { p:x.as_ptr(), len:x.len() as u32 });
            self.iterate_(txn::Iterate::NotStarted,page,key,value,&f);
        }
    }
}

impl<'env> Transaction for Txn<'env> {}
impl<'env,T> Transaction for MutTxn<'env,T> {}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path()).unwrap();
        let mut txn = env.mut_txn_begin();
        let mut root = txn.root().unwrap_or_else(|| txn.create_db());
        txn.put(&mut rng, &mut root, b"test key", b"test value");
        txn.set_root(root);
        txn.commit().unwrap();

        let txn = env.txn_begin();
        let root = txn.root().unwrap();
        assert!(txn.get(&root, b"test key",None).and_then(|mut x| x.next()) == Some(b"test value"))
    }

    #[test]
    fn nested() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path()).unwrap();
        let mut txn = env.mut_txn_begin();
        {
            let mut child_txn = txn.mut_txn_begin();
            let mut root = child_txn.root().unwrap_or_else(|| child_txn.create_db());
            child_txn.put(&mut rng, &mut root, b"A", b"Value for A");
            child_txn.set_root(root);
            child_txn.commit();
        }
        {
            let mut child_txn = txn.mut_txn_begin();
            let mut root = child_txn.root().unwrap();
            child_txn.put(&mut rng, &mut root, b"B", b"Value for B");
            child_txn.set_root(root);
            //child_txn.abort();
        }
        {
            let mut child_txn = txn.mut_txn_begin();
            let mut root = child_txn.root().unwrap_or_else(|| child_txn.create_db());
            child_txn.put(&mut rng, &mut root, b"C", b"Value for C");
            child_txn.set_root(root);
            child_txn.commit();
        }
        txn.commit();

        let txn = env.txn_begin();
        let root = txn.root().unwrap();
        assert!(txn.get(&root, b"A",None).and_then(|mut x| x.next()) == Some(b"Value for A"));
        assert!(txn.get(&root, b"B",None).is_none());
        assert!(txn.get(&root, b"C",None).and_then(|mut x| x.next()) == Some(b"Value for C"));
    }

    #[test]
    fn multiple_db() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path()).unwrap();

        let db_names = [b"great db".to_vec(), b"other db".to_vec(), b"blabla\0blibli".to_vec()];
        let mut random = Vec::new();
        {
            let len = 32;
            let mut txn = env.mut_txn_begin();
            let mut root = txn.root().unwrap_or_else(|| txn.create_db());
            for ref name in db_names.iter() {
                let mut db = txn.open_db(&name[..]).unwrap_or(txn.create_db());
                loop {
                    let k: String = rand::thread_rng()
                        .gen_iter::<char>()
                        .take(len)
                        .collect();
                    let v: String = rand::thread_rng()
                        .gen_iter::<char>()
                        .take(len)
                        .collect();
                    txn.put(&mut rng, &mut db, k.as_bytes(), v.as_bytes());
                    random.push((name.clone(), k, v));
                    
                    let r:u8 = rng.gen();
                    if r > 200 { break }
                }
                txn.put_db(&mut rng, &mut root, &name[..], db);
            }
            txn.set_root(root);
            txn.commit().unwrap();
        }

        let db_name = b"great db";


        let txn = env.txn_begin();
        let root = txn.root().unwrap();
        for &(ref db_name, ref k, ref v) in random.iter() {
            let db = txn.open_db(&db_name[..]).unwrap();
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|mut x| x.next()) == Some(v.as_bytes()));
            assert!(txn.get(&db, k.as_bytes(), Some(v.as_bytes())).and_then(|mut x| x.next()) == Some(v.as_bytes()))
        }
    }


    fn consecutive_commits_(key_len:usize, value_len:usize) -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;
        use rand::SeedableRng;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path()).unwrap();

        let mut random:Vec<(String,String)> = Vec::new();
        let mut buf = Vec::new();
        {
            for i in 0..20 {
                let mut txn = env.mut_txn_begin();
                let mut db = txn.root().unwrap_or_else(|| txn.create_db());
                {
                    // Are all values inserted so far here?
                    for &(ref k, ref v) in random.iter() {
                        assert!(txn.get(&db, k.as_bytes(), None).and_then(|mut x| {
                            buf.clear();
                            for i in x {
                                buf.extend(i)
                            }
                            Some(&buf[..])
                        }) == Some(v.as_bytes()))
                    }
                }
                let k: String = rand::thread_rng()
                    .gen_ascii_chars()
                    .take(key_len)
                    .collect();
                let v: String = rand::thread_rng()
                    .gen_ascii_chars()
                    .take(value_len)
                    .collect();

                txn.put(&mut rng, &mut db, k.as_bytes(), v.as_bytes());
                
                if rng.gen() {
                    txn.set_root(db);
                    txn.commit().unwrap();
                    random.push((k, v));                
                } else {
                    txn.set_root(db);
                    // txn.abort()
                }
            }
        }
        let txn = env.txn_begin();
        let db = txn.root().unwrap();
        for &(ref k, ref v) in random.iter() {
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|mut x| {
                buf.clear();
                for i in x {
                    buf.extend(i)
                }
                Some(&buf[..])
            }) == Some(v.as_bytes()))
        }
    }

    #[test]
    pub fn consecutive_commits() -> () {
        consecutive_commits_(50,60);
    }


    #[test]
    pub fn large_values() -> () {
        consecutive_commits_(500,8000);
    }

    #[test]
    pub fn large_values_collect() -> () {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;
        use rand::SeedableRng;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path()).unwrap();

        let mut random:Vec<(String,String)> = Vec::new();
        let mut buf = Vec::new();

        let key_len = 50;
        let long_len = 8000;
        let short_len = 50;

        // Long
        let k0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(key_len)
            .collect();
        let v0: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(long_len)
            .collect();
        println!("{:?}", &v0[..20]);
        // Short
        let k1: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(key_len)
            .collect();
        let v1: String = rand::thread_rng()
            .gen_ascii_chars()
            .take(short_len)
            .collect();
        {
            let mut txn = env.mut_txn_begin();
            let mut db = txn.root().unwrap_or_else(|| txn.create_db());
            txn.put(&mut rng, &mut db, k0.as_bytes(), v0.as_bytes());
            txn.put(&mut rng, &mut db, k1.as_bytes(), v1.as_bytes());
            txn.set_root(db);
            txn.commit().unwrap();
        }

        {
            let mut txn = env.mut_txn_begin();
            let mut db = txn.root().unwrap_or_else(|| txn.create_db());
            //txn.debug(&db,"/tmp/before");
            txn.del(&mut rng, &mut db, k0.as_bytes(), Some(v0.as_bytes()));
            //txn.debug(&db,"/tmp/after");
            txn.set_root(db);
            txn.commit().unwrap();
        }

        let txn = env.txn_begin();
        let db = txn.root().unwrap();
        for &(ref k, ref v) in random.iter() {
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|mut x| {
                buf.clear();
                for i in x {
                    buf.extend(i)
                }
                Some(&buf[..])
            }) == Some(v.as_bytes()))
        }
    }

}
