// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Fast and reliable key-value store, under the Mozilla Public License (link as you like, share modifications).
//!
//! # Features
//!
//! - ACID semantics.
//!
//! - B trees with copy-on-write.
//!
//! - Support for referential transparency: databases can be cloned in time O(1).
//!
//! - Ultimately, we'd like to have no locks. Right now, there is a
//! cross-process read write lock, that only ```commit``` takes
//! exclusively (other parts of a mutable transaction need just a read
//! access).
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
//! - dynamic loading of pages not in the map, which is especially
//! useful on 32-bits platforms.
//!
//! # For future versions
//!
//! - implement advertised lock model (right now, committing a writer excludes readers, there's no other lock).
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
//!    let env = sanakirja::Env::new(dir.path(),100).unwrap();
//!    let mut txn = env.mut_txn_begin();
//!    let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
//!    txn.put(&mut rng, &mut root, b"test key", b"test value").unwrap();
//!    txn.set_root(0,root);
//!    txn.commit().unwrap();
//!
//!    let txn = env.txn_begin();
//!    let root = txn.root(0).unwrap();
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

pub use transaction::{Statistics,Error};
use transaction::Commit;
mod txn;
pub use txn::{MutTxn, Txn, Value, Db, Iter};
use txn::{P, LoadPage}; // , MAIN_ROOT};
mod put_del;

/// Environment, essentially containing locks and mmaps.
pub struct Env {
    env: transaction::Env,
}


impl Env {
    /// Creates an environment. Size is a number of blocks.
    pub fn new<P: AsRef<Path>>(file: P, size:u64) -> Result<Env, Error> {
        transaction::Env::new(file, size*(1<<12)).and_then(|env| Ok(Env { env: env }))
    }

    /// Start an immutable transaction.
    pub fn txn_begin<'env>(&'env self) -> Txn<'env> {
        Txn {
            txn: self.env.txn_begin()
        }
    }

    /// Start a mutable transaction.

    pub fn mut_txn_begin<'env>(&'env self) -> MutTxn<'env,()> {
        let txn = self.env.mut_txn_begin();
        MutTxn {
            txn: txn,
        }
    }
    /// Returns statistics about pages.
    pub fn statistics(&self) -> Statistics {
        let mut stats = self.env.statistics();
        let txn = self.mut_txn_begin();
        if let Some(db) = txn.rc() {
            txn.iterate(&db, &[], None, |key,mut value| {
                unsafe {
                    let key = u64::from_le(*(key.as_ptr() as *const u64));
                    let value = u64::from_le(*(value.next().unwrap().as_ptr() as *const u64));
                    stats.reference_counts.insert(key,value);
                }
                true
            })
        }
        stats
    }

}

impl<'env,T> MutTxn<'env,T> {
    /// Creates a new database. Use
    ///
    /// ```
    /// // txn.open_db(b"name").unwrap_or_else(|| txn.create_db())
    /// ```
    ///
    /// To open a database called "name" from the root database, and create it if it doesn't exist.
    pub fn create_db(&mut self) -> Result<Db,Error> {
        let mut db = try!(self.alloc_page());
        db.init();
        Ok(Db { root_num:-1, root: db.page_offset() })
    }

    /// Produce an independent fork of a database. This method copies at most one block, and uses reference-counting on child blocks. The two databases share their bindings at the time of the fork, and can safely be considered separate databases after the fork.
    /// A typical way to fork a database and add it under a different name is:
    ///
    /// ```
    /// // let fork = txn.fork_db(&mut rng, &original);
    /// // txn.put_db(&mut rng, &mut root, b"name of the forked db", fork);
    /// 
    /// ```
    pub fn fork_db<R:Rng>(&mut self, rng:&mut R, db:&Db) -> Result<Db,Error> {
        Ok(Db { root_num:-1, root: try!(put_del::fork_db(rng, self, db.root)) })
    }

    /// Specialized version of ```put``` to register the name of a database. Argument ```db``` can be the root database (as in LMDB) or any other database.
    pub fn put_db<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: &[u8], value: Db)->Result<(),Error> {
        let mut val: [u8; 8] = [0; 8];
        unsafe {
            *(val.as_mut_ptr() as *mut u64) = value.root.to_le();
        }
        try!(self.replace(rng, db, key, &val));
        //self.txn.set_root(db.root_num, db.root);
        Ok(())
    }

    /// Add a binding to a B tree.
    pub fn put<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: &[u8])->Result<(),Error> {
        put_del::put(r, self, db, key, value)
    }

    /// Replace the binding for a key. This is actually no more than `del` and `put` in a row: if there are more than one binding for that key, replace the smallest one, in lexicographical order.
    pub fn replace<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: &[u8])->Result<(),Error> {
        put_del::replace(r, self, db, key, value)
    }

    /// Delete the smallest binding (in lexicographical order) from the map matching the key and value. When the `value` argument is `None`, delete the smallest binding for that key.
    pub fn del<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: Option<&[u8]>)->Result<(),Error> {
        put_del::del(r, self, db, key, value)
    }

    /// Specialized version of ```put`` for the case where both the key and value are 64-bits integers.
    pub fn put_u64<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: u64, value: u64)->Result<(),Error> {
        let mut k: [u8; 8] = [0; 8];
        let mut v: [u8; 8] = [0; 8];
        unsafe {
            *(k.as_mut_ptr() as *mut u64) = key.to_le();
            *(v.as_mut_ptr() as *mut u64) = value.to_le();
        }
        self.put(rng, db, &k, &v)
    }
    /// Specialized version of ```del`` for the case where the key is a 64-bits integer, and the value is None.
    pub fn del_u64<R:Rng>(&mut self, rng:&mut R, db:&mut Db, key:u64)->Result<(),Error> {
        let mut k: [u8; 8] = [0; 8];
        unsafe {
            *(k.as_mut_ptr() as *mut u64) = key.to_le();
        }
        self.del(rng, db, &k, None)
    }

    /// Specialized version of ```replace`` for the case where the key is a 64-bits integer.
    pub fn replace_u64<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: u64, value: u64)->Result<(),Error> {
        let mut k: [u8; 8] = [0; 8];
        let mut v: [u8; 8] = [0; 8];
        unsafe {
            *(k.as_mut_ptr() as *mut u64) = key.to_le();
            *(v.as_mut_ptr() as *mut u64) = value.to_le();
        }
        self.replace(rng, db, &k, &v)
    }

    /// Set the root database, consuming it.
    pub fn set_root(&mut self, num:isize, db:Db) {
        assert!(num>=0);
        self.txn.set_root(num+1, db.root)
    }

    /// Create a child transaction, which can be either committed to its parent (but not to the file), or aborted independently from its parent.
    pub fn mut_txn_begin<'txn>(&'txn mut self) -> MutTxn<'env,&'txn mut transaction::MutTxn<'env,T>> {
        MutTxn {
            txn: self.txn.mut_txn_begin()
        }
    }

}

pub trait Transaction:LoadPage {
    /// Load the root database, if there's one.
    fn root(&self, num:isize) -> Option<Db> {
        self.root_db_(num+1)
    }
    /// get the smallest value corresponding to a key (or to a key and a value). The return type is an iterator outputting byte slices.
    fn get<'a>(&'a self, db: &Db, key: &[u8], value:Option<&[u8]>) -> Option<Value<'a,Self>> {
        unsafe {
            let page = self.load_page(db.root);
            let value = value.map(|x| txn::UnsafeValue::S { p:x.as_ptr(), len:x.len() as u32 });
            self.get_(page, key, value).map(|x| Value { txn:self, value:x })
        }
    }

    /// Open an existing database from the root database.
    fn open_db<'a>(&'a self, root_db:&Db, key: &[u8]) -> Option<Db> {
        self.open_db_(root_db, key)
    }

    /// Iterate a function, starting from the `key` and `value` arguments, until the function returns `false`.
    fn iterate<'a, F: FnMut(&'a [u8], Value<'a,Self>) -> bool>(&'a self,
                                                               db: &Db,
                                                               key: &[u8],
                                                               value: Option<&[u8]>,
                                                               mut f: F) {
        unsafe {
            let page = self.load_page(db.root);
            let value = value.map(|x| txn::UnsafeValue::S { p:x.as_ptr(), len:x.len() as u32 });
            self.iterate_(txn::Iterate::NotStarted,page,key,value,&mut f);
        }
    }


    /// Return an iterator on a database, starting with the given key and value.
    fn iter<'a, 'b>(&'a self,
                    db: &Db,
                    key: &[u8],
                    value: Option<&[u8]>,
                    workspace: &'b mut Vec<(u64,u16)>)->Iter<'a,'b,Self> {
        unsafe {
            let page = self.load_page(db.root);
            let value = value.map(|x| txn::UnsafeValue::S { p:x.as_ptr(), len:x.len() as u32 });
            self.iter_(workspace, &page, key,value)
        }
    }


}

impl<'env> Transaction for Txn<'env> {}
impl<'env,T> Transaction for MutTxn<'env,T> {}



impl<'env> MutTxn<'env,()> {
    /// Commit the transaction to the file (consuming it).
    pub fn commit(self) -> Result<(), transaction::Error> {
        self.txn.commit()
    }
}

impl<'env,'txn,T> MutTxn<'env,&'txn mut transaction::MutTxn<'env,T>> {
    /// Commit the child transaction to its parent (consuming it).
    pub fn commit(self) -> Result<(), transaction::Error> {
        self.txn.commit()
    }
}


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
        let env = Env::new(dir.path(), 100).unwrap();
        let mut txn = env.mut_txn_begin();
        let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
        println!("root: {:?}", root);
        txn.put(&mut rng, &mut root, b"test key", b"test value").unwrap();
        txn.set_root(0, root);
        println!("committing");
        txn.commit().unwrap();

        let txn = env.txn_begin();
        let root = txn.root(0);
        println!("root = {:?}", root);
        let root = root.unwrap();
        assert!(txn.get(&root, b"test key",None).and_then(|mut x| x.next()) == Some(b"test value"))
    }

    #[test]
    fn deletions() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use rand::{Rng};
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();
        let mut txn = env.mut_txn_begin();
        let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());

        let mut bindings = Vec::new();
        for i in 0..30 {
            let k: String = rng
                .gen_ascii_chars()
                .take(200)
                .collect(); 
           let v: String = rng
                .gen_ascii_chars()
                .take(200)
                .collect();
            txn.put(&mut rng, &mut root, k.as_bytes(), v.as_bytes());
            bindings.push((k,v));
        }

        bindings.sort();
        let mut i = 0;
        for &(ref k,ref v) in bindings.iter() {
            txn.debug(&root, format!("/tmp/debug_{}",i), false, false);
            //println!(">>>>>>>>>>>>>>>>>> {} deleting {:?}\nv = {:?}", i, k, v);
            txn.del(&mut rng, &mut root, k.as_bytes(), Some(v.as_bytes())).unwrap();
            i+=1;
        }        
        txn.debug(&root, format!("/tmp/debug_{}",i), false, false);
        //println!("{:?}",bindings.len());
        txn.set_root(0, root);
        txn.commit().unwrap();
    }

    
    #[test]
    fn nested() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();
        let mut txn = env.mut_txn_begin();
        {
            let mut child_txn = txn.mut_txn_begin();
            let mut root = child_txn.root(0).unwrap_or_else(|| child_txn.create_db().unwrap());
            child_txn.put(&mut rng, &mut root, b"A", b"Value for A").unwrap();
            child_txn.set_root(0, root);
            child_txn.commit();
        }
        {
            let mut child_txn = txn.mut_txn_begin();
            let mut root = child_txn.root(0).unwrap();
            child_txn.put(&mut rng, &mut root, b"B", b"Value for B").unwrap();
            child_txn.set_root(0, root);
            //child_txn.abort();
        }
        {
            let mut child_txn = txn.mut_txn_begin();
            let mut root = child_txn.root(0).unwrap_or_else(|| child_txn.create_db().unwrap());
            child_txn.put(&mut rng, &mut root, b"C", b"Value for C").unwrap();
            child_txn.set_root(0, root);
            child_txn.commit();
        }
        txn.commit();

        let txn = env.txn_begin();
        let root = txn.root(0).unwrap();
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
        let env = Env::new(dir.path(), 100).unwrap();

        let db_names = [b"great db".to_vec(), b"other db".to_vec(), b"blabla\0blibli".to_vec()];
        let mut random = Vec::new();
        {
            let len = 32;
            let mut txn = env.mut_txn_begin();
            let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
            for ref name in db_names.iter() {
                let mut db = txn.open_db(&root, &name[..]).unwrap_or(txn.create_db().unwrap());
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
            txn.set_root(0, root);
            txn.commit().unwrap();
        }

        let db_name = b"great db";


        let txn = env.txn_begin();
        let root = txn.root(0).unwrap();
        for &(ref db_name, ref k, ref v) in random.iter() {
            let db = txn.open_db(&root, &db_name[..]).unwrap();
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|mut x| x.next()) == Some(v.as_bytes()));
            assert!(txn.get(&db, k.as_bytes(), Some(v.as_bytes())).and_then(|mut x| x.next()) == Some(v.as_bytes()))
        }
    }


    fn consecutive_commits_(key_len:usize, value_len:usize) -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;
        use std;
        use rand::SeedableRng;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();

        let mut random:Vec<(String,String)> = Vec::new();
        let mut buf = Vec::new();
        {
            for i in 0..200 {
                //println!("");
                let mut txn = env.mut_txn_begin();
                let mut db = txn.root(0).unwrap_or_else(|| {
                    //println!("create db");
                    txn.create_db().unwrap()
                });
                {
                    // Are all values inserted so far here?
                    //println!("find");
                    for &(ref k, ref v) in random.iter() {
                        //println!("found");
                        //println!("getting {:?}", k);
                        let got = txn.get(&db, k.as_bytes(), None).and_then(|mut x| {
                            buf.clear();
                            for i in x {
                                buf.extend(i)
                            }
                            Some(&buf[..])
                        });
                        if got != Some(v.as_bytes()) {
                            unsafe {
                                println!("{:?}\n{}",
                                         got.map(|x| std::str::from_utf8_unchecked(x)),
                                         std::str::from_utf8_unchecked(v.as_bytes())
                                );
                            }
                        }
                        assert!(got == Some(v.as_bytes()))
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

                //println!("Putting {:?}, {:?}", k,v);
                txn.put(&mut rng, &mut db, k.as_bytes(), v.as_bytes()).unwrap();
                
                if rng.gen() {
                    //txn.debug(&db,format!("/tmp/debug_{}",i),false,false);
                    txn.set_root(0, db);
                    txn.commit().unwrap();
                    let stats = env.statistics();
                    //println!("statistics: {:?}", stats);
                    random.push((k, v));
                } else {
                    txn.set_root(0, db);
                    // println!("abort !");
                    // txn.abort()
                }
            }
        }
        let txn = env.txn_begin();
        let db = txn.root(0).unwrap();
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


    pub fn large_values() -> () {
        consecutive_commits_(400,8000);
    }

    #[test]
    pub fn large_values_collect() -> () {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;
        use rand::SeedableRng;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();

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
            let mut db = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
            txn.put(&mut rng, &mut db, k0.as_bytes(), v0.as_bytes());
            txn.put(&mut rng, &mut db, k1.as_bytes(), v1.as_bytes());
            txn.set_root(0, db);
            txn.commit().unwrap();
        }

        {
            let mut txn = env.mut_txn_begin();
            let mut db = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
            //txn.debug(&db,"/tmp/before");
            txn.del(&mut rng, &mut db, k0.as_bytes(), Some(v0.as_bytes())).unwrap();
            //txn.debug(&db,"/tmp/after");
            txn.set_root(0, db);
            txn.commit().unwrap();
        }

        let txn = env.txn_begin();
        let db = txn.root(0).unwrap();
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
