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

pub mod transaction;

pub use transaction::{Statistics,Error};
use transaction::Commit;
mod txn;
pub use txn::{MutTxn, Txn, Value, Db, Iter};
use txn::{P, LoadPage}; // , MAIN_ROOT};
mod put;

mod merge;
mod rebalance;
mod del;

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
    /// Creates a new database.
    pub fn create_db(&mut self) -> Result<Db,Error> {
        let mut db = try!(self.alloc_page());
        db.init();
        Ok(Db { root_num:-1, root: db.page_offset() })
    }

    /// Produce an independent fork of a database. This method copies at most one block, and uses reference-counting on child blocks. The two databases share their bindings at the time of the fork, and can safely be considered separate databases after the fork.
    pub fn fork_db<R:Rng>(&mut self, rng:&mut R, db:&Db) -> Result<Db,Error> {
        try!(put::fork_db(rng, self, db.root));
        Ok(Db { root_num:-1, root: db.root })
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
    pub fn put<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: &[u8])->Result<bool,Error> {
        put::put(r, self, db, key, value)
    }

    /// Replace the binding for a key. This is actually no more than `del` and `put` in a row: if there are more than one binding for that key, replace the smallest one, in lexicographical order.
    pub fn replace<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: &[u8])->Result<(),Error> {
        del::replace(r, self, db, key, value)
    }

    /// Delete the smallest binding (in lexicographical order) from the map matching the key and value. When the `value` argument is `None`, delete the smallest binding for that key.
    pub fn del<R:Rng>(&mut self, r:&mut R, db: &mut Db, key: &[u8], value: Option<&[u8]>)->Result<bool,Error> {
        del::del(r, self, db, key, value)
    }

    /// Specialized version of ```put`` for the case where both the key and value are 64-bits integers.
    pub fn put_u64<R:Rng>(&mut self, rng:&mut R, db: &mut Db, key: u64, value: u64)->Result<bool,Error> {
        let mut k: [u8; 8] = [0; 8];
        let mut v: [u8; 8] = [0; 8];
        unsafe {
            *(k.as_mut_ptr() as *mut u64) = key.to_le();
            *(v.as_mut_ptr() as *mut u64) = value.to_le();
        }
        self.put(rng, db, &k, &v)
    }
    /// Specialized version of ```del`` for the case where the key is a 64-bits integer, and the value is None.
    pub fn del_u64<R:Rng>(&mut self, rng:&mut R, db:&mut Db, key:u64)->Result<bool,Error> {
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
    pub fn abort(self) {

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
            self.get_(page, key, value).map(|x| Value::from_unsafe(&x, self))
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
                    workspace: &'b mut Vec<u64>)->Iter<'a,'b,Self> {
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
    pub fn commit(mut self) -> Result<(), transaction::Error> {
        self.txn.commit()
    }
}

impl<'env,'txn,T> MutTxn<'env,&'txn mut transaction::MutTxn<'env,T>> {
    /// Commit the child transaction to its parent (consuming it).
    pub fn commit(mut self) -> Result<(), transaction::Error> {
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
    fn iterators() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        extern crate rustc_serialize;
        use rand::Rng;
        use std;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();
        let mut txn = env.mut_txn_begin();
        let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());

        let mut random = Vec::new();
        for _ in 0..300 {
            let k: String = rng
                .gen_ascii_chars()
                .take(20)
                .collect(); 
            let v: String = rng
                .gen_ascii_chars()
                .take(20)
                .collect();
            
            txn.put(&mut rng, &mut root, k.as_bytes(), v.as_bytes()).unwrap();
            random.push((k,v));
        }
        txn.set_root(0, root);
        println!("committing");
        txn.commit().unwrap();

        random.sort();
        let txn = env.txn_begin();
        let root = txn.root(0).unwrap();
        txn.debug(&[&root], "/tmp/iter", false, false);
        let mut ws = Vec::new();

        let mut i = 100;
        let (ref k0,ref v0) = random[i];
        for (k,_) in txn.iter(&root, k0.as_bytes(), Some(v0.as_bytes()), &mut ws).take(100) {
            let (ref kk,_) = random[i];
            println!("{:?} {:?}",
                     std::str::from_utf8(k).unwrap(),
                     kk);
            assert!(k == kk.as_bytes());
            i+=1
        }
    }


    #[test]
    fn deletions() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        extern crate env_logger;
        use rand::{Rng};

        struct R { current:u32 }
        impl Rng for R {
            fn next_u32(&mut self) -> u32 {
                let x = self.current;
                self.current = x.wrapping_add(1);
                x
            }
        }
        
        let mut rng = R { current:0 }; // rand::thread_rng();
        env_logger::init().unwrap_or(());
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 1000).unwrap();
        let mut txn = env.mut_txn_begin();
        let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());

        let mut bindings = Vec::new();
        for i in 0..500 {
            let k: String = rng
                .gen_ascii_chars()
                .take(200)
                .collect(); 
           let v: String = rng
                .gen_ascii_chars()
                .take(200)
                .collect();
            println!("putting {:?}", i);

            txn.put(&mut rng, &mut root, k.as_bytes(), v.as_bytes()).unwrap();
            txn.debug(&[&root], format!("/tmp/debug_{}",i), false, false);
            bindings.push((k,v));
        }
        println!("now deleting");
        bindings.sort();
        let mut i = 0;
        for &(ref k,ref v) in bindings.iter() {
            //println!(">>>>>>>>>>>>>>>>>> {} deleting {:?}\nv = {:?}", i, k, v);
            let r0 = rng.gen();
            let r1 = rng.gen();
            if r0 {
                txn.del(&mut rng, &mut root, k.as_bytes(),
                        if r1 { None } else { Some(v.as_bytes()) }).unwrap();
            }
            i+=1;
        }        
        txn.debug(&[&root], format!("/tmp/debug_{}",i), false, false);
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
            child_txn.commit().unwrap();
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
            child_txn.commit().unwrap();
        }
        txn.commit().unwrap();

        let txn = env.txn_begin();
        let root = txn.root(0).unwrap();
        assert!(txn.get(&root, b"A",None).and_then(|mut x| x.next()) == Some(b"Value for A"));
        assert!(txn.get(&root, b"B",None).is_none());
        assert!(txn.get(&root, b"C",None).and_then(|mut x| x.next()) == Some(b"Value for C"));
    }

    #[test]
    fn multiple_roots() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();

        let mut random = Vec::new();
        {
            let len = 32;
            let mut txn = env.mut_txn_begin();
            loop {
                let n_root: usize = rng.gen_range(0, 10);
                let mut root = txn.root(n_root as isize).unwrap_or_else(|| txn.create_db().unwrap());

                let k: String = rand::thread_rng()
                    .gen_iter::<char>()
                    .take(len)
                    .collect();
                let v: String = rand::thread_rng()
                    .gen_iter::<char>()
                    .take(len)
                    .collect();
                txn.put(&mut rng, &mut root, k.as_bytes(), v.as_bytes()).unwrap();
                random.push((n_root, k, v));
                
                txn.set_root(n_root as isize, root);
                let r:u8 = rng.gen();
                if r > 200 { break }
            }
            txn.commit().unwrap();
        }

        let txn = env.txn_begin();
        for &(ref db_name, ref k, ref v) in random.iter() {
            let db = txn.root(*db_name as isize).unwrap();
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|mut x| x.next()) == Some(v.as_bytes()));
            assert!(txn.get(&db, k.as_bytes(), Some(v.as_bytes())).and_then(|mut x| x.next()) == Some(v.as_bytes()))
        }
    }


    #[test]
    fn multiple_named_db() -> ()
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
            let mut root = txn.root(42).unwrap_or_else(|| txn.create_db().unwrap());
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
                    txn.put(&mut rng, &mut db, k.as_bytes(), v.as_bytes()).unwrap();
                    random.push((name.clone(), k, v));
                    
                    let r:u8 = rng.gen();
                    if r > 200 { break }
                }
                txn.put_db(&mut rng, &mut root, &name[..], db).unwrap();
            }
            txn.set_root(42, root);
            txn.commit().unwrap();
        }

        let txn = env.txn_begin();
        let root = txn.root(42).unwrap();
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
        extern crate env_logger;
        use rand::Rng;
        use std;

        env_logger::init().unwrap_or(());
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 10000).unwrap();

        let mut random:Vec<(String,String)> = Vec::new();
        let mut buf = Vec::new();
        {
            for i in 0..40 {
                println!("i = {:?}", i);
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
                        let got = txn.get(&db, k.as_bytes(), None).and_then(|x| {
                            //println!("value = {:?}", x.value);
                            buf.clear();
                            for i in x {
                                buf.extend(i)
                            }
                            Some(&buf[..])
                        });
                        if got != Some(v.as_bytes()) {
                            unsafe {
                                println!("{:?}\n{}",
                                         got.map(|x| std::str::from_utf8_unchecked(&x[0..100])),
                                         std::str::from_utf8_unchecked(&(v.as_bytes())[0..100])
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

                println!("Putting {:?}, {:?}", k,v);
                txn.put(&mut rng, &mut db, k.as_bytes(), v.as_bytes()).unwrap();
                
                if rng.gen() {
                    //txn.debug(&db,format!("/tmp/debug_{}",i),false,false);
                    txn.set_root(0, db);
                    txn.commit().unwrap();
                    //let stats = env.statistics();
                    //println!("statistics: {:?}", stats);
                    random.push((k, v));
                } else {
                    txn.set_root(0, db);
                    println!("abort !");
                    txn.abort()
                    // std::mem::drop(txn);
                }
                println!("{:?}", env.statistics());
            }
        }
        let txn = env.txn_begin();
        let db = txn.root(0).unwrap();
        for &(ref k, ref v) in random.iter() {
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|x| {
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
        consecutive_commits_(400,8000);
    }

    #[test]
    pub fn large_values_collect() -> () {
        extern crate tempdir;
        extern crate rand;
        use rand::Rng;

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
            txn.put(&mut rng, &mut db, k0.as_bytes(), v0.as_bytes()).unwrap();
            txn.put(&mut rng, &mut db, k1.as_bytes(), v1.as_bytes()).unwrap();
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

        random.push((k1,v1));
        let txn = env.txn_begin();
        let db = txn.root(0).unwrap();
        for &(ref k, ref v) in random.iter() {
            assert!(txn.get(&db, k.as_bytes(), None).and_then(|x| {
                buf.clear();
                for i in x {
                    buf.extend(i)
                }
                Some(&buf[..])
            }) == Some(v.as_bytes()))
        }
    }





    #[cfg(test)]
    pub fn leakproof_put(env:&Env, n_insertions:usize, value_size:usize) -> () {
        extern crate rand;
        use rand::Rng;
        extern crate env_logger;

        env_logger::init().unwrap_or(());

        let mut rng = rand::thread_rng();

        let key_len = 50;

        for i in 0..n_insertions {
            println!("i={:?}", i);

            let k0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(key_len)
                .collect();
            let v0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(value_size)
                .collect();

            let mut txn = env.mut_txn_begin();
            let mut db = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
            txn.put(&mut rng, &mut db, k0.as_bytes(), v0.as_bytes()).unwrap();

            txn.debug(&[&db], format!("/tmp/before_{}",i), false, false);
            txn.set_root(0, db);
            txn.commit().unwrap();
        }
    }




    #[cfg(test)]
    pub fn leakproof_put_del(env:&Env, n_insertions:usize, value_size:usize) -> () {
        extern crate rand;
        use rand::Rng;
        use std::collections::{HashMap};
        extern crate env_logger;

        env_logger::init().unwrap_or(());
        let mut rng = rand::thread_rng();

        let mut random:HashMap<String,String> = HashMap::new();

        let key_len = 50;

        for i in 0..n_insertions {
            println!("i={:?}", i);
            let k0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(key_len)
                .collect();
            let v0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(value_size)
                .collect();

            let mut txn = env.mut_txn_begin();
            let mut db = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
            txn.put(&mut rng, &mut db, k0.as_bytes(), v0.as_bytes()).unwrap();

            random.insert(k0,v0);
            
            txn.debug(&[&db], format!("/tmp/before_{}",i), false, false);
            txn.set_root(0, db);
            txn.commit().unwrap();
        }
        let mut i = 0;
        for (ref k, ref v) in random.iter() {
            debug!("i = {:?}, k = {:?}", i, k);
            let mut txn = env.mut_txn_begin();
            let mut db = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
            txn.del(&mut rng, &mut db, k.as_bytes(), Some(v.as_bytes())).unwrap();
            txn.debug(&[&db], format!("/tmp/after_{}",i), false, false);
            txn.set_root(0, db);
            txn.commit().unwrap();
            env.statistics();
            check_memory(&env, false);
            i+=1;
        }
    }

    
    #[cfg(test)]
    fn check_memory(env:&Env, print:bool) {
        use std::collections::{HashSet};
        use super::txn::{Page,LoadPage,P, read_key_value, UnsafeValue};

        let txn = env.txn_begin();
        let db = txn.root(0).unwrap();
        fn count_pages(txn:&Txn, page:&Page, pages:&mut HashSet<u64>, value_pages:&mut HashSet<u64>) {
            unsafe {
                let already = pages.insert(page.page_offset());
                if already {
                    let mut current = 0;
                    while current != 0xffff {
                        let child = u64::from_le(*(page.offset(current as isize) as *const u64).offset(2));
                        if child > 0 {
                            let child = txn.load_page(child);
                            count_pages(txn, &child, pages, value_pages);
                        }
                        if current > 0 {
                            let (_,value) = read_key_value(page.offset(current as isize));
                            if let UnsafeValue::O { offset, len } = value {
                                count_values(txn, offset, len, value_pages);
                            }
                        }
                        current = u16::from_le(*(page.offset(current as isize) as *const u16));
                    }
                }
            }
        }
        fn count_values(txn:&Txn, mut offset:u64, mut len:u32, pages:&mut HashSet<u64>) {
            loop {
                //println!("current offset = {:?}", offset);
                pages.insert(offset);
                if len <= super::transaction::PAGE_SIZE as u32 {
                    break
                } else {
                    let p = txn.load_page(offset);
                    unsafe {
                        offset = u64::from_le(*(p.offset(0) as *const u64));
                    }
                    len -= (super::transaction::PAGE_SIZE-8) as u32
                }
                
            }
        }
        let mut used_pages = HashSet::new();
        let mut value_pages = HashSet::new();
        let cow = txn.load_page(db.root);
        count_pages(&txn, &cow, &mut used_pages, &mut value_pages);
        let statistics = env.statistics();

        // Check that no page is referenced and free at the same time.
        assert!(statistics.free_pages.intersection(&used_pages).next().is_none());
        assert!(statistics.free_pages.intersection(&value_pages).next().is_none());
        assert!(value_pages.intersection(&used_pages).next().is_none());

        // Check that no page is referenced/free and bookkeeping at the same time.
        for i in statistics.bookkeeping_pages.iter() {
            if !(used_pages.contains(i) && value_pages.contains(i) && statistics.free_pages.contains(i)) {
                println!("i={:?}", i)
            }
            assert!(! used_pages.contains(i));
            assert!(! value_pages.contains(i));
            assert!(! statistics.free_pages.contains(i));
        }
        if print {
            println!("env statistics: {:?}", env.statistics());
            println!("counted pages: {:?}", used_pages);
            println!("value pages: {:?}", value_pages);

            println!("total: {:?}, counted: {:?}",
                     (statistics.total_pages as usize),
                     1
                     + statistics.bookkeeping_pages.len()
                     + statistics.free_pages.len()
                     + used_pages.len()
                     + value_pages.len());
        }
        let mut leaking = Vec::new();
        let mut p = 4096;
        while p < statistics.total_pages*4096 {
            if !(statistics.bookkeeping_pages.contains(&p)
                 || statistics.free_pages.contains(&p)
                 || used_pages.contains(&p)
                 || value_pages.contains(&p)) {
                leaking.push(p)
                }
            p+=4096
        }
        println!("leaking: {:?}", leaking);
        assert!( (statistics.total_pages as usize) ==
                  1
                  + statistics.bookkeeping_pages.len()
                  + statistics.free_pages.len()
                  + used_pages.len()
                  + value_pages.len()
        );
    }

    #[test]
    pub fn leakproof_put_short() {
        extern crate tempdir;
        let dir = tempdir::TempDir::new("pijul").unwrap();

        let n_insertions = 1000;
        let value_size = 50;

        let env = Env::new(dir.path(), 5000).unwrap();
        leakproof_put(&env, n_insertions, value_size);
        check_memory(&env, true);
    }

    #[test]
    pub fn leakproof_put_long() {
        extern crate tempdir;
        let dir = tempdir::TempDir::new("pijul").unwrap();

        let n_insertions = 100;
        let value_size = 1200;

        let env = Env::new(dir.path(), 5000).unwrap();
        leakproof_put(&env, n_insertions, value_size);
        check_memory(&env, true);
    }

    #[test]
    pub fn leakproof_put_really_long() {
        extern crate tempdir;
        let dir = tempdir::TempDir::new("pijul").unwrap();

        let n_insertions = 1000;
        let value_size = 8000;

        let env = Env::new(dir.path(), 5000).unwrap();
        leakproof_put(&env, n_insertions, value_size);
        check_memory(&env, true);
    }

    #[test]
    pub fn leakproof_put_del_short() {
        extern crate tempdir;
        let dir = tempdir::TempDir::new("pijul").unwrap();

        let n_insertions = 1000;
        let value_size = 400;

        let env = Env::new(dir.path(), 5000 as u64).unwrap();
        leakproof_put_del(&env, n_insertions, value_size);
        println!("checking");
        check_memory(&env, true);
    }

    #[test]
    pub fn leakproof_put_del_long() {
        extern crate tempdir;
        let dir = tempdir::TempDir::new("pijul").unwrap();

        let n_insertions = 1000;
        let value_size = 500;

        let env = Env::new(dir.path(), 10000 as u64).unwrap();
        leakproof_put_del(&env, n_insertions, value_size);
        println!("checking");
        check_memory(&env, true);
    }

    #[test]
    pub fn leakproof_put_del_really_long() {
        extern crate tempdir;
        let dir = tempdir::TempDir::new("pijul").unwrap();

        let n_insertions = 100;
        let value_size = 8000;

        let env = Env::new(dir.path(), 10000 as u64).unwrap();
        leakproof_put_del(&env, n_insertions, value_size);
        println!("checking");
        check_memory(&env, true);
    }

    #[test]
    fn fork_put_basic() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use super::Transaction;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();
        let mut txn = env.mut_txn_begin();
        let mut root = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());
        println!("root: {:?}", root);

        let common = b"test_key";
        let common_value = b"blabla";

        let key0 = b"key 0";
        let key0_value = b"blibli";

        let key1 = b"key 1";
        let key1_value = b"blublu";

        txn.put(&mut rng, &mut root, common, common_value).unwrap();
        let mut root2 = txn.fork_db(&mut rng, &root).unwrap();
        txn.put(&mut rng, &mut root, key0, key0_value).unwrap();
        txn.put(&mut rng, &mut root2, key1, key1_value).unwrap();
        txn.set_root(0, root);
        txn.set_root(1, root2);
        txn.commit().unwrap();
        println!("committed");

        let txn = env.txn_begin();
        let root0 = txn.root(0).unwrap();
        let root1 = txn.root(1).unwrap();

        assert!(txn.get(&root0, common, None).is_some());
        assert!(txn.get(&root0, key0, None).is_some());
        assert!(txn.get(&root0, key1, None).is_none());

        assert!(txn.get(&root1, common, None).is_some());
        assert!(txn.get(&root1, key0, None).is_none());
        assert!(txn.get(&root1, key1, None).is_some());
    }

    #[test]
    fn fork_put_many() -> ()
    {
        extern crate tempdir;
        extern crate rand;
        use super::Transaction;
        use rand::Rng;
        use std::collections::HashMap;
        let mut rng = rand::thread_rng();
        let dir = tempdir::TempDir::new("pijul").unwrap();
        let env = Env::new(dir.path(), 100).unwrap();


        let key_len = 200;
        let value_len = 200;
        let n_insertions = 200;

        let mut values0 = HashMap::new();
        let mut values1 = HashMap::new();
        
        let mut txn = env.mut_txn_begin();
        let mut root0 = txn.root(0).unwrap_or_else(|| txn.create_db().unwrap());

        for i in 0..n_insertions {
            println!("i = {:?}", i);
            let k0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(key_len)
                .collect();
            let v0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(value_len)
                .collect();

            txn.put(&mut rng, &mut root0, k0.as_bytes(), v0.as_bytes()).unwrap();
            values0.insert(k0.clone(),v0.clone());
            values1.insert(k0,v0);
        }

        let mut root1 = txn.fork_db(&mut rng, &root0).unwrap();
        txn.debug(&[&root0, &root1], format!("/tmp/before"), false, false);
        for j in 0..(n_insertions / 20) {
            println!("j = {:?}", j);

            let k0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(key_len)
                .collect();
            let v0: String = rand::thread_rng()
                .gen_ascii_chars()
                .take(value_len)
                .collect();

            if rng.gen() {
                println!("in 0");
                txn.put(&mut rng, &mut root0, k0.as_bytes(), v0.as_bytes()).unwrap();
                values0.insert(k0.clone(),v0.clone());
            } else {
                println!("in 1");
                txn.put(&mut rng, &mut root1, k0.as_bytes(), v0.as_bytes()).unwrap();
                values1.insert(k0,v0);
            }
            txn.debug(&[&root0, &root1], format!("/tmp/after_{}",j), false, false);
        }
        txn.debug(&[&root0, &root1], format!("/tmp/forked"), false, false);
        txn.set_root(0, root0);
        txn.set_root(1, root1);


        txn.commit().unwrap();



    }
    

}
