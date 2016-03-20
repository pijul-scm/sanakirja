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
//! - No locks, writers exclude each other, and only exclude readers during calls to ```commit()``` (readers still read the database as it was before the start of the writers/mutable transaction).
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
//! - dynamic loading of pages not in the map, especially on 32-bits platforms ('transaction.rs', half-easy)
//!
//! - deleting the last entry in a page.
//!
//! - merging pages (delete).
//!
//! - combined "CoW + delete".
//!
//! - making sure keys fit in cut pages, indication of available space (put). Then, documenting the format.
//!
//! - reference counting, and then clone (half-easy)
//!
//! - B-node compaction on copy, and efficient copy when deleting (move entries with current key to the end of the page).
//!
//! - decide whether to restrict the API to an iterator of &[u8], or keep concatenated mmaps (for very large values)
//!
//! # Example
//!
//! ```
//! let dir = "/tmp/test";
//! let env = sanakirja::Env::new(dir).unwrap();
//! let mut txn = env.mut_txn_begin();
//! let mut root = txn.root_db();
//! root = txn.put(root,b"test key", b"test value");
//! txn.set_global_root(root);
//! txn.commit().unwrap();
//!
//! let txn = env.txn_begin();
//! let root = txn.root_db();
//! assert!(txn.get(&root, b"test key", None).map(|x| x.as_slice()) == Some(b"test value"))
//! ```
//!

extern crate libc;
extern crate rand;

#[macro_use]
extern crate log;
extern crate fs2;
use rand::Rng;
use std::path::Path;

mod memmap;
mod transaction;

pub use transaction::Statistics;

mod txn;
pub use txn::{MutTxn, Txn, Value, Db};
use txn::{P, LoadPage};
//mod rebalance;
mod put;
//mod del;

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
        unsafe {
            let p_extra = self.env.extra() as *const u64;
            Txn {
                txn: self.env.txn_begin(),
                btree_root: u64::from_le(*p_extra),
            }
        }
    }

    /// Start a mutable transaction.
    pub fn mut_txn_begin<'env>(&'env self) -> MutTxn<'env> {
        unsafe {
            let mut txn = self.env.mut_txn_begin();
            let p_extra = self.env.extra() as *const u64;
            let btree_root = u64::from_le(*p_extra);
            let btree_root = if btree_root == 0 {
                let p = txn.alloc_page().unwrap();
                std::ptr::write_bytes(p.data as *mut u8, 0, 24);
                p.offset
            } else {
                btree_root
            };
            MutTxn {
                txn: txn,
                btree_root: btree_root,
            }
        }
    }
}


// Insert must return the new root.
// When searching the tree, note whether at least one page had RC >= 2. If so, reallocate + copy all pages on the path.


impl<'env> MutTxn<'env> {
    pub fn root_db(&self) -> Db {
        self.root_db_()
    }
    pub fn commit(self) -> Result<(), transaction::Error> {
        let extra = self.btree_root.to_le();
        self.txn.commit(&[extra])
    }
    pub fn create_db(&mut self) -> Db {
        let mut btree = self.alloc_page();
        btree.init();
        Db { root: btree.page_offset() }
        // root_offset = off;
    }
    pub fn open_db(&self, key: &[u8]) -> Option<Db> {
        self.open_db_(key)
    }
    pub fn put_db<R:Rng>(&mut self, rng:&mut R, db: Db, key: &[u8], value: Db) -> Db {
        let mut val: [u8; 8] = [0; 8];
        unsafe {
            *(val.as_mut_ptr() as *mut u64) = value.root.to_le();
        }
        std::mem::forget(value); // No need to decrease the RC for that page.
        self.put(rng, db, key, &val)
    }
    pub fn set_global_root(&mut self, db: Db) {
        self.btree_root = db.root
    }
    pub fn put<R:Rng>(&mut self, r:&mut R, db: Db, key: &[u8], value: &[u8]) -> Db {
        put::put(r, self, db, key, value)
    }
    pub fn del(&mut self, db: Db, key: &[u8], value: Option<&[u8]>) -> Db {
        put::del(self, db, key, value)
    }
    pub fn get<'a>(&'a self, db: &Db, key: &[u8], value: Option<&[u8]>) -> Option<Value<'a,Self>> {
        self.get_(db, key, value).map(|x| Value { txn:self, value:x })
    }

    pub fn iterate<'a, F: Fn(&'a [u8], Value<'a,Self>) -> bool + Copy>(&'a self,
                                                                       db: Db,
                                                                       key: &[u8],
                                                                       value: Option<&[u8]>,
                                                                       f: F) {
        unimplemented!()
    }
}

impl<'env> Txn<'env> {
    pub fn root_db(&self) -> Db {
        self.root_db_()
    }
    pub fn get<'a>(&'a self, db: &Db, key: &[u8], value: Option<&[u8]>) -> Option<Value<'a,Self>> {
        self.get_(db, key, value).map(|x| Value { txn:self, value:x })
    }
    pub fn open_db<'a>(&'a self, key: &[u8]) -> Option<Db> {
        self.open_db_(key)
    }
    pub fn iterate<'a, F: Fn(&'a [u8], Value<'a,Self>) -> bool + Copy>(&'a self,
                                                                 db: Db,
                                                                 key: &[u8],
                                                                 value: Option<&[u8]>,
                                                                 f: F) {
        unimplemented!()
    }
}


#[test]
fn basic_test() -> ()
{
    extern crate tempdir;
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path()).unwrap();
    let mut txn = env.mut_txn_begin();
    let mut root = txn.root_db();
    root = txn.put(root,b"test key", b"test value");
    txn.set_global_root(root);
    txn.commit().unwrap();

    let txn = env.txn_begin();
    let root = txn.root_db();
    assert!(txn.get(&root, b"test key", None).map(|x| x.as_slice()) == Some(b"test value"))
}
