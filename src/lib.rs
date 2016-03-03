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
//! - error handling (easy)
//!
//! - delete (half-easy)
//!
//! - dynamic loading of pages not in the map (in file 'transaction.rs', half-easy)
//!
//! - several databases (hard)
//!
//! - reference counting (half-easy)
//!
//! # Example
//!
//! ```
//! use sanakirja::Env;
//! let env=Env::new("/tmp/test").unwrap();
//! let mut txn=env.mut_txn_begin();
//! txn.put(b"test key", b"test value");
//! txn.commit().unwrap();
//!
//! let txn=env.txn_begin();
//! assert!(txn.get(b"test key",None) == Some(b"test value"))
//! ```
//!


extern crate libc;
use libc::c_int;

#[macro_use]
extern crate log;
extern crate fs2;

use std::path::Path;
use std::ptr::copy_nonoverlapping;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufWriter, Write};

mod memmap;
mod transaction;

pub use transaction::Statistics;
use transaction::{PAGE_SIZE, PAGE_SIZE_64};
use std::collections::{HashSet};

/// Mutable transaction
pub struct MutTxn<'env> {
    txn: transaction::MutTxn<'env>,
    btree_root: u64
}

/// Immutable transaction
pub struct Txn<'env> {
    txn: transaction::Txn<'env>,
    btree_root: u64
}

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
                btree_root: u64::from_le(*p_extra)
            }
        }
    }

    /// Start a mutable transaction.
    pub fn mut_txn_begin<'env>(&'env self) -> MutTxn<'env> {
        unsafe {
            let mut txn=self.env.mut_txn_begin();
            let p_extra = self.env.extra() as *const u64;
            let btree_root = u64::from_le(*p_extra);
            let btree_root = if btree_root == 0 {
                let p=txn.alloc_page().unwrap();
                p.offset
            } else {
                btree_root
            };
            MutTxn {
                txn: txn,
                btree_root: btree_root
            }
        }
    }
}

// Difference between mutpage and mutpages: mutpages might also contain just one page, but it is unmapped whenever it goes out of scope, whereas P belongs to the main map. Useful for 32-bits platforms.


#[derive(Debug)]
struct MutPage {
    page: transaction::MutPage,
}
#[derive(Debug)]
struct Page {
    page: transaction::Page,
}

const MAX_KEY_SIZE: usize = PAGE_SIZE >> 2;
const VALUE_SIZE_THRESHOLD: usize = PAGE_SIZE >> 2;

fn value_record_size(key: &[u8], value: Value) -> u16 {
    match value {
        Value::S(s) if s.len() < VALUE_SIZE_THRESHOLD => {
            let size = 28 + key.len() as u16 + value.len() as u16;
            size + ((8 - (size & 7)) & 7)
        }
        Value::S(_) | Value::O{..} => {
            let size = 28 + key.len() as u16 + 8;
            size + ((8 - (size & 7)) & 7)
        }
    }
}


// Page layout: Starts with a header of 24 bytes.
// - 64 bits: RC
// - 16 bits: offset of the first free spot, from the byte before
// - 16 bits: offset of the root of the tree, from the byte before
// - 16 bits: how much space is occupied in this page? (controls compaction)
// - 16 bits: padding
// - beginning of coding space (different encodings in B-nodes and B-leaves)


trait P {
    /// offset of the page in the file.
    fn page_offset(&self) -> u64;

    /// pointer to the first word of the page.
    fn data(&self) -> *const u64;

    /// 0 if cannot alloc, valid offset else (offset in bytes from the byte before the coding section).
    fn can_alloc(&self, size: u16) -> u16 {
        unsafe {
            assert!(size & 7 == 0); // 64 bits aligned.
            let first_free = self.first_free();

            let next_page = (self.data() as *mut u8).offset(PAGE_SIZE as isize) as *const u8;
            let current = (self.data() as *const u8).offset(15 + first_free as isize);
            if current.offset(size as isize) <= next_page {
                first_free
            } else {
                0
            }
        }
    }

    // Value of the page's reference counter.
    fn rc(&self) -> u64 {
        unsafe { u64::from_le(*(self.data())) }
    }

    // First free spot in this page (head of the linked list, number of |u32| from the last glue.
    fn first_free(&self) -> u16 {
        unsafe {
            let first_free = u16::from_le(*(self.p_first_free()));
            let first_free = if first_free > 0 {
                first_free
            } else {
                1
            };
            first_free
        }
    }
    fn p_first_free(&self) -> *mut u16 {
        unsafe { self.data().offset(1) as *mut u16 }
    }

    fn root(&self) -> u16 {
        unsafe {
            let p_root = (self.data() as *const u16).offset(5);
            u16::from_le(*p_root)
        }
    }
    fn set_root(&self, root: u16) {
        unsafe {
            let p_root = (self.data() as *mut u16).offset(5);
            *p_root = root.to_le()
        }
    }
    // Amount of space occupied in the page
    fn occupied_space(&self) -> u16 {
        unsafe {
            let p_occ = (self.data() as *const u16).offset(6);
            u16::from_le(*p_occ)
        }
    }

    // offset in u32.
    fn offset(&self, off: isize) -> *mut u8 {
        unsafe {
            let p = self.data() as *mut u8;
            p.offset(15 + off)
        }
    }
}

impl P for Cow {
    fn data(&self) -> *const u64 {
        let Cow(ref s) = *self;
        match s {
            &transaction::Cow::Page(ref p) => p.data as *const u64,
            &transaction::Cow::MutPage(ref p) => p.data as *const u64,
        }
    }
    fn page_offset(&self) -> u64 {
        let Cow(ref s) = *self;
        match s {
            &transaction::Cow::Page(ref p) => p.offset,
            &transaction::Cow::MutPage(ref p) => p.offset,
        }
    }
}

impl P for Page {
    fn page_offset(&self) -> u64 {
        self.page.offset
    }
    fn data(&self) -> *const u64 {
        self.page.data as *mut u64
    }
}

impl P for MutPage {
    fn page_offset(&self) -> u64 {
        self.page.offset
    }
    fn data(&self) -> *const u64 {
        self.page.data as *mut u64
    }
}


impl MutPage {
    fn init(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.page.data as *mut u8, 0, 16);
            self.incr_rc()
        }
    }
    fn incr_rc(&mut self) {
        unsafe {
            *(self.page.data as *mut u64) = (self.rc()+1).to_le();
        }
    }

    /// Takes a size in bytes, returns an offset from the word before
    /// the beginning of the contents (0 is invalid, 1 is the first
    /// offset).
    fn alloc(&mut self, first_free: u16, size: u16) {
        unsafe {
            debug_assert!(size & 7 == 0); // 32 bits aligned.
            *(self.p_first_free()) = (first_free + size).to_le();
        }
    }

    // Layout of a node: 24 + |key|+|value|, rounded up to 64-bits.
    // - 64 bits: left, little endian. if the first 32 bits == 1, local offset, else global in bytes.
    // - 64 bits: right, little endian. if the first 32 bits == 1, local offset, else global in bytes.
    // - 32 bits: value length, if >PAGE_SIZE/4, the value is a 64-bits offset of a page.
    // - 16 bits: key length
    // - 16 bits: cardinal, = 1+sum of children in the same page
    // - value
    // - key
    // - padding for 64 bits/8 bytes alignment.

    // allocate and write key, value, left and right neighbors.
    fn alloc_key_value(&mut self,
                       off_ptr: u16,
                       size: u16,
                       key: &[u8],
                       value: Value,
                       l: u64,
                       r: u64) {
        unsafe {
            self.alloc(off_ptr, size);
            // println!("off_ptr={:?}, size = {:?}",off_ptr, size);
            // off is the beginning of a free zone. Write the node there.
            // ///////////////////////////////////////////////
            let ptr = self.offset(off_ptr as isize) as *mut u32;
            // println!("ptr: {} {:?}",off_ptr,ptr0);
            // This is a leaf, so l and r are offsets in the file, not local offsets.
            let ptr = ptr as *mut u64;
            *ptr = l.to_le();
            *(ptr.offset(1)) = r.to_le();
            let ptr = ptr as *mut u32;
            *(ptr.offset(4)) = (value.len() as u32).to_le();

            let ptr = ptr as *mut u16;
            *(ptr.offset(10)) = (key.len() as u16).to_le();
            *(ptr.offset(11)) = 1;
            // +(if l!=0 { 1 } else { 0 } + if r!=0 { 1 } else { 0 } as u32).to_le(); // balance number
            // println!("alloc_key_value: copying {:?} {:?} to {:?}",key,value,ptr);
            match value {
                Value::S(value) => {
                    let ptr = ptr as *mut u8;
                    let ptr = ptr.offset(24);
                    copy_nonoverlapping(value.as_ptr(), ptr, value.len());
                    copy_nonoverlapping(key.as_ptr(), ptr.offset(value.len() as isize), key.len());
                }
                Value::O{offset,..} => {
                    debug_assert!(offset != 0);
                    *((ptr as *mut u64).offset(3)) = offset.to_le();
                    let ptr = ptr as *mut u8;
                    copy_nonoverlapping(key.as_ptr(), ptr.offset(32), key.len());
                }
            }
        }
    }
}




fn read_key_value<'a>(p: &'a u8) -> (&'a [u8], Value) {
    unsafe {
        let p32 = p as *const u8 as *const u32;
        let key_len = u16::from_le(*(p32.offset(5) as *const u16));
        let val_len = u32::from_le(*(p32.offset(4)));
        if (val_len as usize) < VALUE_SIZE_THRESHOLD {
            (std::slice::from_raw_parts((p as *const u8).offset(24 + val_len as isize),
                                        key_len as usize),
             Value::S(std::slice::from_raw_parts((p as *const u8).offset(24), val_len as usize)))
        } else {
            (std::slice::from_raw_parts((p as *const u8).offset(32), key_len as usize),
             {
                let offset = u64::from_le(*((p32 as *const u64).offset(3)));
                Value::O {
                    offset: offset,
                    len: val_len,
                }
            })
        }
    }
}

#[derive(Debug)]
struct Cow(transaction::Cow);

impl Cow {
    // fn from_mut_page(p:MutPage)->Cow {
    // Cow(transaction::Cow::MutPage(p.page))
    // }
    // fn is_mutable(&self)->bool {
    // let &Cow(ref s)=self;
    // match s { &transaction::Cow::MutPage(_)=>true, _=>false }
    // }
    //
    fn into_mut_page(self, txn: &mut MutTxn) -> MutPage {
        let Cow(s) = self;
        match s {
            transaction::Cow::MutPage(p) => MutPage { page: p },
            transaction::Cow::Page(p) => {
                unsafe {
                    let result = txn.txn.alloc_page().unwrap();
                    copy_nonoverlapping(p.data, result.data, PAGE_SIZE);
                    // TODO: decrement and check RC
                    p.free(&mut txn.txn);
                    MutPage { page: result }
                }
            }
        }
    }
    // fn into_page(self)->Page {
    // let Cow(s)=self;
    // match s {
    // transaction::Cow::Page(p)=> Page { page:p },
    // transaction::Cow::MutPage(p)=> Page { page:p.into_page() }
    // }
    // }
    //
}

pub enum Loaded<'a> {
    Map {
        map: *mut u8,
        len: u64,
        contents: &'a [u8],
    },
    S(&'a [u8]),
}

impl<'a> Loaded<'a> {
    fn contents(&self) -> &'a [u8] {
        match self {
            &Loaded::S(s) => s,
            &Loaded::Map{contents,..} => contents,
        }
    }
    fn len(&self) -> usize {
        match self {
            &Loaded::S(s) => s.len(),
            &Loaded::Map{contents,..} => contents.len(),
        }
    }
}

impl<'a> Drop for Loaded<'a> {
    fn drop(&mut self) {
        match self {
            &mut Loaded::Map{map,len,..} => unsafe { memmap::munmap(map, len) },
            _ => {}
        }
    }
}

trait LoadPage {
    fn fd(&self) -> c_int;
    fn length(&self) -> u64;
    fn root_db_(&self)->Db;
    fn open_db_<'a>(&'a self, key: &[u8]) -> Option<Db> {
        let db = self.get_(self.root_db_(),key,None);
        if let Some(Value::S(db)) = db {
            unsafe {
                Some(Db { root: u64::from_le(*(db.as_ptr() as *const u64)) })
            }
        } else {
            None
        }
    }


    fn load_page(&self, off: u64) -> Page;
    fn load_value<'a>(&self, value: &Value<'a>) -> Loaded<'a> {
        match *value {
            Value::S(s) => Loaded::S(s),
            Value::O{offset,len,..} => unsafe {
                debug!("load_value {:?}", value);
                let page = memmap::mmap(self.fd(), None, offset, PAGE_SIZE_64);
                let mut total = PAGE_SIZE as isize;
                let mut cur = page as *const u64;
                debug!("pages, cur:{:?}", cur);
                while *cur != 0 {
                    debug!("page:{:?}, cur:{:?} {:?}", page, *cur, total);
                    let result = memmap::mmap(self.fd(),
                                              Some(page.offset(total)),
                                              *cur,
                                              PAGE_SIZE_64);
                    debug!("result={:?}, asked {:?}", result, page.offset(total));
                    assert!(result == page.offset(total));
                    total += PAGE_SIZE as isize;
                    cur = cur.offset(1)
                }
                Loaded::Map {
                    map: page,
                    len: total as u64,
                    contents: std::slice::from_raw_parts(cur.offset(1) as *const u8, len as usize),
                }
            },
        }
    }
    fn get_<'a>(&'a self, db:Db, key: &[u8], value: Option<&[u8]>) -> Option<Value<'a>> {
        let root_page=self.load_page(db.root);
        self.binary_tree_get(&root_page, key, value, root_page.root() as u32)
    }

    // non tail-rec version
    fn binary_tree_get<'a>(&self,
                           page: &Page,
                           key: &[u8],
                           value: Option<&[u8]>,
                           current: u32)
                           -> Option<Value<'a>> {
        unsafe {
            debug!("binary_tree_get:{:?}", page);
            let ptr = page.offset(current as isize) as *mut u32;

            let (key0, value0) = read_key_value(&*(ptr as *const u8));
            let cmp = if let Some(value_) = value {
                let cmp = key.cmp(&key0);
                if cmp == Ordering::Equal {
                    let value0 = self.load_value(&value0);
                    value_.cmp(value0.contents())
                } else {
                    cmp
                }
            } else {
                key.cmp(&key0)
            };
            // debug!("({:?},{:?}), {:?}, ({:?},{:?})",
            // std::str::from_utf8_unchecked(key),
            // std::str::from_utf8_unchecked(t.load_value(value_)),
            // cmp,
            // std::str::from_utf8_unchecked(key0),
            // std::str::from_utf8_unchecked(t.load_value(value0)));
            //
            match cmp {
                Ordering::Equal | Ordering::Less => {
                    let result = {
                        let left0 = u32::from_le(*(ptr as *const u32));
                        if left0 == 1 {
                            let next = u32::from_le(*(ptr.offset(1)));
                            if next == 0 {
                                None
                            } else {
                                self.binary_tree_get(page, key, value, next)
                            }
                        } else {
                            // Global offset
                            let left = u64::from_le(*(ptr as *const u64));
                            if left == 0 {
                                None
                            } else {
                                // left child is another page.
                                let page_ = self.load_page(left);
                                let root_ = page_.root();
                                self.binary_tree_get(&page_, key, value, root_ as u32)
                            }
                        }
                    };
                    if cmp == Ordering::Equal {
                        Some(value0)
                    } else {
                        result
                    }
                }
                Ordering::Greater => {
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    debug!("right0={:?}", right0);
                    if right0 == 1 {
                        let next = u32::from_le(*(ptr.offset(3)));
                        if next == 0 {
                            None
                        } else {
                            self.binary_tree_get(page, key, value, next)
                        }
                    } else {
                        // global offset, follow
                        let right = u64::from_le(*((ptr as *const u64).offset(1)));
                        if right == 0 {
                            None
                        } else {
                            // right child is another page
                            let page_ = self.load_page(right);
                            let root_ = page_.root();
                            self.binary_tree_get(&page_, key, value, root_ as u32)
                        }
                    }
                }
            }
        }
    }





    fn tree_iterate<'a,F: Fn(&'a [u8], &'a [u8]) -> bool + Copy>(&'a self,
                                                                 page: &Page,
                                                                 key: &[u8],
                                                                 value: Option<&[u8]>,
                                                                 f: F,
                                                                 current: u32,
                                                                 started: bool)
                                                                 -> Option<bool> {
        unsafe {
            debug!("binary_tree_get:{:?}", page);
            let ptr = page.offset(current as isize) as *mut u32;

            let value_ = value.unwrap_or(b"");
            let (key0, value0) = read_key_value(&*(ptr as *const u8));
            let mut value0_loaded = None;
            let cmp = {
                let cmp = key.cmp(&key0);
                if cmp == Ordering::Equal {
                    if let Some(value) = value {
                        value0_loaded = Some(self.load_value(&value0));
                        let cont = value0_loaded.as_ref().unwrap();
                        value.cmp(cont.contents())
                    } else {
                        cmp
                    }
                } else {
                    cmp
                }
            };
            debug!("({:?},{:?}), {:?}, ({:?},{:?})",
                   std::str::from_utf8_unchecked(key),
                   std::str::from_utf8_unchecked(value_),
                   cmp,
                   std::str::from_utf8_unchecked(key0),
                   std::str::from_utf8_unchecked(self.load_value(&value0).contents()));

            // If we've already started iterating, or else if the key can be found on our left.
            let result_left = if started ||
                (!started && (cmp == Ordering::Equal || cmp == Ordering::Less)) {
                    let result = {
                        let left0 = u32::from_le(*(ptr as *const u32));
                        if left0 == 1 {
                            let next = u32::from_le(*(ptr.offset(1)));
                            if next == 0 {
                                None
                            } else {
                                self.tree_iterate(page, key, value, f, next, started)
                            }
                        } else {
                            // Global offset
                            let left = u64::from_le(*(ptr as *const u64));
                            if left == 0 {
                                None
                            } else {
                                // left child is another page.
                                let page_ = self.load_page(left);
                                let root_ = page_.root();
                                self.tree_iterate(&page_, key, value, f, root_ as u32, started)
                            }
                        }
                    };
                    match result {
                        Some(true) => {
                            let value0 = if let Some(value0) = value0_loaded {
                                value0
                            } else {
                                value0_loaded = Some(self.load_value(&value0));
                                value0_loaded.unwrap()
                            };
                            Some(f(key0, value0.contents()))
                        }
                        None if cmp == Ordering::Equal => {
                            let value0 = if let Some(value0) = value0_loaded {
                                value0
                            } else {
                                value0_loaded = Some(self.load_value(&value0));
                                value0_loaded.unwrap()
                            };
                            Some(f(key0, value0.contents()))
                        }
                        _ => result, // we've stopped already
                    }
                } else {
                    None
                };


            if result_left == Some(false) {
                Some(false)
            } else {
                if (result_left.is_none() && cmp == Ordering::Greater) || result_left.is_some() {
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    if right0 == 1 {
                        let next = u32::from_le(*(ptr.offset(3)));
                        if next == 0 {
                            None
                        } else {
                            self.tree_iterate(page,
                                         key,
                                         value,
                                         f,
                                         next,
                                         started || result_left.is_some())
                        }
                    } else {
                        // global offset, follow
                        let right = u64::from_le(*((ptr as *const u64).offset(1)));
                        if right == 0 {
                            None
                        } else {
                            // right child is another page
                            let page_ = self.load_page(right);
                            let root_ = page_.root();
                            self.tree_iterate(&page_,
                                         key,
                                         value,
                                         f,
                                         root_ as u32,
                                         started || result_left.is_some())
                        }
                    }
                } else {
                    result_left
                }
            }
        }
    }









}

impl<'env> LoadPage for MutTxn<'env> {
    fn length(&self) -> u64 {
        self.txn.env.length
    }
    fn root_db_(&self)->Db {
        Db { root:self.btree_root }
    }
    fn fd(&self) -> c_int {
        self.txn.env.fd
    }
    fn load_page(&self, off: u64) -> Page {
        Page { page: self.txn.load_page(off) }
    }
}
impl<'env> LoadPage for Txn<'env> {
    fn length(&self) -> u64 {
        self.txn.env.length
    }
    fn root_db_(&self)->Db {
        Db { root:self.btree_root }
    }
    fn fd(&self) -> c_int {
        self.txn.env.fd
    }
    fn load_page(&self, off: u64) -> Page {
        Page { page: self.txn.load_page(off) }
    }
}

#[derive(Debug,Clone,Copy)]
pub enum Value<'a> {
    S(&'a [u8]),
    O {
        offset: u64,
        len: u32,
    },
}

impl<'a> Value<'a> {
    pub fn len(&self) -> u32 {
        match self {
            &Value::S(s) => s.len() as u32,
            &Value::O{len,..} => len,
        }
    }
    pub fn as_slice(&self) -> &'a [u8] {
        match self {
            &Value::S(ref s)=>s,
            &Value::O{..}=>unimplemented!()
        }
    }
}

// Insert must return the new root.
// When searching the tree, note whether at least one page had RC >= 2. If so, reallocate + copy all pages on the path.
#[derive(Debug,Clone,Copy)]
pub struct Db { root:u64 }

impl<'env> MutTxn<'env> {
    pub fn root_db(&self)->Db {
        self.root_db_()
    }
    pub fn commit(self) -> Result<(), transaction::Error> {
        let extra = self.btree_root.to_le();
        self.txn.commit(&[extra])
    }
    pub fn load<'a>(&self, value: &Value<'a>) -> Loaded<'a> {
        self.load_value(value)
    }
    pub fn create_db(&mut self)->Db {
        let mut btree = self.alloc_page();
        btree.init();
        Db { root:btree.page_offset() }
        //root_offset = off;
    }
    pub fn open_db<'a>(&'a self, key: &[u8]) -> Option<Db> {
        self.open_db_(key)
    }
    pub fn put_db(&mut self,db:Db,key:&[u8],value:Db)->Db {
        let mut val:[u8;8]=[0;8];
        unsafe {
            *(val.as_mut_ptr() as *mut u64) = value.root.to_le()
        }
        self.put(db,key,&val)
    }
    pub fn set_global_root(&mut self,db:Db) {
        self.btree_root = db.root
    }
    pub fn put(&mut self, db:Db, key: &[u8], value: &[u8])->Db {
        assert!(key.len() < MAX_KEY_SIZE);
        let root_page = Cow(self.txn.load_cow_page(db.root));
        let put_result=
            self.insert(root_page,
                        key,
                        Value::S(value),
                        0,
                        0,
                        1);
            /*

        let put_result = if let Some(root) = self.load_cow_root() {
            //root_offset = root.page_offset();
            debug!("put root = {:?}", root.page_offset());
            let rc=root.rc();
            self.insert(root, key, Value::S(value), 0, 0, rc)
        } else {
            debug!("put:no root");
        };
         */
        match put_result {
            Insert::Split { key:key0,value:value0,left:l,right:r,free_page:fr } => {
                /*unsafe {
                let key0=std::str::from_utf8_unchecked(&key0[..]);
                let value0=std::str::from_utf8_unchecked(&value0[..]);
                //println!("split root on {:?}",(key0,value0,l,r));
            }*/
                // the root page has split, we need to allocate a new one.
                let mut btree = self.alloc_page();
                debug!("new root page:{:?}",btree);
                btree.init();
                let btree_off=btree.page_offset();
                //self.btree_root = off;

                let size=value_record_size(key0,value0);
                let off=btree.can_alloc(size);
                debug_assert!(off>0);
                btree.alloc_key_value(off,size,key0,value0,l,r);
                if fr>0 {
                    unsafe { transaction::free(&mut self.txn, fr) }
                }
                btree.set_root(off);
                Db { root:btree_off }
            },
            Insert::Ok { page,.. } => {
                Db { root:page.page_offset() }
            }
        }
    }
    fn load_cow_page(&mut self, off: u64) -> Cow {
        Cow(self.txn.load_cow_page(off))
    }

    fn alloc_page(&mut self) -> MutPage {
        let page = self.txn.alloc_page().unwrap();
        MutPage { page: page }
    }

    fn alloc_pages(&mut self, value: &[u8]) -> u64 {
        unsafe {
            // n*PAGE_SIZE - 8 * n
            let actual_page_size = PAGE_SIZE - 8;

            let n = value.len() / actual_page_size;
            let n = if n * actual_page_size < value.len() {
                n + 1
            } else {
                n
            };
            assert!(8 * (n + 1) < PAGE_SIZE);

            let first_page = self.alloc_page();
            let mut page_ptr = first_page.data() as *mut u64;

            let copyable_len = if value.len() < PAGE_SIZE - 8 * n {
                value.len()
            } else {
                PAGE_SIZE - 8 * n
            };
            copy_nonoverlapping(value.as_ptr(),
                                (first_page.data() as *mut u8).offset(8 * n as isize),
                                copyable_len);
            let mut value_offset = copyable_len;

            let mut total_length = PAGE_SIZE;

            while total_length < 8 * n + value.len() {
                let page = self.alloc_page();
                *page_ptr = page.page_offset().to_le();
                page_ptr = page_ptr.offset(1);

                let copyable_len = if value.len() - value_offset < PAGE_SIZE {
                    value.len() - value_offset
                } else {
                    PAGE_SIZE
                };
                copy_nonoverlapping(value.as_ptr().offset(value_offset as isize),
                                    page.data() as *mut u8,
                                    copyable_len);
                value_offset += copyable_len;
                total_length += PAGE_SIZE
            }
            *page_ptr = 0;
            first_page.page_offset()
        }
    }

    fn alloc_value<'a>(&mut self, value: Value<'a>) -> Value<'a> {
        match value {
            Value::S(s) if s.len() < VALUE_SIZE_THRESHOLD => value,
            Value::O{..} => value,
            Value::S(s) => {
                let off = self.alloc_pages(s);
                Value::O {
                    offset: off,
                    len: s.len() as u32,
                }
            }
        }
    }

    // Finds binary tree root and calls binary_tree_insert on it.
    fn insert<'a>(&mut self,
                  page: Cow,
                  key: &[u8],
                  value: Value<'a>,
                  l: u64,
                  r: u64,
                  max_rc:u64)
                  -> Insert<'a> { // [u8], Value<'a>, u64, u64, u64)> {
        let root = page.root();
        debug!("insert: root={:?}, {:?},{:?}", root, key, value);
        if root == 0 {
            let mut page = page.into_mut_page(self);
            let size = value_record_size(key, value);
            let off = page.can_alloc(size);
            debug_assert!(off > 0);

            let value = self.alloc_value(value);
            page.alloc_key_value(off, size, key, value, l, r);
            debug!("inserted {}", off);
            page.set_root(off);
            debug!("root set 0");
            Insert::Ok { page:page, off:off }
        } else {
            let rc = std::cmp::max(page.rc(),max_rc);
            let result = self.binary_tree_insert(page, key, value, l, r, rc, 0, 0, root as u32);
            debug!("result {:?}", result);
            match result {
                Insert::Ok{page,off} => {
                    page.set_root(off as u16);
                    // unsafe {
                    // let ptr=page.offset(root);
                    // incr(ptr.offset(6));
                    // }
                    debug!("root set");
                    Insert::Ok { page:page,off:off }
                },
                result => result
            }
        }
    }


    // Returns None if the changes have been done in one of the children of "page", Some(Insert::Ok(..)) if "page" is a B-leaf or a B-node and we inserted something in it, and Some(Insert::Split(...)) if page was split.
    fn binary_tree_insert<'a>(&mut self,
                              page: Cow,
                              key: &[u8],
                              value: Value<'a>,
                              l: u64,
                              r: u64,
                              max_rc:u64,
                              depth: usize,
                              path: u64,
                              current: u32)
                              -> Insert<'a> {
        unsafe {
            debug!("binary tree insert:{} {}", depth, path);
            unsafe fn node_ptr(page: &MutPage,
                               mut length: usize,
                               mut path: u64,
                               mut current: u32)
                               -> u16 {
                while length > 0 {
                    let ptr = page.offset(current as isize) as *mut u32;
                    // println!("node_ptr:{:?}",if path&1==0 { u32::from_le(*ptr) } else { u32::from_le(*(ptr.offset(2))) });
                    // assert!(if path&1==0 { u32::from_le(*ptr)==1 } else { u32::from_le(*(ptr.offset(2))) == 1 });
                    current = if path & 1 == 0 {
                        u32::from_le(*(ptr.offset(1)))
                    } else {
                        u32::from_le(*(ptr.offset(3)))
                    };
                    length -= 1;
                    path >>= 1;
                }
                current as u16
            }
            let ptr = page.offset(current as isize) as *mut u32;
            // Inlining this closure takes the whole thing from 2.33 to 1.7 (ratio (sanakirja put time)/(lmdb put time)).
            let continue_local = |txn: &mut MutTxn,
                                  page: Cow,
                                  side_offset: isize,
                                  next_path: u64|
                                  -> Insert<'a> {
                let next = u32::from_le(*(ptr.offset(side_offset + 1)));
                if next == 0 {
                    // free branch.
                    let size = value_record_size(key, value);
                    debug!("size={:?}", size);
                    let off_ptr = page.can_alloc(size);
                    if off_ptr > 0 {
                        let mut page = page.into_mut_page(txn);
                        let value = txn.alloc_value(value);
                        debug!("continue_local, value={:?}", value);
                        page.alloc_key_value(off_ptr, size, key, value, l, r);
                        let current = node_ptr(&page, depth, path, page.root() as u32);
                        let ptr = page.offset(current as isize);
                        *((ptr as *mut u32).offset(side_offset)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(side_offset + 1)) = (off_ptr as u32).to_le();
                        incr((ptr as *mut u16).offset(11));
                        Insert::Ok {
                            off: rebalance(&mut page, current),
                            page: page,
                        }
                    } else {
                        // No more space in this page
                        txn.split_and_insert(&page, key, value, l, r, 0)
                    }
                } else {
                    let result = txn.binary_tree_insert(page,
                                                        key,
                                                        value,
                                                        l,
                                                        r,
                                                        max_rc,
                                                        depth + 1,
                                                        next_path,
                                                        next);
                    if let Insert::Ok{off,mut page} = result {
                        let current = node_ptr(&page, depth, path, page.root() as u32);
                        let ptr = page.offset(current as isize);
                        *((ptr as *mut u32).offset(side_offset)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(side_offset + 1)) = (off as u32).to_le();
                        incr((ptr as *mut u16).offset(11));
                        Insert::Ok {
                            off: rebalance(&mut page, current),
                            page: page,
                        }
                    } else {
                        result
                    }
                }
            };

            let continue_global = |txn: &mut MutTxn, page: Cow, right_child: bool| {
                debug!("continue_global");
                // Global offset
                let child_ptr = if right_child {
                    (ptr as *const u64).offset(1)
                } else {
                    ptr as *const u64
                };
                let child = u64::from_le(*child_ptr);
                if child == 0 {
                    // free left child.
                    let size = value_record_size(key, value);
                    let off = page.can_alloc(size);
                    if off > 0 {
                        let mut page = page.into_mut_page(txn);
                        let value = txn.alloc_value(value);
                        page.alloc_key_value(off, size, key, value, l, r);
                        // Either there's room
                        let current = node_ptr(&page, depth, path, page.root() as u32);
                        let ptr = page.offset(current as isize);
                        // page was mutable and has not been split. We can insert!
                        if right_child {
                            *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                            *((ptr as *mut u32).offset(3)) = (off as u32).to_le();
                        } else {
                            *((ptr as *mut u32).offset(0)) = (1 as u32).to_le();
                            *((ptr as *mut u32).offset(1)) = (off as u32).to_le();
                        }
                        incr((ptr as *mut u16).offset(11));
                        Insert::Ok {
                            off: rebalance(&mut page, current),
                            page: page,
                        }
                    } else {
                        // println!("page cannot allocate");
                        txn.split_and_insert(&page, key, value, l, r, 0)
                    }
                } else {
                    let page_ = txn.load_cow_page(child);
                    let max_rc = std::cmp::max(max_rc,page_.rc());
                    let result = txn.insert(page_, key, value, l, r, max_rc);
                    if let Insert::Split { key:k0,value:v0,left:l0,right:r0,free_page:fr0 } = result {
                        let size = value_record_size(k0, v0);
                        let off = page.can_alloc(size);
                        if off > 0 {
                            let mut page = page.into_mut_page(txn);
                            // page_ split, we need to insert the resulting key here.
                            page.alloc_key_value(off, size, k0, v0, l0, r0);
                            // Either there's room
                            let current = node_ptr(&page, depth, path, page.root() as u32);
                            let ptr = page.offset(current as isize);
                            // Either there's room for it.
                            if right_child {
                                *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                                *((ptr as *mut u32).offset(3)) = (off as u32).to_le();
                            } else {
                                *((ptr as *mut u32).offset(0)) = (1 as u32).to_le();
                                *((ptr as *mut u32).offset(1)) = (off as u32).to_le();
                            }
                            incr((ptr as *mut u16).offset(11));
                            transaction::free(&mut txn.txn, fr0);
                            let bal = rebalance(&mut page, current);
                            Insert::Ok {
                                page: page,
                                off: bal,
                            }
                        } else {
                            // debug!("Could not find space for child pages {} {}",l0,r0);
                            // page_ was split and there is no space here to keep track of its replacement.
                            txn.split_and_insert(&page, k0, v0, l0, r0, fr0)
                        }
                    } else {
                        result
                    }
                }
            };


            // let count = u32::from_le(*(ptr.offset(6)));
            let cmp = {
                let (key0, value0) = read_key_value(&*(ptr as *const u8));
                let cmp = key.cmp(&key0);
                if cmp == Ordering::Equal {
                    let value = self.load_value(&value);
                    let value = value.contents();
                    let value0 = self.load_value(&value0);
                    let value0 = value0.contents();
                    value.cmp(value0)
                } else {
                    cmp
                }
            };
            match cmp {
                Ordering::Less | Ordering::Equal => {
                    let left0 = u32::from_le(*(ptr as *const u32));
                    debug!("left0={:?}", left0);
                    if left0 == 1 {
                        // continue_local(self, page,ptr,1,path,key,value,l,r,depth,path)
                        continue_local(self, page, 0, path)
                    } else {
                        continue_global(self, page, false)
                    }
                }
                _ => {
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    debug!("right0={:?}", right0);
                    if right0 == 1 {
                        let next_path = path | (1 << depth);
                        continue_local(self, page, 2, next_path)
                    } else {
                        continue_global(self, page, true)
                    }
                }
            }
        }
    }


    fn split_and_insert<'a>(&mut self,
                            page: &Cow,
                            k: &[u8],
                            v: Value<'a>,
                            l: u64,
                            r: u64,
                            fr: u64)
                            -> Insert<'a> {
        // fr is the page where k and v live, if they're not from a lifetime larger than self.

        // page.page.free(&mut self.txn);
        // self.debug("/tmp/before_split", 0);
        // println!("split {:?}",page);
        unsafe {
            debug!("split_and_insert: {:?},{:?},{:?}",
                   std::str::from_utf8_unchecked(k),
                   l,
                   r)
        };
        debug!("\n\nsplit page {:?} !\n", page);
        // tree traversal
        fn iter(txn: &mut MutTxn, page: &Cow, dest: &mut MutPage, current: u32) -> u16 {
            unsafe {
                let ptr = page.offset(current as isize);
                let (key, value) = read_key_value(&*(ptr as *const u8));
                // set with lr=00 for now, will update immediately after.
                let size = value_record_size(key, value);
                let dest_off = dest.can_alloc(size);
                debug_assert!(dest_off > 0);
                dest.alloc_key_value(dest_off, size, key, value, 0, 0);
                let dest_ptr: *mut u32 = dest.offset(dest_off as isize) as *mut u32;

                let left0 = u32::from_le(*(ptr as *const u32));
                if left0 == 1 {
                    // local offset, follow
                    let left = u32::from_le(*((ptr as *const u32).offset(1)));
                    *(dest_ptr as *mut u32) = (1 as u32).to_le();
                    let left = iter(txn, page, dest, left);
                    *((dest_ptr as *mut u32).offset(1)) = (left as u32).to_le();
                } else {
                    // global offset, copy
                    let child = u64::from_le(*((ptr as *const u64).offset(1)));
                    *((dest_ptr as *mut u64).offset(1)) = if child != page.page_offset() {
                        *(ptr as *const u64)
                    } else {
                        0
                    }
                }
                let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                if right0 == 1 {
                    // local offset, follow
                    let right = u32::from_le(*((ptr as *const u32).offset(3)));
                    *((dest_ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                    let right = iter(txn, page, dest, right);
                    *((dest_ptr as *mut u32).offset(3)) = (right as u32).to_le();
                } else {
                    // global offset, copy it
                    let child = u64::from_le(*((ptr as *const u64).offset(1)));
                    *((dest_ptr as *mut u64).offset(1)) = if child != page.page_offset() {
                        *((ptr as *const u64).offset(1))
                    } else {
                        0
                    }
                }
                // make counts equal
                let dest_ptr = dest_ptr as *mut u16;
                *dest_ptr.offset(11) = *(ptr as *const u16).offset(11);
                //
                dest_off
            }
        }
        unsafe {

            debug!("page root:{}", page.root());
            let mut left_page = MutPage { page: self.txn.alloc_page().unwrap() };
            let mut right_page = MutPage { page: self.txn.alloc_page().unwrap() };
            debug!("left page: {:?}, right page: {:?}",
                   left_page.page.offset,
                   right_page.page.offset);
            left_page.init();
            right_page.init();

            let ptr_root = page.offset(page.root() as isize) as *mut u32;
            debug!("filling left page");
            {
                let left = u32::from_le(*ptr_root);
                if left == 1 {
                    // local offset
                    let left = u32::from_le(*((ptr_root as *const u32).offset(1)));
                    let left_root = iter(self, page, &mut left_page, left);
                    left_page.set_root(left_root as u16);
                } else {
                    // global offset, the tree is not balanced.
                    //let path = "/tmp/before_split";
                    //self.debug(path, 0);
                    //panic!("not splitting unbalanced tree, dumped into {}", path)
                    unreachable!()
                }
            }
            debug!("filling right page");
            {
                let right = u32::from_le(*(ptr_root.offset(2)));
                if right == 1 {
                    // local offset
                    let right = u32::from_le(*((ptr_root as *const u32).offset(3)));
                    let right_root = iter(self, page, &mut right_page, right);
                    right_page.set_root(right_root as u16);
                } else {
                    // global offset, the tree is not balanced.
                    //let path = "/tmp/before_split";
                    //self.debug(path, 0);
                    //panic!("not splitting unbalanced tree, dumped into {}", path)
                    unreachable!()
                }
            }
            debug!("done filling");
            let (key, value) = read_key_value(&*(ptr_root as *const u8));
            debug!("split_and_insert, reinserting: {:?},{:?},{:?}",
                   std::str::from_utf8_unchecked(k),
                   l,
                   r);
            let left_offset = left_page.page.offset;
            let right_offset = right_page.page.offset;

            let cmp = k.cmp(key);
            let cmp = match cmp {
                Ordering::Less | Ordering::Greater => cmp,
                Ordering::Equal => {
                    let v = self.load_value(&v);
                    let value = self.load_value(&value);
                    v.contents().cmp(&value.contents())
                }
            };
            match cmp {
                Ordering::Less | Ordering::Equal => {
                    let root = left_page.root();
                    let left_page = Cow(transaction::Cow::MutPage(left_page.page));
                    let result = self.binary_tree_insert(left_page, k, v, l, r, 1, 0, 0, root as u32);
                    if let Insert::Ok{page,off} = result {
                        page.set_root(off as u16)
                    } else {
                        panic!("problem left: {:?}", result)
                    }
                }
                _ => {
                    let root = right_page.root();
                    let right_page = Cow(transaction::Cow::MutPage(right_page.page));
                    let result = self.binary_tree_insert(right_page, k, v, l, r, 1, 0, 0, root as u32);
                    if let Insert::Ok{page,off} = result {
                        page.set_root(off as u16)
                    } else {
                        panic!("problem right: {:?}", result)
                    }
                }
            }
            if fr > 0 {
                transaction::free(&mut self.txn, fr)
            }
            Insert::Split {
                key: key,
                value: value,
                left: left_offset,
                right: right_offset,
                free_page: page.page_offset(),
            }
        }
    }

    pub fn get<'a>(&'a self, db:Db, key: &[u8], value: Option<&[u8]>) -> Option<Value<'a>> {
        self.get_(db, key, value)
    }

    pub fn iterate<'a, F: Fn(&'a [u8], &'a [u8]) -> bool + Copy>(&'a self,
                                                                 db:Db,
                                                                 key: &[u8],
                                                                 value: Option<&[u8]>,
                                                                 f: F) {
        let root_page = self.load_page(db.root);
        let root = root_page.root();
        self.tree_iterate(&root_page, key, value, f, root as u32, false);
    }

    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db:Db, p: P) {
        debug(self, db, p)
    }
}

impl<'env> Txn<'env> {
    pub fn root_db(&self)->Db {
        self.root_db_()
    }
    pub fn get<'a>(&'a self, db:Db, key: &[u8], value: Option<&[u8]>) -> Option<Value<'a>> {
        self.get_(db, key, value)
    }
    pub fn open_db<'a>(&'a self, key: &[u8]) -> Option<Db> {
        self.open_db_(key)
    }
    pub fn iterate<'a, F: Fn(&'a [u8], &'a [u8]) -> bool + Copy>(&'a self,
                                                                 db:Db,
                                                                 key: &[u8],
                                                                 value: Option<&[u8]>,
                                                                 f: F) {
        let root_page = self.load_page(db.root);
        let root = root_page.root();
        self.tree_iterate(&root_page, key, value, f, root as u32, false);
    }

    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db:Db, p: P) {
        debug(self, db, p)
    }
}

fn debug<P: AsRef<Path>, T: LoadPage>(t: &T, db:Db, p: P) {
    let page = t.load_page(db.root);
    let f = File::create(p.as_ref()).unwrap();
    let mut buf = BufWriter::new(f);
    writeln!(&mut buf, "digraph{{").unwrap();
    let mut h = HashSet::new();
    fn print_page<T: LoadPage>(txn: &T,
                               pages: &mut HashSet<u64>,
                               buf: &mut BufWriter<File>,
                               p: &Page,
                               print_children: bool) {
        if !pages.contains(&p.page.offset) {
            pages.insert(p.page.offset);
            if print_children {
                writeln!(buf,
                         "subgraph cluster{} {{\nlabel=\"Page {}\";\ncolor=black;",
                         p.page.offset,
                         p.page.offset)
                    .unwrap();
            }
            let root = p.root();
            debug!("page root:{:?}", root);
            let mut h = Vec::new();
            let mut edges = Vec::new();
            let mut hh = HashSet::new();
            print_tree(txn, &mut hh, buf, &mut edges, &mut h, p, root as u32);
            if print_children {
                writeln!(buf, "}}").unwrap();
            }
            for p in edges.iter() {
                writeln!(buf, "{}", p).unwrap()
            }
            if print_children {
                for p in h.iter() {
                    print_page(txn, pages, buf, p, true)
                }
            }
        }
    }

    fn print_tree<T: LoadPage>(txn: &T,
                               nodes: &mut HashSet<u32>,
                               buf: &mut BufWriter<File>,
                               edges: &mut Vec<String>,
                               pages: &mut Vec<Page>,
                               p: &Page,
                               off: u32) {
        unsafe {
            // println!("print tree:{:?}",off);
            let ptr = p.offset(off as isize) as *const u32;
            let count = u16::from_le(*(ptr as *const u16).offset(11));
            let (key, value) = read_key_value(&*(ptr as *const u8));
            let key = std::str::from_utf8_unchecked(key);
            let value = txn.load_value(&value);
            let mut value_ = Vec::new();
            let value = if value.len() > 20 {
                value_.extend(&(value.contents())[0..20]);
                value_.extend(b"...");
                &value_[..]
            } else {
                value.contents()
            };
            let value = std::str::from_utf8_unchecked(value);
            // println!("key,value={:?},{:?}",key,value);
            writeln!(buf,
                     "n_{}_{}[label=\"{}, '{}'->'{}'\"];",
                     p.page.offset,
                     off,
                     count,
                     key,
                     value)
                .unwrap();
            if !nodes.contains(&off) {
                nodes.insert(off);

                let left_local = u32::from_le(*(ptr as *const u32));
                // println!("debug, left_local={:?}",left_local);
                if left_local == 1 {
                    let left = u32::from_le(*(ptr.offset(1)));
                    writeln!(buf,
                             "n_{}_{}->n_{}_{}[color=\"red\"];",
                             p.page.offset,
                             off,
                             p.page.offset,
                             left)
                        .unwrap();
                    print_tree(txn, nodes, buf, edges, pages, p, left);
                } else {
                    let page = u64::from_le(*(ptr as *const u64));
                    // println!("debug, page={:?}",u32::from_le(*((ptr as *const u32).offset(1))));
                    // println!("debug, page={:?}",page);
                    if page > 0 && page < txn.length() {
                        let page = txn.load_page(page);
                        let root = page.root();
                        edges.push(format!("n_{}_{}->n_{}_{}[color=\"red\"];",
                                           p.page.offset,
                                           off,
                                           page.page.offset,
                                           root));
                        pages.push(page)
                    } else {
                        if page > 0 {
                            panic!("Wrong page offset:{}", page);
                        }
                    }
                }
                let right_local = u32::from_le(*((ptr as *const u32).offset(2)));
                // println!("debug, right_local={:?}",right_local);
                if right_local == 1 {
                    let right = u32::from_le(*(ptr.offset(3)));
                    edges.push(format!("n_{}_{}->n_{}_{}[color=\"green\"];",
                                       p.page.offset,
                                       off,
                                       p.page.offset,
                                       right));
                    print_tree(txn, nodes, buf, edges, pages, p, right);
                } else {
                    let page = u64::from_le(*((ptr as *const u64).offset(1)));
                    // println!("debug, page={:?}",page);
                    if page > 0 && page < txn.length() {
                        let page = txn.load_page(page);
                        let root = page.root();
                        edges.push(format!("n_{}_{}->n_{}_{}[color=\"green\"];",
                                           p.page.offset,
                                           off,
                                           page.page.offset,
                                           root));
                        pages.push(page)
                    } else {
                        if page > 0 {
                            panic!("Wrong page offset:{}", page);
                        }
                    }
                }
            }
        }
    }
    print_page(t, &mut h, &mut buf, &page,
               true // print children
    );
    writeln!(&mut buf, "}}").unwrap();
}


// unsafe fn tree_get<'a,T:LoadPage>(t:&'a T, key:&[u8], value:Option<&[u8]>)->Option<&'a[u8]> {
// if let Some(root_page) = t.load_root() {
//
// let mut page=root_page;
// let mut current=page.root();
// loop {
// println!("root={:?}, current={:?}",page,current);
// let ptr=page.offset(current) as *mut u32;
//
// let value_=value.unwrap_or(b"");
// let (key0,value0)=read_key_value(&*(ptr as *const u8));
// let cmp= if let Some(value_)=value {
// (key,value_).cmp(&(key0,value0))
// } else {
// key.cmp(&key0)
// };
// debug!("({:?},{:?}), {:?}, ({:?},{:?})",
// std::str::from_utf8_unchecked(key),
// std::str::from_utf8_unchecked(value_),
// cmp,
// std::str::from_utf8_unchecked(key0),
// std::str::from_utf8_unchecked(value0));
// match cmp {
// Ordering::Equal=>return Some(value0),
// Ordering::Less=>{
// let left0 = u32::from_le(*(ptr as *const u32));
// if left0 == 1 {
// let next=u32::from_le(*(ptr.offset(1)));
// if next==0 {
// return None
// } else {
// current=next
// binary_tree_get(t,page,key,value,next)
// }
// } else {
// Global offset
// let left = u64::from_le(*(ptr as *const u64));
// if left==0 {
// return None
// } else {
// left child is another page.
// page=t.load_page(left);
// current=page.root();
// binary_tree_get(t,&page_,key,value,root_)
// }
// }
// if cmp==Ordering::Equal { return Some(value0) } //else { result }
// },
// Ordering::Greater =>{
// let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
// debug!("right0={:?}",right0);
// if right0 == 1 {
// let next=u32::from_le(*(ptr.offset(3)));
// if next==0 {
// return None
// } else {
// current=next
// binary_tree_get(t,page,key,value,next)
// }
// } else {
// global offset, follow
// let right = u64::from_le(*((ptr as *const u64).offset(1)));
// if right==0 {
// return None
// } else {
// right child is another page
// page=t.load_page(right);
// current=page.root();
// binary_tree_get(t,&page_,key,value,root_)
// }
// }
// }
// }
// }
// } else {
// None
// }
// }
//


unsafe fn incr(p: *mut u16) {
    *p = (u16::from_le(*p) + 1).to_le()
}


/// Converts v(u(a,b),c) into u(a,v(b,c))
fn tree_rotate_clockwise(page: &mut MutPage, v: u16) -> u16 {
    debug!("rotate clockwise");
    unsafe {
        let ptr = page.offset(v as isize) as *mut u32;

        let u_is_local = u32::to_le(*ptr) == 1;
        if u_is_local {
            let off_u = *(ptr.offset(1));
            let ptr_u = page.offset(off_u as isize) as *mut u32;

            // fetch node size
            let v_size = u16::from_le(*(ptr as *const u16).offset(11));
            let u_size = u16::from_le(*((ptr_u as *const u16).offset(11)));

            let b_size = {
                if u32::from_le(*(ptr_u.offset(2))) == 1 {
                    let off_b = u32::from_le(*(ptr_u.offset(3)));
                    if off_b != 0 {
                        let ptr_b = page.offset(off_b as isize);
                        u16::from_le(*((ptr_b as *const u16).offset(11)))
                    } else {
                        0
                    }
                } else {
                    // let off=u64::from_le(*((ptr_u as *const u64).offset(1)));
                    0//if off!=0 { 1 } else { 0 }
                }
            };

            // Change the left of v to b
            *(ptr as *mut u64) = *((ptr_u as *const u64).offset(1));

            // Change the right of u to v
            *(ptr_u.offset(2)) = (1 as u32).to_le();
            *(ptr_u.offset(3)) = (v as u32).to_le();
            // debug!("overflow? {} {} {}",v_size,b_size,u_size);
            *(ptr as *mut u16).offset(11) = ((v_size + b_size) - u_size).to_le();
            *(ptr_u as *mut u16).offset(11) = v_size.to_le();
            //
            off_u as u16
        } else {
            // Cannot rotate
            v
        }
    }
}

/// Converts u(a,v(b,c)) into v(u(a,b),c)
fn tree_rotate_anticlockwise(page: &mut MutPage, u: u16) -> u16 {
    debug!("rotate anticlockwise");
    unsafe {
        let ptr = page.offset(u as isize) as *mut u32;

        let v_is_local = u32::from_le(*(ptr.offset(2))) == 1;
        if v_is_local {
            let off_v = u32::from_le(*(ptr.offset(3)));
            let ptr_v = page.offset(off_v as isize) as *mut u32;

            // fetch node size
            let u_size = u16::from_le(*(ptr as *const u16).offset(11));
            let v_size = u16::from_le(*((ptr_v as *const u16).offset(11)));
            let b_size = {
                if u32::from_le(*ptr_v) == 1 {
                    let off_b = u32::from_le(*(ptr_v.offset(1)));
                    if off_b != 0 {
                        let ptr_b = page.offset(off_b as isize) as *const u16;
                        u16::from_le(*(ptr_b.offset(11)))
                    } else {
                        0
                    }
                } else {
                    // if this is really a child
                    // let off=u64::from_le(*(ptr_v as *const u64));
                    0//if off!=0 { 1 } else { 0 }
                }
            };

            // Change the right of u to b
            *((ptr as *mut u64).offset(1)) = *(ptr_v as *const u64);
            // Change the left of v to u
            *ptr_v = (1 as u32).to_le();
            *(ptr_v.offset(1)) = (u as u32).to_le();
            *(ptr as *mut u16).offset(11) = ((u_size + b_size) - v_size).to_le();
            *(ptr_v as *mut u16).offset(11) = u_size.to_le();
            //
            off_v as u16
        } else {
            // Cannot rotate
            u
        }
    }
}

/// Rebalances a binary tree.
fn rebalance(page: &mut MutPage, node: u16) -> u16 {
    debug!("rebalance");
    let x = unsafe {
        let ptr = page.offset(node as isize) as *mut u32;
        let left_local = u32::from_le(*ptr);
        let right_local = u32::from_le(*(ptr.offset(2)));
        let left_cardinal = {
            if left_local == 1 {
                let left = u32::from_le(*(ptr.offset(1)));
                let left_ptr = page.offset(left as isize) as *const u16;
                u16::from_le(*(left_ptr.offset(11)))
            } else {
                0//1
            }
        };
        let right_cardinal = {
            if right_local == 1 {
                let right = u32::from_le(*(ptr.offset(3)));
                let right_ptr = page.offset(right as isize) as *const u16;
                u16::from_le(*(right_ptr.offset(11)))
            } else {
                0//1
            }
        };
        if left_cardinal + 2 < right_cardinal {
            tree_rotate_anticlockwise(page, node)
        } else if right_cardinal + 2 < left_cardinal {
            tree_rotate_clockwise(page, node)
        } else {
            node
        }
    };
    debug!("/rebalance");
    x
}


#[derive(Debug)]
enum Insert<'a> {
    Ok {
        page: MutPage,
        off: u16,
    },
    Split {
        key: &'a [u8],
        value: Value<'a>,
        left: u64,
        right: u64,
        free_page: u64,
    },
}

// #[cfg(test)]
// mod tests {
// use super::*;
// extern crate test;
// use self::test::Bencher;
// extern crate rand;
// use self::rand::{Rng,thread_rng,sample};
// extern crate tempdir;
//
// #[test]
// fn it_works() {
// assert_eq!(4, add_two(2));
// }
//
// #[bench]
// fn bench_put_1000(b: &mut Bencher) {
// let n=1000; // number of bindings to add
// let m=100; // size of the sample to get
// b.iter(|| {
// let dir = self::tempdir::TempDir::new("pijul").unwrap();
// let env=Env::new(dir.path()).unwrap();
// let mut bindings=Vec::new();
// let mut rng=thread_rng();
// {
// let mut txn=env.mut_txn_begin();
// for i in 0..n {
// let x=rng.gen::<i32>();
// let y=rng.gen::<i32>();
// let sx=format!("{}",i);
// let sy=format!("{}",(i*i)%17);
// txn.put(sx.as_bytes(),sy.as_bytes());
// bindings.push((sx,sy,true));
// }
// txn.commit();
// }
// {
// let txn=env.txn_begin();
// for &(ref sx,ref sy,ref b) in sample(&mut rng, bindings.iter(), m) {
// if let Some(y)=txn.get(sx.as_bytes(),None) {
// assert!(*b && y==sy.as_bytes())
// } else {
// assert!(! *b)
// }
// }
// }
// });
// }
// }
//
