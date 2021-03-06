// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.


// TODO:
// - get rid of initial length, grow file as needed. In other words, write lock + unmap + set_len + mmap.

// X 32 bits mmap64 -> delegated to memmap crate.
// X Windows -> delegated to memmap crate.
// X SPARC (8kB pages) -> Allocate two consecutive pages instead of one. The BTree won't see the difference anyway.
// X 32 bits compatibility. mmap has 64 bits offsets.
// X process and thread mutex for mutable transactions.
// X multiple consecutive pages (done with glue_pages)
// X PAGE_SIZE is now a constant, check modulos/divisions to make that constant too.
// X merge last page : done for just the last page, but could probably be improved.
// X count allocated pages (debug/test).
// X test page size in build.rs
// X documentation

// Types guarantee: the only pages we write are the ones we allocate.

// LMDB takes care of zombie readers, at the cost of checking a file of size linear in the number of PIDs at the beginning of every transaction. Also, doesn't work on USB sticks. More details: mdb.c, line 2606: PID locks.

use std;
use std::sync::{RwLock, RwLockReadGuard, Mutex, MutexGuard};
use std::ptr::copy_nonoverlapping;
use std::collections::{HashSet,HashMap};
use fs2::FileExt;
use std::fs::{File,OpenOptions};
use std::path::Path;
use memmap;

pub const CURRENT_VERSION: u64 = 0;

const OFF_MAP_LENGTH:isize = 1;
const OFF_CURRENT_FREE:isize = 2;
// We need a fixed page size for compatibility reasons. Most systems will have half of this, but some (SPARC) don't...
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_16: u16 = 4096;
pub const PAGE_SIZE_64: u64 = 4096;

pub const ZERO_HEADER: isize = 24; // size of the header on page 0, in bytes.
#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    NotEnoughSpace,
    Poison
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::NotEnoughSpace => write!(f, "Not enough space. Try opening the environment with a larger size."),
            Error::Poison => write!(f, "Not enough space. Try opening the environment with a larger size."),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref err) => err.description(),
            Error::NotEnoughSpace => "Not enough space. Try opening the environment with a larger size.",
            Error::Poison => "Poison error"
        }
    }
    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::NotEnoughSpace => None,
            Error::Poison => None

        }
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IO(e)
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_: std::sync::PoisonError<T>) -> Error {
        Error::Poison
    }
}

// Lock order: first take thread locks, then process locks.

// Why are there two synchronization mechanisms?
// Because we would need to upgrade the read lock into a write lock, and rust has no way to do this.
// So, we take a mutex to make sure no other mutable transaction can start,
// and then at the time of writing, we also take the RwLock.

/// Environment, required to start any transactions. Thread-safe, but opening the same database several times in the same process is not cross-platform.
pub struct Env {
    pub length: u64,
    lock_file: File,
    mutable_file: File,
    mmap: memmap::Mmap,
    map: *mut u8,
    lock: RwLock<()>, // Ensure all reads are done when sync starts.
    mutable: Mutex<()>, // Ensure only one mutable transaction can be started.
}
unsafe impl Send for Env {}
unsafe impl Sync for Env {}

pub struct Txn<'env> {
    pub env: &'env Env,
    guard: RwLockReadGuard<'env, ()>,
}

pub struct MutTxn<'env,T> {
    pub env: &'env Env,
    mutable: Option<MutexGuard<'env, ()>>,
    parent:T,
    last_page: u64,
    current_list_page: Page, // current page storing the list of free pages.
    current_list_length: u64, // length of the current page of free pages.
    current_list_position: u64, // position in the current page of free pages.
    occupied_clean_pages: HashSet<u64>, /* Offsets of pages that were allocated by this transaction, and have not been freed since. */
    free_clean_pages: Vec<u64>, /* Offsets of pages that were allocated by this transaction, and then freed. */
    free_pages: Vec<u64>, /* Offsets of old pages freed by this transaction. These were *not* allocated by this transaction. */
    pub roots:HashMap<isize,u64>,
}

impl<'env> Drop for Txn<'env> {
    fn drop(&mut self) {
        self.env.lock_file.unlock().unwrap();
        *self.guard;
    }
}
impl<'env,T> Drop for MutTxn<'env,T> {
    fn drop(&mut self) {
        debug!("dropping transaction");
        self.env.mutable_file.unlock().unwrap();
        if let Some(ref mut guard) = self.mutable {
            debug!("dropping guard");
            **guard
        }
    }
}


#[derive(Debug)]
pub struct Statistics {
    pub free_pages: HashSet<u64>,
    pub bookkeeping_pages: Vec<u64>,
    pub total_pages: u64,
    pub reference_counts: HashMap<u64,u64>
}


impl Env {
    /// Initialize environment. log_length must be at least log(PAGE_SIZE)
    pub fn new<P: AsRef<Path>>(path: P, length: u64) -> Result<Env, Error> {
        //let length = (1 as u64).shl(log_length);
        let db_path = path.as_ref().join("db");
        let db_exists = std::fs::metadata(&db_path).is_ok();
        let file = try!(
            OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(false)
                .create(true)
                .open(db_path)
        );
        try!(file.set_len(length));
        let mut mmap = try!(memmap::Mmap::open(&file, memmap::Protection::ReadWrite));
        let lock_file = try!(File::create(path.as_ref()
                                          .join("db")
                                          .with_extension("lock")));
        let mutable_file = try!(File::create(path.as_ref()
                                             .join("db")
                                             .with_extension("mut")));
        let map = mmap.mut_ptr();
        if !db_exists {
            unsafe {
                std::ptr::write_bytes(map, 0, PAGE_SIZE);
                *(map as *mut u64) = CURRENT_VERSION.to_le();
            }
        } else {
            assert!(unsafe { u64::from_le(*(map as *const u64)) == CURRENT_VERSION })
        }
        let env = Env {
            length: length,
            mmap: mmap,
            map: map,
            lock_file: lock_file,
            mutable_file: mutable_file,
            lock: RwLock::new(()),
            mutable: Mutex::new(()),
        };
        Ok(env)
    }
    /// Start a read-only transaction.
    pub fn txn_begin<'env>(&'env self) -> Result<Txn<'env>,Error> {
        let read = try!(self.lock.read());
        try!(self.lock_file.lock_shared());
        Ok(Txn {
            env: self,
            guard: read,
        })
    }

    /// Start a mutable transaction. Mutable transactions that go out of scope are automatically aborted.
    pub fn mut_txn_begin<'env>(&'env self) -> Result<MutTxn<'env,()>, Error> {
        unsafe {
            let last_page = u64::from_le(*((self.map as *const u64).offset(OFF_MAP_LENGTH)));
            let current_list_page = u64::from_le(*((self.map as *const u64).offset(OFF_CURRENT_FREE)));

            debug!("map header = {:?}, {:?}", last_page ,current_list_page);
            let guard = try!(self.mutable.lock());
            debug!("taking file lock");
            try!(self.mutable_file.lock_exclusive());
            debug!("lock ok");
            let current_list_page = Page {
                data: self.map.offset(current_list_page as isize),
                offset: current_list_page,
            };
            let current_list_length = if current_list_page.offset == 0 {
                0
            } else {
                u64::from_le(*((current_list_page.data as *const u64).offset(1)))
            };
            Ok(MutTxn {
                env: self,
                mutable: Some(guard),
                parent:(),
                last_page: if last_page == 0 {
                    PAGE_SIZE as u64
                } else {
                    last_page
                },
                current_list_page: current_list_page,
                current_list_length: current_list_length,
                current_list_position: current_list_length, /* position of the word immediately after the top. */
                occupied_clean_pages: HashSet::new(),
                free_clean_pages: Vec::new(),
                free_pages: Vec::new(),
                roots: HashMap::new(),
            })
        }
    }

    /// Compute statistics about pages. This is a potentially costlty operation, as we need to go through all bookkeeping pages.
    pub fn statistics(&self) -> Statistics {
        unsafe {
            let total_pages = u64::from_le(*((self.map as *const u64).offset(OFF_MAP_LENGTH))) as usize;
            let mut free_pages = HashSet::new();
            let mut bookkeeping_pages = Vec::new();
            let mut cur = u64::from_le(*((self.map as *const u64).offset(OFF_CURRENT_FREE)));
            while cur != 0 {
                bookkeeping_pages.push(cur);
                let p = self.map.offset(cur as isize) as *const u64;
                let prev = u64::from_le(*p);
                let len = u64::from_le(*(p.offset(1))); // size (number of u64).
                debug!("bookkeeping page: {:?}, {} {}", cur, prev, len);
                {
                    let mut p: *const u64 = (p as *const u64).offset(2);
                    let mut i = 0;
                    while i < len {
                        let free_page = u64::from_le(*p);
                        if !free_pages.insert(free_page) {
                            panic!("free page counted twice: {:?}",free_page)
                        }
                        p = p.offset(1);
                        i += 1
                    }
                }
                cur = prev
            }
            let refcounts = HashMap::new();
            Statistics {
                total_pages: (total_pages / PAGE_SIZE) as u64,
                free_pages: free_pages,
                bookkeeping_pages: bookkeeping_pages,
                reference_counts: refcounts
            }
        }
    }
}

/// This is a semi-owned page: just as we can mutate several indices of an array in the same scope, we must be able to get several pages from a single environment in the same scope. However, pages don't outlive their environment. Pages longer than one PAGE_SIZE might trigger calls to munmap when they go out of scope.
#[derive(Debug)]
pub struct Page {
    pub data: *const u8,
    pub offset: u64,
}
#[derive(Debug)]
pub struct MutPage {
    pub data: *mut u8,
    pub offset: u64,
}

impl MutPage {
    pub fn as_page(&self) -> Page {
        Page { data:self.data, offset: self.offset }
    }
}

pub unsafe fn free<T>(txn: &mut MutTxn<T>, offset: u64) {
    debug!("transaction::free page: {:?}", offset);
    if txn.occupied_clean_pages.remove(&offset) {
        txn.free_clean_pages.push(offset);
    } else {
        // Else, register it for freeing (we cannot reuse it in this transaction).
        txn.free_pages.push(offset)
    }
}

impl<'env> Txn<'env> {
    /// Find the appropriate map segment
    pub fn load_page(&self, off: u64) -> Page {
        debug!("load_page: off={:?}, length = {:?}", off, self.env.length);
        assert!(off < self.env.length);
        unsafe {
            Page {
                data: self.env.map.offset(off as isize),
                offset: off,
            }
        }
    }
    pub fn root(&self,num:isize) -> u64 {
        assert!(ZERO_HEADER + ((num+1)<<3) < (PAGE_SIZE as isize));
        unsafe {
            u64::from_le(*((self.env.map.offset(ZERO_HEADER) as *const u64).offset(num)))
        }
    }
}

#[derive(Debug)]
pub enum Cow {
    Page(Page),
    MutPage(MutPage),
}

impl<'env,T> MutTxn<'env,T> {
    pub fn mut_txn_begin<'txn>(&'txn mut self) -> Result<MutTxn<'env,&'txn mut MutTxn<'env,T>>, Error> {
        unsafe {
            let mut txn = MutTxn {
                env: self.env,
                mutable: None,
                parent: std::mem::uninitialized(),
                last_page: self.last_page,
                current_list_page: Page { data:self.current_list_page.data,
                                          offset: self.current_list_page.offset },
                current_list_length: self.current_list_length,
                current_list_position: self.current_list_position,
                occupied_clean_pages: HashSet::new(),
                free_clean_pages: Vec::new(),
                free_pages: Vec::new(),
                roots:self.roots.clone(),
                //reference_counts:self.reference_counts
            };
            txn.parent = self;
            Ok(txn)
        }
    }
    pub fn load_page(&self, off: u64) -> Page {
        if off >= self.env.length {
            panic!("{:?} >= {:?}", off,self.env.length)
        }
        unsafe {
            Page {
                data: self.env.map.offset(off as isize),
                offset: off,
            }
        }
    }
    pub fn root(&self, num:isize) -> u64 {
        if let Some(root) = self.roots.get(&num) {
            *root
        } else {
            assert!(ZERO_HEADER + ((num+1)<<3) < (PAGE_SIZE as isize));
            unsafe {
                u64::from_le(*((self.env.map.offset(ZERO_HEADER) as *const u64).offset(num as isize)))
            }
        }
    }
    pub fn set_root(&mut self, num:isize, value:u64) {
        self.roots.insert(num,value);
    }
    pub fn load_cow_page(&mut self, off: u64) -> Cow {
        debug!("transaction::load_mut_page: {:?} {:?}",
               off,
               self.occupied_clean_pages);
        assert!(off < self.env.length);
        if off != 0 && self.occupied_clean_pages.contains(&off) {
            unsafe {
                Cow::MutPage(MutPage {
                    data: self.env.map.offset(off as isize),
                    offset: off,
                })
            }
        } else {
            unsafe {
                let d = self.env.map.offset(off as isize);
                Cow::Page(Page {
                    data: d,
                    offset: off,
                })
            }
        }
    }

    /// Pop a free page from the list of free pages.
    fn free_pages_pop(&mut self) -> Option<u64> {
        debug!("free_pages_pop, current_list_position:{}",
               self.current_list_position);
        if self.current_list_page.offset == 0 {
            None
        } else {
            if self.current_list_position == 0 {
                let previous_page = unsafe { u64::from_le(*(self.current_list_page.data as *const u64)) };
                debug!("free_pages_pop, previous page:{}", previous_page);
                if previous_page == 0 {
                    None
                } else {
                    // free page (i.e. push to the list of old
                    // free pages), move to previous bookkeeping
                    // pages, and call recursively.
                    self.free_pages.push(self.current_list_page.offset);
                    unsafe {
                        self.current_list_page = Page {
                            data: self.env.map.offset(previous_page as isize),
                            offset: previous_page,
                        };
                        self.current_list_length = u64::from_le(*((self.current_list_page.data as *const u64).offset(1)))
                    }
                    self.current_list_position = self.current_list_length;
                    self.free_pages_pop()
                }
            } else {
                let pos = self.current_list_position;
                // find the page at the top.
                self.current_list_position -= 1;
                debug!("free_pages_pop, new position:{}", self.current_list_position);
                unsafe {
                    Some(u64::from_le(*((self.current_list_page.data as *mut u64).offset(1 + pos as isize))))
                }
            }
        }
    }
    /// Allocate a single page.
    pub fn alloc_page(&mut self) -> Result<MutPage,Error> {
        debug!("alloc page");
        // If we have allocated and freed a page in this transaction, use it first.
        if let Some(page) = self.free_clean_pages.pop() {
            debug!("clean page reuse:{}", page);
            self.occupied_clean_pages.insert(page);
            Ok(MutPage {
                data: unsafe { self.env.map.offset(page as isize) },
                offset: page,
            })
        } else {
            // Else, if there are free pages, take one.
            if let Some(page) = self.free_pages_pop() {
                debug!("using an old free page: {}", page);
                self.occupied_clean_pages.insert(page);
                Ok(MutPage {
                    data: unsafe { self.env.map.offset(page as isize) },
                    offset: page,
                })
            } else {
                // Else, allocate in the free space.
                let last = self.last_page;
                debug!("eating the free space: {}", last);
                if self.last_page + PAGE_SIZE_64 < self.env.length {
                    self.last_page += PAGE_SIZE_64;
                    self.occupied_clean_pages.insert(last);
                    Ok(MutPage {
                        data: unsafe { self.env.map.offset(last as isize) },
                        offset: last,
                    })
                } else {
                    Err(Error::NotEnoughSpace)
                }
            }
        }
    }
}

pub trait Commit {
    fn commit(&mut self)->Result<(),Error>;
}

impl<'a,'env,T> Commit for MutTxn<'env,&'a mut MutTxn<'env,T>> {
    fn commit(&mut self)->Result<(),Error> {

        self.parent.last_page = self.last_page;
        self.parent.current_list_page = Page { offset:self.current_list_page.offset,
                                               data:self.current_list_page.data };
        self.parent.current_list_length = self.current_list_length;
        self.parent.current_list_position = self.current_list_position;
        self.parent.occupied_clean_pages.extend(self.occupied_clean_pages.iter());
        self.parent.free_clean_pages.extend(self.free_clean_pages.iter());
        self.parent.free_pages.extend(self.free_pages.iter());
        for (u,v) in self.roots.iter() {
            self.parent.roots.insert(*u,*v);
        }
        Ok(())
    }
}

impl<'env> Commit for MutTxn<'env,()> {
    /// Commit a transaction. This is guaranteed to be atomic: either the commit succeeds, and all the changes made during the transaction are written to disk. Or the commit doesn't succeed, and we're back to the state just before starting the transaction.
    fn commit(&mut self) -> Result<(), Error> {
        // Tasks:
        // - allocate new pages (copy-on-write) to write the new list of free pages, including edited "stack pages".
        //
        // - write top of the stack
        // - write user data
        //
        // everything can be sync'ed at any time, except that the first page needs to be sync'ed last.
        unsafe {
            // Copy the current bookkeeping page to a newly allocated page.
            let mut current_page = try!(self.alloc_page());
            if self.current_list_page.offset != 0 {
                // If there was at least one bookkeeping page before.
                debug!("commit: realloc BK, copy {:?}", self.current_list_position);
                copy_nonoverlapping(self.current_list_page.data as *const u64,
                                    current_page.data as *mut u64,
                                    2 + self.current_list_position as usize);
                *((current_page.data as *mut u64).offset(1)) = self.current_list_position.to_le();

                // and free the previous current bookkeeping page.
                debug!("freeing BK page {:?}", self.current_list_page.offset);
                self.free_pages.push(self.current_list_page.offset);

            } else {
                // Else, init the page.
                *(current_page.data as *mut u64) = 0; // previous page: none
                *((current_page.data as *mut u64).offset(1)) = 0; // len: 0
            }

            while !(self.free_pages.is_empty() && self.free_clean_pages.is_empty()) {
                debug!("commit: pushing");
                // If page is full, or this is the first page, allocate new page.
                let len = u64::from_le(*((current_page.data as *const u64).offset(1)));
                debug!("len={:?}", len);
                if 16 + len * 8 + 8 >= PAGE_SIZE as u64 {
                    debug!("commit: current is full, len={}", len);
                    // 8 more bytes wouldn't fit in this page, time to allocate a new one

                    let p = self.free_pages
                        .pop()
                        .unwrap_or_else(|| self.free_clean_pages.pop().unwrap());

                    let new_page =
                        MutPage {
                            data: self.env.map.offset(p as isize),
                            offset: p,
                        };

                    debug!("commit {} allocated {:?}", line!(), new_page.offset);
                    // Write a reference to the current page (which cannot be null).
                    *(new_page.data as *mut u64) = current_page.offset.to_le();
                    // Write the length of the new page (0).
                    *((new_page.data as *mut u64).offset(1)) = 0;

                    current_page = new_page;
                } else {
                    // push
                    let p = self.free_pages
                        .pop()
                        .unwrap_or_else(|| self.free_clean_pages.pop().unwrap());
                    debug!("commit: push {}", p);

                    *((current_page.data as *mut u64).offset(1)) = (len + 1).to_le(); // increase length.
                    *((current_page.data as *mut u64).offset(2 + len as isize)) = p.to_le(); // write pointer.
                }
            }
            // Take lock
            {
                debug!("commit: taking local lock");
                *self.env.lock.write().unwrap();
                debug!("commit: taking file lock");
                self.env.lock_file.lock_exclusive().unwrap();
                debug!("commit: lock ok");
                for (u, v) in self.roots.iter() {
                    *((self.env.map.offset(ZERO_HEADER) as *mut u64).offset(*u as isize)) = (*v).to_le();
                }
                // synchronize all maps. Since PAGE_SIZE is not always
                // an actual page size, we flush the first two pages
                // last, instead of just the last one.
                try!(self.env.mmap.flush_range(2*PAGE_SIZE, (self.env.length - 2*PAGE_SIZE_64) as usize));

                *((self.env.map as *mut u64).offset(OFF_MAP_LENGTH)) = self.last_page.to_le();
                *((self.env.map as *mut u64).offset(OFF_CURRENT_FREE)) = current_page.offset.to_le();
                try!(self.env.mmap.flush_range(0, 2*PAGE_SIZE));
                self.env.lock_file.unlock().unwrap();
                Ok(())
            }
        }
    }
    // Abort the transaction. This is actually a no-op, just as a machine crash aborts a transaction. Letting the transaction go out of scope would have the same effect.
    // pub fn abort(self){
    // }
}
