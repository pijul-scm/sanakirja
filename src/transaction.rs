// TODO:
// - get rid of initial length, grow file as needed. See fallocate, ftruncate. Equivalents on windows?
// - Windows
// - 32 bits mmap64
// - SPARC (8kB pages) -> Allocate two consecutive pages instead of one. The BTree won't see the difference anyway.

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

use libc;
use libc::{c_void,size_t,MS_SYNC,off_t,PROT_WRITE,PROT_READ,MAP_SHARED,MAP_FIXED,munmap,c_int,O_CREAT,O_RDWR};
use std;

use std::ffi::CString;
use std::sync::{RwLock,RwLockReadGuard,Mutex,MutexGuard};
use std::ptr::copy_nonoverlapping;
use std::collections::{HashSet};
use std::cell::{RefCell,RefMut};
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::Shl;
use fs2::FileExt;
use std::fs::File;
use std::path::Path;

// We need a fixed page size for compatibility reasons. Most systems will have half of this, but some (SPARC) don't...
pub const PAGE_SIZE:usize=8192;
pub const PAGE_SIZE_64:u64=8192;
pub const LOG_PAGE_SIZE:usize=13;

pub const ZERO_HEADER:isize=16; // size of the header on page 0, in bytes.

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
}
impl From<std::io::Error> for Error {
    fn from(e:std::io::Error)->Error { Error::IO(e) }
}
// Lock order: first take thread locks, then process locks.

// Why are there two synchronization mechanisms?
// Because we would need to upgrade the read lock into a write lock, and rust has no way to do this.
// So, we take a mutex to make sure no other mutable transaction can start,
// and then at the time of writing, we also take the RwLock.

/// Environment, required to start any transactions. Thread-safe, but opening the same database several times in the same process is not cross-platform.
pub struct Env {
    length:u64,
    log_length:usize,
    mask_length:u64,
    lock_file:File,
    mutable_file:File,
    map:*mut u8,
    fd:c_int,
    lock:RwLock<()>, // Ensure all reads are done when sync starts.
    mutable:Mutex<()> // Ensure only one mutable transaction can be started.
}
unsafe impl Send for Env {}
unsafe impl Sync for Env {}

pub struct Txn<'env> {
    env:&'env Env,
    guard:RwLockReadGuard<'env,()>,
}


pub struct MutTxn<'env> {
    env:&'env Env,
    mutable:MutexGuard<'env,()>,
    last_page:u64,
    current_list_page:Page, // current page storing the list of free pages.
    current_list_length:u64, // length of the current page of free pages.
    current_list_position:u64, // position in the current page of free pages.
    occupied_clean_pages:HashSet<u64>, // Offsets of pages that were allocated by this transaction, and have not been freed since.
    free_clean_pages:Vec<u64>, // Offsets of pages that were allocated by this transaction, and then freed.
    free_pages:Vec<u64>, // Offsets of old pages freed by this transaction. These were *not* allocated by this transaction.
}

impl<'env> Drop for Txn<'env>{
    fn drop(&mut self){
        self.env.lock_file.unlock().unwrap();
        *self.guard;
    }
}
impl<'env> Drop for MutTxn<'env>{
    fn drop(&mut self){
        self.env.mutable_file.unlock().unwrap();
    }
}


#[derive(Debug)]
pub struct Statistics {
    free_pages:HashSet<u64>,
    bookkeeping_pages:Vec<u64>,
    total_pages:u64
}

unsafe fn mmap(fd:c_int,place:Option<*mut u8>,length:u64)->*mut u8 {
    let e=libc::mmap(place.unwrap_or(std::ptr::null_mut()) as *mut c_void,
                     length as size_t,
                     PROT_READ|PROT_WRITE,
                     MAP_SHARED,
                     fd,0);
    if e == libc::MAP_FAILED {
        std::ptr::null_mut()
    } else {
        e as *mut u8
    }
}


impl Env {
    /// Initialize environment. log_length must be at least log(PAGE_SIZE)
    pub fn new<P:AsRef<Path>>(file:P,log_length:usize)->Result<Env,Error> {
        unsafe {
            let length:u64=(1 as u64).shl(log_length);
            assert!(length>=PAGE_SIZE_64);
            let path=file.as_ref().join("db");
            let name=CString::new(path.to_str().unwrap()).unwrap();
            let fd=libc::open(name.as_ptr(),O_CREAT|O_RDWR,0o777);
            let ftrunc=libc::ftruncate(fd,length as off_t);
            if ftrunc<0 {
                Err(Error::IO(std::io::Error::last_os_error()))
            } else {
                let memory=mmap(fd,None,length);
                if memory.is_null() {
                    Err(Error::IO(std::io::Error::last_os_error()))
                } else {
                    let lock_file=try!(File::create(file.as_ref().join("db").with_extension(".lock")));
                    let mutable_file=try!(File::create(file.as_ref().join("db").with_extension(".mut")));
                    let env=Env {
                        length:length,
                        log_length:log_length,
                        mask_length:(length-1) as u64,
                        map:memory as *mut u8,
                        lock_file:lock_file,
                        mutable_file:mutable_file,
                        fd:fd,
                        lock:RwLock::new(()),
                        mutable:Mutex::new(())
                    };
                    Ok(env)
                }
            }
        }
    }
    /// Start a read-only transaction.
    pub fn txn_begin<'env>(&'env self)->Txn<'env> {
        let read=self.lock.read().unwrap();
        self.lock_file.lock_shared().unwrap();
        Txn { env:self,guard:read }
    }
    fn read_map_header(&self)->(u64,u64) {
        unsafe {
            let last_page=u64::from_le(*(self.map as *const u64));
            let current_list_page=u64::from_le(*((self.map as *const u64).offset(1)));
            (last_page,current_list_page)
        }
    }
    /// Start a mutable transaction. Mutable transactions that go out of scope are automatically aborted.
    pub fn mut_txn_begin<'env>(&'env self)->MutTxn<'env> {
        unsafe {
            let (last_page,current_list_page)=self.read_map_header();
            let guard=self.mutable.lock().unwrap();
            self.mutable_file.lock_exclusive().unwrap();
            let current_list_page = Page { data:self.map.offset(current_list_page as isize),
                                           offset:current_list_page };
            let current_list_length=
                if current_list_page.offset == 0 { 0 } else {
                    u64::from_le(*((current_list_page.data as *const u64).offset(1)))
                };
            MutTxn {
                env:self,
                mutable:guard,
                last_page:if last_page == 0 { PAGE_SIZE as u64 } else { last_page },
                current_list_page:current_list_page,
                current_list_length:current_list_length,
                current_list_position:current_list_length, // position of the word immediately after the top.
                occupied_clean_pages:HashSet::new(),
                free_clean_pages:Vec::new(),
                free_pages:Vec::new()
            }
        }
    }

    pub fn extra(&self)->*mut u8 {
        unsafe {
            self.map.offset(ZERO_HEADER)
        }
    }

    /// Compute statistics about pages. This is a potentially costlty operation, as we need to go through all bookkeeping pages.
    pub fn statistics(&self)->Statistics{
        unsafe {
            let total_pages = u64::from_le(*(self.map as *const u64)) as usize;
            let mut free_pages = HashSet::new();
            let mut bookkeeping_pages = Vec::new();
            let mut cur= u64::from_le(*((self.map as *const u64).offset(1)));
            while cur!=0 {
                bookkeeping_pages.push(cur);
                let p=self.map.offset(cur as isize) as *const u64;
                let prev=u64::from_le(*p);
                let len= u64::from_le(*(p.offset(1))); // size (number of u64).
                debug!("bookkeeping page: {:?}, {} {}", cur, prev, len);
                {
                    let mut p:*const u64=(p as *const u64).offset(2);
                    let mut i=0;
                    while i<len {
                        let free_page=u64::from_le(*p);
                        if !free_pages.insert(free_page) {
                            panic!("free page counted twice")
                        }
                        p=p.offset(1);
                        i+=1
                    }
                }
                cur=prev
            }
            Statistics {
                total_pages:(total_pages/PAGE_SIZE) as u64,
                free_pages:free_pages,
                bookkeeping_pages:bookkeeping_pages
            }
        }
    }
}

/// This is a semi-owned page: just as we can mutate several indices of an array in the same scope, we must be able to get several pages from a single environment in the same scope. However, pages don't outlive their environment. Pages longer than one PAGE_SIZE might trigger calls to munmap when they go out of scope.
#[derive(Debug)]
pub struct Page {
    pub data:*const u8,
    pub offset:u64
}
#[derive(Debug)]
pub struct MutPage {
    pub data:*mut u8,
    pub offset:u64
}


impl Page {
    pub unsafe fn as_slice<'a>(&'a self)->&'a[u8]{
        std::slice::from_raw_parts(self.data as *const u8,PAGE_SIZE)
    }
    pub fn free(&self,txn:&mut MutTxn) {
        // If this page was allocated during this transaction
        if txn.occupied_clean_pages.remove(&self.offset) {
            txn.free_clean_pages.push(self.offset);
        } else {
            // Else, register it for freeing (we cannot reuse it in this transaction).
            txn.free_pages.push(self.offset)
        }
    }
}

impl MutPage {
    pub unsafe fn as_slice<'a>(&'a self)->&'a[u8] {
        std::slice::from_raw_parts(self.data as *const u8,PAGE_SIZE)
    }
    pub unsafe fn as_mut_slice<'a>(&'a mut self)->&'a mut [u8]{
        std::slice::from_raw_parts_mut(self.data as *mut u8,PAGE_SIZE)
    }
    pub fn free(&self,txn:&mut MutTxn) {
        let p:&Page = unsafe { std::mem::transmute(self) }; p.free(txn)
    }
}


pub struct MutPages<'a> { pub pages:Pages<'a> }

pub struct Pages<'a> {
    pub map:*mut u8,
    len:usize,
    pages:PhantomData<&'a()>
}

impl<'a> Drop for Pages<'a> {
    fn drop(&mut self) {
        let memory=self.map;
        unsafe {
            munmap(memory as *mut c_void,self.len);
        }
    }
}
impl <'env>Txn<'env> {
    /// Find the appropriate map segment
    pub fn offset(&self,off:u64)->*mut u8 {
        unsafe { self.env.map.offset(off as isize) }
    }
    pub fn load_page(&self,off:u64)->Page {
        unsafe {
            Page { data:self.env.map.offset(off as isize),
                   offset:off }
        }
    }
}

#[derive(Debug)]
pub enum Cow {
    Page(Page),
    MutPage(MutPage)
}

impl <'env>MutTxn<'env> {

    pub fn load_page(&self,off:u64)->Page {
        unsafe {
            Page { data:self.env.map.offset(off as isize),
                   offset:off }
        }
    }
    pub fn load_mut_page(&mut self,off:u64)->Cow {
        debug!("transaction::load_mut_page: {:?} {:?}", off, self.occupied_clean_pages);
        if off !=0 && self.occupied_clean_pages.contains(&off) {
            unsafe { Cow::MutPage(MutPage { data:self.env.map.offset(off as isize),
                                            offset:off }) }
        } else {
            unsafe {
                let d=self.env.map.offset(off as isize);
                Cow::Page(Page { data:d,offset:off })
            }
        }
    }

    /// Pop a free page from the list of free pages.
    fn free_pages_pop(&mut self)->Option<u64> {
        unsafe {
            debug!("free_pages_pop, current_list_position:{}",self.current_list_position);
            if self.current_list_page.offset==0 { None } else {
                if self.current_list_position==0 {
                    let previous_page = u64::from_le(*(self.current_list_page.data as *const u64));
                    debug!("free_pages_pop, previous page:{}",previous_page);
                    if previous_page == 0 {
                        None
                    } else {
                        // free page, move to previous one and call recursively.
                        self.free_pages.push(self.current_list_page.offset);
                        self.current_list_length = u64::from_le(*((self.current_list_page.data as *const u64).offset(1)));
                        self.current_list_page = Page { data:self.env.map.offset(previous_page as isize),
                                                        offset:previous_page };

                        self.free_pages_pop()
                    }
                } else {
                    let pos=self.current_list_position;
                    // find the page at the top.
                    self.current_list_position -= 1;
                    debug!("free_pages_pop, new position:{}",self.current_list_position);
                    Some(u64::from_le(*((self.current_list_page.data as *mut u64).offset(1+pos as isize))))
                }
            }
        }
    }
    /// Allocate a single page.
    pub fn alloc_page(&mut self)->Option<MutPage> {
        debug!("alloc page");
        // If we have allocated and freed a page in this transaction, use it first.
        if let Some(page)=self.free_clean_pages.pop() {
            debug!("clean page reuse:{}",page);
            self.occupied_clean_pages.insert(page);
            Some(MutPage {
                data:unsafe { self.env.map.offset(page as isize) },
                offset:page
            })
        } else {
            // Else, if there are free pages, take one.
            if let Some(page)=self.free_pages_pop() {
                debug!("using an old free page: {}",page);
                self.occupied_clean_pages.insert(page);
                Some(MutPage {
                    data:unsafe { self.env.map.offset(page as isize) },
                    offset:page
                })
            } else {
                // Else, allocate in the free space.
                let last=self.last_page;
                debug!("eating the free space: {}",last);
                if self.last_page+PAGE_SIZE_64 < self.env.length {
                    self.last_page += PAGE_SIZE_64;
                    self.occupied_clean_pages.insert(last);
                    Some(MutPage {
                        data:unsafe { self.env.map.offset(last as isize) },
                        offset:last
                    })
                } else { None }
            }
        }
    }

    /// Commit a transaction. This is guaranteed to be atomic: either the commit succeeds, and all the changes made during the transaction are written to disk. Or the commit doesn't succeed, and we're back to the state just before starting the transaction.
    pub fn commit(mut self,extra:&[u8])->Result<(),Error>{
        // Tasks:
        // - allocate new pages (copy-on-write) to write the new list of free pages, including edited "stack pages".
        //
        // - write top of the stack
        // - write user data
        //
        // everything can be sync'ed at any time, except that the first page needs to be sync'ed last.
        unsafe {
            // While we've not written everything.
            // Write free pages first.
            let mut current_page:*mut u8= std::ptr::null_mut();
            let mut current_page_offset=0;
            // Trick here: we want to merge the last free page with the blank space, but since the following while loop can allocate pages, there is a risk the blank space gets overwritten. Therefore, we'll change the value of last_free_page (in page 0) only if a page has been freed next to it, and none has been allocated in the blank space.
            let mut last_freed_page = 0;

            while ! (self.free_pages.is_empty() && self.free_clean_pages.is_empty()) {
                debug!("commit: pushing");
                // If page is full, or this is the first page, allocate new page.
                if current_page.is_null() {
                    debug!("commit: current is null");
                    // First page, copy-on-write
                    let new_page = self.alloc_page().unwrap();
                    if self.current_list_page.offset != 0 {
                        debug!("Copying from {} to {}",
                               self.current_list_page.offset,
                               new_page.offset);
                        copy_nonoverlapping(self.current_list_page.data as *const u64,
                                            new_page.data as *mut u64,
                                            2 + self.current_list_position as usize);
                        *((new_page.data as *mut u64).offset(1)) = self.current_list_position.to_le();
                        self.free_pages.push(self.current_list_page.offset);
                        let off=u64::from_le(*(new_page.data as *const u64));
                        let len=u64::from_le(*((new_page.data as *const u64).offset(1)));
                        debug!("off={}, len={}",off,len);
                    } else {
                        debug!("commit: allocate");
                        *(new_page.data as *mut u64) = 0; // previous page: none
                        *((new_page.data as *mut u64).offset(1)) = 0; // len: 0
                    }
                    current_page = new_page.data;
                    current_page_offset = new_page.offset;
                } else {
                    debug!("commit: current is not null");
                    let len=u64::from_le(*((current_page as *const u64).offset(1)));
                    if len*8 + 24 > PAGE_SIZE as u64 {
                        debug!("commit: current is full, len={}",len);
                        // 8 more bytes wouldn't fit in this page, time to allocate a new one
                        let new_page = self.alloc_page().unwrap();

                        // Write a reference to the current page (which cannot be null).
                        *(new_page.data as *mut u64)=current_page_offset.to_le();
                        // Write the length of the new page (0).
                        *((new_page.data as *mut u64).offset(1)) = 0;

                        current_page = new_page.data;
                        current_page_offset = new_page.offset
                    } else {
                        // push
                        let p=self.free_pages.pop().unwrap_or_else(|| self.free_clean_pages.pop().unwrap());
                        debug!("commit: push {}",p);
                        // This is one of the pages freed by this transaction.
                        last_freed_page = max(p,last_freed_page);

                        *((current_page as *mut u64).offset(1)) = (len+1).to_le(); // increase length.
                        *((current_page as *mut u64).offset(2+len as isize)) = p.to_le(); // write pointer.
                    }
                }
            }
            // Take lock
            {
                *self.env.lock.write().unwrap();
                self.env.lock_file.lock_exclusive().unwrap();
                if last_freed_page == self.last_page - PAGE_SIZE as u64 {
                    // If the last page was freed by the
                    // transaction. Maybe other blocks just before it
                    // were freed too, but they're not merged into the
                    // blank space. Maybe they should, but since the
                    // penultimate page might have been freed in a
                    // previous transaction and not reused, this is
                    // not very general either, so providing this
                    // simple mechanism allows for shrinking.
                    *(self.env.map as *mut u64) = last_freed_page.to_le();
                } else {
                    *(self.env.map as *mut u64) = self.last_page.to_le();
                }
                *((self.env.map as *mut u64).offset(1)) = current_page_offset.to_le();
                println!("commit: {:?}",extra);
                copy_nonoverlapping(extra.as_ptr(),
                                    self.env.map.offset(ZERO_HEADER),
                                    extra.len());
                // synchronize all maps
                let ok=libc::msync(self.env.map.offset(PAGE_SIZE as isize) as *mut c_void,
                                   (self.env.length as u64 - PAGE_SIZE as u64) as size_t,MS_SYNC);
                if ok!=0 { return Err(Error::IO(std::io::Error::last_os_error())) }

                let ok= libc::msync(self.env.map as *mut c_void,PAGE_SIZE as size_t,MS_SYNC);
                if ok!=0 { return Err(Error::IO(std::io::Error::last_os_error())) }

                *self.mutable; // This is actually just unit (prevents dead code warnings)
                Ok(())
            }
        }
    }

    /// Abort the transaction. This is actually a no-op, just as a machine crash aborts a transaction. Letting the transaction go out of scope would have the same effect.
    pub fn abort(self){
    }
}

impl Drop for Env {
    fn drop(&mut self){
        unsafe {
            munmap(self.map as *mut c_void,self.length as size_t);
            libc::close(self.fd);
        }
    }
}




/*
pub fn glue_mut_pages<'a>(env:&Env,pages:&'a[MutPage])->Result<MutPages<'a>,Error> {
    unsafe {
        glue_pages(env,std::mem::transmute(pages)).and_then(|x| Ok(MutPages {pages:x}))
    }
}
pub fn glue_pages<'a>(env:&Env,pages:&'a[Page])->Result<Pages<'a>,Error> {
    let mut memory=std::ptr::null_mut();
    let mut p0=std::ptr::null_mut();
    let mut l=0;
    for p in pages {
        unsafe {
            if memory.is_null() {
                memory=libc::mmap(memory as *mut c_void,
                                  PAGE_SIZE as size_t,
                                  PROT_READ|PROT_WRITE,
                                  MAP_SHARED,
                                  env.fd,
                                  p.offset as off_t
                                  ) as *mut u8;
            } else {
                memory=libc::mmap(memory.offset(PAGE_SIZE as isize) as *mut c_void,
                                  PAGE_SIZE as size_t,
                                  PROT_READ|PROT_WRITE,
                                  MAP_SHARED | MAP_FIXED,
                                  env.fd,
                                  p.offset as off_t
                                  ) as *mut u8;
            }
            if memory as *mut c_void == libc::MAP_FAILED {
                let err=std::io::Error::last_os_error();
                {
                    // the Drop trait unmaps the memory.
                    Pages {map:p0,len:l,pages:PhantomData };
                }
                return Err(Error::IO(err));
            } else {
                if p0.is_null() {
                    p0=memory
                }
                l += PAGE_SIZE
            }
        }
    }
    Ok(Pages {map:p0,len:l,pages:PhantomData})
}
*/
