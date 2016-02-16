// TODO:
// - get rid of initial length, grow file as needed. See fallocate, ftruncate. Equivalents on windows?
// - Windows
// - 32 bits mmap64
// - SPARC (8kB pages)

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
use libc::{c_void,size_t,MS_SYNC,off_t,PROT_WRITE,PROT_READ,MAP_SHARED,MAP_FIXED,mmap,munmap,c_int,O_CREAT,O_RDWR};
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

use constants::*;


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
    length:usize,
    log_length:usize,
    mask_length:u64,
    lock_file:File,
    mutable_file:File,
    maps:RefCell<Vec<*mut u8>>,
    fd:c_int,
    lock:RwLock<()>, // Ensure all reads are done when sync starts.
    mutable:Mutex<()> // Ensure only one mutable transaction can be started.
}
unsafe impl Send for Env {}
unsafe impl Sync for Env {}

pub unsafe fn readle_64(p0:*const u8)->u64 {
    (*(p0 as *const u64)).to_le()
}

pub unsafe fn writele_64(p:*mut u8,v:u64) {
    *(p as *mut u64) = v.to_le()
}

pub struct Txn<'env> {
    env:&'env Env,
    guard:RwLockReadGuard<'env,()>,
}


pub struct MutTxn<'env> {
    env:&'env Env,
    mutable:MutexGuard<'env,()>,
    last_page:u64,
    current_list_page:Page,
    current_list_length:u64,
    current_list_position:u64,
    occupied_clean_pages:HashSet<u64>, // Offsets of pages that were allocated by this transaction.
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

impl Env {

    /// Initialize environment. log_length must be at least log(PAGE_SIZE)
    pub fn new<P:AsRef<Path>>(file:P,log_length:usize)->Result<Env,Error> {
        unsafe {
            let length:usize=(1 as usize).shl(log_length);
            assert!(length>=PAGE_SIZE);
            let path=file.as_ref().join("db");
            let name=CString::new(path.to_str().unwrap()).unwrap();
            let fd=libc::open(name.as_ptr(),O_CREAT|O_RDWR,0o777);
            let ftrunc=libc::ftruncate(fd,length as off_t);
            if ftrunc<0 {
                Err(Error::IO(std::io::Error::last_os_error()))
            } else {
                let memory=libc::mmap(std::ptr::null_mut(),
                                      length as size_t,
                                      PROT_READ|PROT_WRITE,
                                      MAP_SHARED,
                                      fd,0);
                if memory==libc::MAP_FAILED {
                    Err(Error::IO(std::io::Error::last_os_error()))
                } else {
                    let lock_file=try!(File::create(file.as_ref().join("db").with_extension(".lock")));
                    let mutable_file=try!(File::create(file.as_ref().join("db").with_extension(".mut")));
                    let env=Env {
                        length:length,
                        log_length:log_length,
                        mask_length:(length-1) as u64,
                        maps:RefCell::new(vec!(memory as *mut u8)),
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

    /// Start a mutable transaction. Mutable transactions that go out of scope are automatically aborted.
    pub fn mut_txn_begin<'env>(&'env self)->MutTxn<'env> {
        unsafe {
            let map=self.maps.borrow();
            let last_page=readle_64(map[0]);
            let current_list_page=readle_64(map[0].offset(8));

            let guard=self.mutable.lock().unwrap();
            self.mutable_file.lock_exclusive().unwrap();
            let current_list_page = Page { data:offset(self,self.maps.borrow_mut(),current_list_page),
                                           len:PAGE_SIZE, offset:current_list_page };
            let current_list_length=if current_list_page.offset == 0 { 0 } else {
                readle_64(current_list_page.data.offset(8))
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


    /// Compute statistics about pages. This is a potentially costlty operation, as we need to go through all bookkeeping pages.
    pub fn statistics(&self)->Statistics{
        unsafe {
            let map=self.maps.borrow();
            let total_pages = readle_64(map[0].offset(0)) as usize;
            let mut free_pages = HashSet::new();
            let mut bookkeeping_pages = Vec::new();
            let mut cur=readle_64(map[0].offset(8));
            while cur!=0 {
                bookkeeping_pages.push(cur);
                let p=map[0].offset(cur as isize);
                let prev=readle_64(p);
                let len=readle_64(p.offset(8));
                println!("- {:?}, {}", cur, len);
                {
                    let mut i=0;
                    while i<len {
                        let free_page=readle_64(p.offset(16+i as isize));
                        if !free_pages.insert(free_page) {
                            panic!("free page counted twice")
                        }
                        i+=8
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
pub struct Page {
    pub data:*const u8,
    pub len:usize,
    pub offset:u64
}
pub struct MutPage {
    pub data:*mut u8,
    pub len:usize,
    pub offset:u64
}


impl Page {
    pub unsafe fn as_slice<'a>(&'a self)->&'a[u8]{
        std::slice::from_raw_parts(self.data as *const u8,self.len as usize)
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
        std::slice::from_raw_parts(self.data as *const u8,self.len as usize)
    }
    pub unsafe fn as_mut_slice<'a>(&'a mut self)->&'a mut [u8]{
        std::slice::from_raw_parts_mut(self.data as *mut u8,self.len)
    }
    pub fn free(&self,txn:&mut MutTxn) {
        let p:&Page = unsafe { std::mem::transmute(self) }; p.free(txn)
    }
}


pub struct MutPages<'a> { pub pages:Pages<'a> }

pub struct Pages<'a> {
    pub map:*mut u8,
    pub len:usize,
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

fn offset(env:&Env, mut maps:std::cell::RefMut<Vec<*mut u8>>,off:u64)-> *mut u8 {
    // Allocate more space in the file if needed, adding a new mapping
    let index=(off>>env.log_length) as usize;
    while index>= maps.len() {
        maps.push(std::ptr::null_mut())
    }
    unsafe {
        let map = maps.get_unchecked_mut(index);
        if (*map).is_null(){
            *map=mmap(std::ptr::null_mut(),
                      env.length,
                      PROT_READ|PROT_WRITE,
                      MAP_SHARED,
                      env.fd,off as off_t) as *mut u8;
            if (*map as *mut c_void)==libc::MAP_FAILED{
                panic!(format!("mmap failed: {:?}", std::io::Error::last_os_error()))
            }
        }
        (*map).offset((off & env.mask_length) as isize)
    }
}

pub fn load_page(env:&Env,maps:RefMut<Vec<*mut u8>>,off:u64)->Page {
    Page { data:offset(env,maps,off),
           len:PAGE_SIZE,
           offset:off }
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

impl <'env>Txn<'env> {
    /// Find the appropriate map segment
    pub fn offset(&self,off:u64)->*mut u8 {
        offset(self.env,self.env.maps.borrow_mut(), off)
    }
    pub fn load_page(&self,off:u64)->Page {
        load_page(self.env,self.env.maps.borrow_mut(),off)
    }
}
impl <'env>MutTxn<'env> {
    /// Find the appropriate map segment
    pub fn offset(&self,off:u64)->*mut u8 {
        offset(self.env,self.env.maps.borrow_mut(), off)
    }
    pub fn load_page(&self,off:u64)->Page {
        load_page(self.env,self.env.maps.borrow_mut(),off)
    }
    pub fn load_mut_page(&mut self,off:u64)->MutPage {
        let page=load_page(self.env,self.env.maps.borrow_mut(),off);
        if off !=0 && self.occupied_clean_pages.contains(&off) {
            unsafe { std::mem::transmute(page) }
        } else {
            let result=self.alloc_page().unwrap();
            unsafe { copy_nonoverlapping(page.data,result.data,PAGE_SIZE) }
            result
        }
    }
    /*
    pub fn glue_pages<'a>(&self,pages:&'a[Page])->Result<Pages<'a>,Error> {
        glue_pages(self.env,pages)
    }
    pub fn glue_mut_pages<'a>(&self,pages:&'a[MutPage])->Result<MutPages<'a>,Error> {
        glue_mut_pages(self.env,pages)
    }
    */
    /// Pop a free page from the list of free pages.
    fn free_pages_pop(&mut self)->Option<u64> {
        unsafe {
            debug!("free_pages_pop, current_list_position:{}",self.current_list_position);
            if self.current_list_page.offset==0 { None } else {
                if self.current_list_position==0 {
                    let previous_page = readle_64(self.current_list_page.data);
                    debug!("free_pages_pop, previous page:{}",previous_page);
                    if previous_page == 0 {
                        None
                    } else {
                        // free page, move to previous one and call recursively.
                        self.free_pages.push(self.current_list_page.offset);
                        self.current_list_length = readle_64(self.current_list_page.data.offset(8));
                        self.current_list_page = Page { data:self.offset(previous_page),
                                                        len:PAGE_SIZE, offset:previous_page };

                        self.free_pages_pop()
                    }
                } else {
                    let pos=self.current_list_position;
                    // find the page at the top.
                    self.current_list_position -= 8;
                    debug!("free_pages_pop, new position:{}",self.current_list_position);
                    Some(readle_64(self.current_list_page.data.offset(8 + pos as isize)))
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
            Some(MutPage {
                data:self.offset(page),
                len:PAGE_SIZE as usize,
                offset:page
            })
        } else {
            // Else, if there are free pages, take one.
            if let Some(page)=self.free_pages_pop() {
                debug!("using an old free page: {}",page);
                self.occupied_clean_pages.insert(page);
                Some(MutPage {
                    data:self.offset(page),
                    len:PAGE_SIZE as usize,
                    offset:page
                })
            } else {
                // Else, allocate in the free space.
                let last=self.last_page;
                debug!("eating the free space: {}",last);
                self.last_page += PAGE_SIZE as u64;
                Some(MutPage {
                    data:self.offset(last),
                    len:PAGE_SIZE as usize,
                    offset:last
                })
            }
        }
    }

    /// Commit a transaction. This is guaranteed to be atomic: either the commit succeeds, and all the changes made during the transaction are written to disk. Or the commit doesn't succeed, and we're back to the state just before starting the transaction.
    pub fn commit(mut self)->Result<(),Error>{
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
                        copy_nonoverlapping(self.current_list_page.data,
                                            new_page.data,
                                            16 + self.current_list_position as usize);
                        writele_64(new_page.data.offset(8), self.current_list_position);
                        self.free_pages.push(self.current_list_page.offset);
                        let off=readle_64(new_page.data);
                        let len=readle_64(self.current_list_page.data.offset(8));
                        debug!("off={}, len={}",off,len);
                    } else {
                        debug!("commit: allocate");
                        writele_64(new_page.data, 0); // previous page: none
                        writele_64(new_page.data.offset(8), 0); // len: 0
                    }
                    current_page = new_page.data;
                    current_page_offset = new_page.offset;
                } else {
                    debug!("commit: current is not null");
                    let len=readle_64(current_page.offset(8));
                    if len + 24 > PAGE_SIZE as u64 {
                        debug!("commit: current is full, len={}",len);
                        // 8 more bytes wouldn't fit in this page, time to allocate a new one
                        let new_page = self.alloc_page().unwrap();

                        // Write a reference to the current page (which cannot be null).
                        writele_64(new_page.data, current_page_offset);
                        // Write the length of the new page (0).
                        writele_64(new_page.data.offset(8), 0);

                        current_page = new_page.data;
                        current_page_offset = new_page.offset
                    } else {
                        // push
                        let p=self.free_pages.pop().unwrap_or_else(|| self.free_clean_pages.pop().unwrap());
                        debug!("commit: push {}",p);
                        // This is one of the pages freed by this transaction.
                        last_freed_page = max(p,last_freed_page);

                        writele_64(current_page.offset(8),len+8); // increase length.
                        writele_64(current_page.offset(16+len as isize), p); // write pointer.
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
                    writele_64(self.offset(0),last_freed_page);
                } else {
                    writele_64(self.offset(0),self.last_page);
                }
                writele_64(self.offset(8),current_page_offset);
                // synchronize all maps
                let maps=self.env.maps.borrow();
                for map in maps.iter().skip(1) {
                    let ok= libc::msync(*map as *mut c_void,self.env.length as size_t,MS_SYNC);
                    if ok!=0 {
                        return Err(Error::IO(std::io::Error::last_os_error()))
                    }
                }
                let ok=libc::msync(maps[0].offset(PAGE_SIZE as isize) as *mut c_void,
                                   (self.env.length as u64 - PAGE_SIZE as u64) as size_t,MS_SYNC);
                if ok!=0 { return Err(Error::IO(std::io::Error::last_os_error())) }

                let ok= libc::msync(maps[0] as *mut c_void,PAGE_SIZE as size_t,MS_SYNC);
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
            for p in self.maps.borrow().iter() {
                unsafe {
                    if !(*p).is_null() {
                        munmap(*p as *mut c_void,self.length);
                    }
                }
            }
            libc::close(self.fd);
        }
    }
}