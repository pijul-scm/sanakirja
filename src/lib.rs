// TODO:
// - get rid of initial length -- grow file as needed.
// - process and thread mutex for mutable transactions.
//   see http://www.gnu.org/software/libc/manual/html_node/File-Locks.html
//
// X multiple consecutive pages (done with glue_pages)
// X PAGE_SIZE is now a constant, check modulos/divisions to make that constant too.
// X merge last page : done for just the last page, but could probably be improved.
// X count allocated pages (debug/test).
// X test page size in build.rs
// X documentation

// Types guarantees:
// - The only pages we write are the ones we allocate.


// Problem 1:
// - Start a reader, moving very slowly down the tree.
// - Quickly start a mutable transaction, free some of the pages the reader is reading.
// - Commit, start another one, overwrite some of the free pages.
// - Go on reading the pages -> the reader reads garbage.

// Solution: before commiting page 0, wait until all readers are done (try to take a lock on the lock file). How about suspended readers?

// Other solution: before starting the second write transaction, make sure all readers are done. How?



// mdb.c, line 2606: PID locks.
// crate fs2: lock files.

extern crate libc;
use libc::*;
use std::ffi::CString;
use std::sync::{Arc,RwLock,RwLockReadGuard,Mutex,MutexGuard};
use std::ptr::copy_nonoverlapping;
use std::collections::HashSet;
#[macro_use]
extern crate log;
use std::cmp::max;
use std::marker::PhantomData;

mod constants;
use constants::*;

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    MutableTxn
}

// Why are there two synchronization mechanisms?
// Because we would need to upgrade the read lock into a write lock, and rust has no way to do this.
// So, we take a mutex to make sure no other mutable transaction can start,
// and then at the time of writing, we also take the RwLock.
pub struct Env {
    length:u64,
    map:*mut u8,
    fd:c_int,
    lock:Arc<RwLock<()>>, // Ensure all reads are done when sync starts.
    mutable:Arc<Mutex<()>> // Ensure only one mutable transaction can be started.
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
    lock:Arc<RwLock<()>>,
    mutable:MutexGuard<'env,()>,
    last_page:u64,
    current_list_page:u64,
    current_list_length:u64,
    current_list_position:u64,
    occupied_clean_pages:HashSet<u64>,
    free_clean_pages:Vec<u64>,
    free_pages:Vec<u64>,
}

#[derive(Debug)]
pub struct Statistics {
    free_pages:HashSet<u64>,
    bookkeeping_pages:Vec<u64>,
    total_pages:u64
}

impl Env {

    /// Initialize environment.
    pub fn new(file:&str,n_pages:usize)->Result<Env,Error> {
        unsafe {
            let length=PAGE_SIZE as u64 * n_pages as u64;
            let name=CString::new(file).unwrap();
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
                    Ok(Env {
                        length:length,
                        map:memory as *mut u8,
                        fd:fd,
                        lock:Arc::new(RwLock::new(())),
                        mutable:Arc::new(Mutex::new(()))
                    })
                }
            }
        }
    }

    /// Start a read-only transaction.
    pub fn txn_begin<'env>(&'env self)->Txn<'env> {
        let read=self.lock.read().unwrap();
        Txn { env:self,guard:read }
    }
    /// Start a mutable transaction. Mutable transactions that go out of scope are automatically aborted.
    pub fn txn_mut_begin<'env>(&'env self)->Result<MutTxn<'env>,Error> {
        unsafe {
            let last_page=readle_64(self.map);
            let current_list_page=readle_64(self.map.offset(8));
            let current_list_length = if current_list_page == 0 { 0 } else {
                readle_64(self.map.offset(current_list_page as isize+8))
            };
            let current_list_position = current_list_length;
            let guard=self.mutable.lock().unwrap();
            Ok(MutTxn {
                env:self,
                lock:self.lock.clone(),
                mutable:guard,
                last_page:if last_page == 0 { PAGE_SIZE as u64 } else { last_page },
                current_list_page:current_list_page,
                current_list_length:current_list_length,
                current_list_position:current_list_position, // position of the word immediately after the top.
                occupied_clean_pages:HashSet::new(),
                free_clean_pages:Vec::new(),
                free_pages:Vec::new()
            })
        }
    }

    /// Compute statistics about pages. This is a potentially costlty operation, as we need to go through all bookkeeping pages.
    pub fn statistics(&self)->Statistics{
        unsafe {
            let total_pages = readle_64(self.map) as usize;
            let mut free_pages = HashSet::new();
            let mut bookkeeping_pages = Vec::new();
            let mut cur=readle_64(self.map.offset(8));
            loop {
                bookkeeping_pages.push(cur);
                let p=self.map.offset(cur as isize);
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
                if prev==0 { break } else {
                    cur=prev
                }
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
    pub data:*mut u8,
    pub len:usize
}

// Any other definition for MutPage breaks transmutes.
pub struct MutPage{page:Page}

impl Page {
    pub unsafe fn as_slice<'a>(&'a self)->&'a[u8]{
        std::slice::from_raw_parts(self.data as *const u8,self.len as usize)
    }
    pub fn free(&self,txn:&mut MutTxn) {
        let offset=(self.data as usize as u64) - (txn.env.map as usize as u64);
        // If this page was allocated during this transaction
        if txn.occupied_clean_pages.remove(&offset) {
            txn.free_clean_pages.push(offset);
        } else {
            // Else, register it for freeing (we cannot reuse it in this transaction).
            txn.free_pages.push(offset)
        }
    }
}

impl MutPage {
    pub unsafe fn as_slice<'a>(&'a self)->&'a[u8] { self.page.as_slice() }
    pub unsafe fn as_mut_slice<'a>(&'a mut self)->&'a mut [u8]{
        std::slice::from_raw_parts_mut(self.page.data as *mut u8,self.page.len)
    }
    pub fn free(&self,txn:&mut MutTxn) {
        self.page.free(txn)
    }
}


pub struct MutPages<'a> { pages:Pages<'a> }

pub struct Pages<'a> {
    map:*mut u8,
    len:usize,
    pages:PhantomData<&'a()>
}

impl<'a> Drop for Pages<'a> {
    fn drop(&mut self) {
        let mut memory=self.map;
        let mut l=0;
        while l<self.len {
            unsafe {
                munmap(memory as *mut c_void,PAGE_SIZE);
                l+=PAGE_SIZE;
                memory=memory.offset(PAGE_SIZE as isize)
            }
        }
    }
}


impl <'env>MutTxn<'env> {

    /// Pop a free page from the list of free pages.
    fn free_pages_pop(&mut self)->Option<u64> {
        unsafe {
            debug!("free_pages_pop, current_list_position:{}",self.current_list_position);
            if self.current_list_position==0 {
                let previous_page = readle_64(self.env.map.offset(self.current_list_page as isize));
                debug!("free_pages_pop, previous page:{}",previous_page);
                if previous_page == 0 {
                    None
                } else {
                    // free page, move to previous one and call recursively.
                    self.free_pages.push(self.current_list_page);
                    self.current_list_length = readle_64(self.env.map.offset(self.current_list_page as isize + 8));
                    self.current_list_page = previous_page;

                    self.free_pages_pop()
                }
            } else {
                let cur=self.current_list_page;
                let pos=self.current_list_position;
                // find the page at the top.
                self.current_list_position -= 8;
                debug!("free_pages_pop, new position:{}",self.current_list_position);
                Some(readle_64(self.env.map.offset((cur + 8 + pos) as isize)))
            }
        }
    }
    /// Allocate a single page.
    pub fn alloc_page(&mut self)->Option<MutPage> {
        debug!("alloc page");
        unsafe {
            // If we have allocated and freed a page in this transaction, use it first.
            if let Some(page)=self.free_clean_pages.pop() {
                debug!("clean page reuse:{}",page);
                Some(MutPage{page:Page {
                    data:self.env.map.offset(page as isize),
                    len:PAGE_SIZE as usize,
                }})
            } else {
                // Else, if there are free pages, take one.
                if let Some(page)=self.free_pages_pop() {
                    debug!("using an old free page: {}",page);
                    self.occupied_clean_pages.insert(page);
                    Some(MutPage{page:Page {
                        data:self.env.map.offset(page as isize),
                        len:PAGE_SIZE as usize,
                    }})
                } else {
                    // Else, allocate in the free space.
                    let last=self.last_page;
                    debug!("eating the free space: {}",last);
                    self.last_page += PAGE_SIZE as u64;
                    Some(MutPage{page:Page {
                        data:self.env.map.offset(last as isize),
                        len:PAGE_SIZE as usize
                    }})
                }
            }
        }
    }

    pub fn load_page(&self,off:u64)->Page {
        unsafe {
            Page { data:self.env.map.offset(off as isize),
                   len:PAGE_SIZE }
        }
    }
    pub fn glue_mut_pages<'a>(&self,pages:&'a[MutPage])->Result<MutPages<'a>,Error> {
        unsafe {
            self.glue_pages(std::mem::transmute(pages)).and_then(|x| Ok(MutPages {pages:x}))
        }
    }
    pub fn glue_pages<'a>(&self,pages:&'a[Page])->Result<Pages<'a>,Error> {
        let mut memory=std::ptr::null_mut();
        let mut p0=std::ptr::null_mut();
        let mut l=0;
        for p in pages {
            unsafe {
                memory=libc::mmap(memory.offset(PAGE_SIZE as isize) as *mut c_void,
                                  PAGE_SIZE as size_t,
                                  PROT_READ|PROT_WRITE,
                                  MAP_SHARED | MAP_FIXED,
                                  self.env.fd,

                                  (p.data as usize - self.env.map as usize) as off_t

                                  ) as *mut u8;
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

            // Trick here: we want to merge the last free page with the blank space, but since the following while loop can allocate pages, there is a risk the blank space gets overwritten. Therefore, we'll change the value of last_free_page (in page 0) only if a page has been freed next to it, and none has been allocated in the blank space.
            let mut last_freed_page = 0;

            while ! (self.free_pages.is_empty() && self.free_clean_pages.is_empty()) {
                debug!("commit: pushing");
                // If page is full, or this is the first page, allocate new page.
                if current_page.is_null() {
                    debug!("commit: current is null");
                    // First page, copy-on-write
                    let new_page = self.alloc_page().unwrap();
                    if self.current_list_page != 0 {
                        debug!("Copying from {} to {}",
                               self.current_list_page,
                               (new_page.page.data as usize as u64) - (self.env.map as usize as u64));
                        copy_nonoverlapping(self.env.map.offset(self.current_list_page as isize),
                                            new_page.page.data,
                                            16 + self.current_list_position as usize);
                        writele_64(new_page.page.data.offset(8), self.current_list_position);
                        self.free_pages.push(self.current_list_page);
                        let off=readle_64(new_page.page.data);
                        let len=readle_64(self.env.map.offset(self.current_list_page as isize + 8));
                        debug!("off={}, len={}",off,len);
                    } else {
                        debug!("commit: allocate");
                        writele_64(new_page.page.data, 0); // previous page: none
                        writele_64(new_page.page.data.offset(8), 0); // len: 0
                    }
                    current_page = new_page.page.data
                } else {
                    debug!("commit: current is not null");
                    let len=readle_64(current_page.offset(8));
                    if len + 24 > PAGE_SIZE as u64 {
                        debug!("commit: current is full, len={}",len);
                        // 8 more bytes wouldn't fit in this page, time to allocate a new one
                        let new_page = self.alloc_page().unwrap();

                        // Write a reference to the current page (which cannot be null).
                        writele_64(new_page.page.data, current_page as usize as u64 - self.env.map as usize as u64);
                        // Write the length of the new page (0).
                        writele_64(new_page.page.data.offset(8), 0);

                        current_page = new_page.page.data
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
                *self.lock.write().unwrap();
                if last_freed_page == self.last_page - PAGE_SIZE as u64 {
                    writele_64(self.env.map,last_freed_page);
                } else {
                    writele_64(self.env.map,self.last_page);
                }
                if !current_page.is_null() {
                    writele_64(self.env.map.offset(8),
                               current_page as usize as u64 - self.env.map as usize as u64);
                }
            }
        }
        // Now commit in order.
        {
            let mut ok= unsafe {libc::msync(self.env.map.offset(PAGE_SIZE as isize) as *mut c_void,
                                            (self.env.length - PAGE_SIZE as u64) as size_t,MS_SYNC) };
            if ok!=0 {
                return Err(Error::IO(std::io::Error::last_os_error()))
            } else {
                ok= unsafe {libc::msync(self.env.map as *mut c_void,PAGE_SIZE as size_t,MS_SYNC) };
                if ok!=0 {
                    return Err(Error::IO(std::io::Error::last_os_error()))
                } else {
                    Ok(())
                }
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
            libc::munmap(self.map as *mut c_void,self.length as size_t);
            libc::close(self.fd);
        }
    }
}
