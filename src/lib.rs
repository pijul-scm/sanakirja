extern crate libc;
use libc::*;
use std::ffi::CString;
//use std::ptr::copy_nonoverlapping;


// Encoding of a page:
// 1 - one byte indicating whether the page (0) starts a blank space, (1) is free.
// 2 - offset (u64) of the next free page
// 3 - size of the allocated space (must be a multiple of the page size)

// TODO: compress 1 and 2, page sizes are always even.
// Can we use just a single integer for 2 and 3 ?

fn page_starts_blank(flag:u8)->bool {
    flag==0
}

const PAGE_HEADER_SIZE:usize=
    1 // page type
    +8 // next_free_page (64 bits) ;
    +8 // what's the size of this page ?
    ;

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    MutableTxn
}

// Here is the reasoning:
// - first_free_page is the head of the list of pages that are free. We might or might not have used them during the transaction, before freeing them.
// - first_clean_page is the head of "usable" pages, i.e. pages that we have never used during this transaction.
//
// The guarantee is that no concurrent read has access to any clean page, but they might be reading free pages.

pub struct Env {
    page_size:off_t,
    length:off_t,
    map:*mut u8,
    fd:c_int,
    first_free_page:u64,
    ongoing_mutable_txn:std::sync::Arc<std::sync::Mutex<bool>>,
}

unsafe impl Send for Env {}
unsafe impl Sync for Env {}

pub unsafe fn readbe_64(p0:*mut u8)->u64 {
    let mut acc=0;
    let mut p=p0;
    for _ in 0..8 {
        acc = (acc << 8) | (*p as u64);
        p=p.offset(1);
    }
    acc
}

pub unsafe fn writebe_64(p:*mut u8,mut v:u64) {
    let mut p=p.offset(7);
    while v>0 {
        *p = (v&0xff) as u8;
        v= v >> 8;
        p=p.offset(-1);
    }
}

pub struct Txn<'env> {
    env:&'env Env,
    mutable:std::sync::Arc<std::sync::Mutex<bool>>,
    first_free_page:u64,
    first_clean_page:u64
}

pub struct MutTxn<'env> {
    txn:Txn<'env>
}

impl Env {
    pub fn new(file:&str)->Result<Env,Error> {
        unsafe {
            let page_size =sysconf(_SC_PAGESIZE) as off_t;

            let length=100 * page_size;

            let name=CString::new(file).unwrap();
            let fd=libc::open(name.as_ptr(),O_CREAT|O_RDWR,0o777);
            let ftrunc=libc::ftruncate(fd,length);
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
                    let memory=memory as *mut u8;
                    let first_free_page= if *memory==0 {
                        *memory=1;
                        readbe_64(memory.offset(1))
                    } else {
                        0
                    };
                    Ok(Env {
                        page_size:page_size,
                        length:length,
                        map:memory as *mut u8,
                        fd:fd,
                        first_free_page:first_free_page,
                        ongoing_mutable_txn:std::sync::Arc::new(std::sync::Mutex::new(false))
                    })
                }
            }
        }
    }
    pub fn txn_begin<'env>(&'env self)->Txn<'env> {
        let free=self.first_free_page;
        Txn { env:self, first_free_page:free, first_clean_page:free, mutable:self.ongoing_mutable_txn.clone() }
    }
    pub fn txn_mut_begin<'env>(&'env self)->Result<MutTxn<'env>,Error> {
        let txn=self.txn_begin();
        {
            let mut mutex = txn.mutable.lock().unwrap();
            if *mutex {
                return Err(Error::MutableTxn)
            } else {
                *mutex=true;
            }
        }
        Ok(MutTxn { txn:txn })
    }
}

impl <'env>MutTxn<'env> {
    pub fn alloc_page(&mut self)->Option<Page> {
        unsafe {
            let ptr=self.txn.env.map.offset(self.txn.first_clean_page as isize);
            if page_starts_blank(*ptr) {

                self.txn.first_clean_page += self.txn.env.page_size as u64;

                writebe_64(ptr.offset(1), self.txn.first_clean_page);
                Some(Page { data:ptr,len:self.txn.env.page_size as usize })
            } else {
                let offset_next_page = readbe_64(ptr.offset(1));
                self.txn.first_clean_page = offset_next_page;
                Some(Page { data:ptr,len:self.txn.env.page_size as usize })
            }
        }
    }

    pub fn free_page(&mut self,p:Page) {
        unsafe {
            if page_starts_blank(*((p.data as *mut u8).offset(p.len as isize))) {
                *p.data = 0;
                self.txn.first_free_page = (p.data as *mut u8 as u64) - (self.txn.env.map as u64)
            } else {
                *p.data = 1;
                writebe_64((p.data as *mut u8).offset(1), self.txn.first_free_page);
                self.txn.first_free_page = (p.data as *mut u8 as u64) - (self.txn.env.map as u64)
            }
        }
    }
    pub fn commit(self)->Result<(),Error>{
        let ok= unsafe {libc::msync(self.txn.env.map as *mut c_void,self.txn.env.length as size_t,MS_SYNC) };
        *(self.txn.mutable.lock().unwrap()) = false;
        if ok==0 {
            Ok(())
        } else {
            Err(Error::IO(std::io::Error::last_os_error()))
        }
    }
    pub fn abort(self){
        *(self.txn.mutable.lock().unwrap()) = false;
    }
}

// Pages don't borrow their transaction. We need to mutate the
// environment structure in allocate_page, so the first allocate in a
// transaction would prevent subsequent ones.
//
// This is also why they cannot be slices, although they really look like slices.
pub struct Page {
    pub data:*mut u8,
    pub len:usize
}

impl Page {
    pub unsafe fn as_slice<'a>(&'a self)->&'a[u8]{
        std::slice::from_raw_parts((self.data as *const u8).offset(PAGE_HEADER_SIZE as isize),
                                   self.len as usize - PAGE_HEADER_SIZE)
    }
    pub unsafe fn as_mut_slice<'a>(&'a mut self)->&'a mut [u8]{
        std::slice::from_raw_parts_mut((self.data as *mut u8).offset(PAGE_HEADER_SIZE as isize),
                                       self.len as usize - PAGE_HEADER_SIZE)
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
