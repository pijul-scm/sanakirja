extern crate libc;
use libc::*;
use std::ffi::CString;
use std::ptr::copy_nonoverlapping;


// Encoding of a page:
// - one byte indicating whether the page (0) starts a blank space, (1) is free or (2) is occupied

fn page_starts_blank(flag:u8)->bool {
    flag==0
}

const PAGE_HEADER_SIZE:usize=
    1 // page type
    +8 // next_free_page (64 bits) ;
    ;

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error)
}

pub struct Env {
    page_size:off_t,
    length:off_t,
    map:*mut u8,
    fd:c_int,
    first_free_page:u64
}

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
                        first_free_page:first_free_page
                    })
                }
            }
        }
    }

    fn alloc_page<'a>(&'a mut self)->Option<Page> {
        unsafe {
            let ptr=self.map.offset(self.first_free_page as isize);
            if page_starts_blank(*ptr) {

                self.first_free_page += self.page_size as u64;

                writebe_64(ptr.offset(1), self.first_free_page);
                Some(Page { page:ptr,page_size:self.page_size as usize })
            } else {
                let offset_next_page = readbe_64(ptr.offset(1));
                self.first_free_page = offset_next_page;
                Some(Page { page:ptr,page_size:self.page_size as usize })
            }
        }
    }

    fn free_page<'a>(&'a mut self,p:Page) {
        unsafe {
            if page_starts_blank(*((p.page as *mut u8).offset(self.page_size as isize))) {
                *p.page = 0;
                self.first_free_page = (p.page as *mut u8 as u64) - (self.map as u64)
            } else {
                *p.page = 1;
                writebe_64((p.page as *mut u8).offset(1), self.first_free_page);
                self.first_free_page = (p.page as *mut u8 as u64) - (self.map as u64)
            }
        }
    }
}

// Pages don't borrow their environment, just as array cells don't borrow the whole array
struct Page {
    page:*mut u8,
    page_size:usize
}

impl Page {
    unsafe fn as_slice<'a>(&'a self)->&'a[u8]{
        std::slice::from_raw_parts((self.page as *const u8).offset(PAGE_HEADER_SIZE as isize),
                                   self.page_size as usize - PAGE_HEADER_SIZE)
    }
    unsafe fn as_mut_slice<'a>(&'a mut self)->&'a mut [u8]{
        std::slice::from_raw_parts_mut((self.page as *mut u8).offset(PAGE_HEADER_SIZE as isize),
                                       self.page_size as usize - PAGE_HEADER_SIZE)
    }
}

impl Drop for Env {
    fn drop(&mut self){
        unsafe {
            //libc::msync(self.map as *mut c_void,self.length as size_t,MS_SYNC);
            libc::munmap(self.map as *mut c_void,self.length as size_t);
            libc::close(self.fd);
        }
    }
}

fn main(){
    let mut env=Env::new("/tmp/test").unwrap();
    {
        let mut page=env.alloc_page().unwrap();
        let mut p=unsafe { page.as_mut_slice() };
        let v=b"blabla";
        for i in 0..v.len() {
            p[i]=v[i]
        }
    }
    let mut page=env.alloc_page().unwrap();
    {
        let mut p=unsafe { page.as_mut_slice() };
        let v=b"blibli";
        for i in 0..v.len() {
            p[i]=v[i]
        }
    }
    env.free_page(page);
}
