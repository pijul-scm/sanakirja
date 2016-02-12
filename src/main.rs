extern crate libc;
use libc::*;
use std::ffi::CString;
use std::ptr::copy_nonoverlapping;

struct Page {
    page:*mut u8
}
#[derive(Debug)]
pub enum Error {
    IO(std::io::Error)
}

pub struct Env {
    page_size:off_t,
    length:off_t,
    map:*mut u8,
    fd:c_int
}

impl Env {
    pub fn new()->Env {
        unsafe {
            let page_size =sysconf(_SC_PAGESIZE) as off_t;
            Env {
                page_size:page_size,
                length:10*page_size,
                map:std::ptr::null_mut(),
                fd:0
            }
        }
    }
    pub fn open(&mut self,file:&str)->Result<(),Error> {
        unsafe {
            let name=CString::new(file).unwrap();
            let fd=libc::open(name.as_ptr(),O_CREAT|O_RDWR,0o777);
            let ftrunc=libc::ftruncate(fd,self.length);
            if ftrunc<0 {
                Err(Error::IO(std::io::Error::last_os_error()))
            } else {
                let memory=libc::mmap(std::ptr::null_mut(),
                                      self.length as size_t,
                                      PROT_READ|PROT_WRITE,
                                      MAP_SHARED,
                                      fd,0);
                if memory==libc::MAP_FAILED {
                    Err(Error::IO(std::io::Error::last_os_error()))
                } else {
                    self.fd=fd;
                    self.map=memory as *mut u8;
                    Ok(())
                }
            }
        }
    }
}

impl Drop for Env {
    fn drop(&mut self){
        unsafe {
            if ! self.map.is_null() {
                //libc::msync(self.map as *mut c_void,self.length as size_t,MS_SYNC);
                libc::munmap(self.map as *mut c_void,self.length as size_t);
                libc::close(self.fd);
            }
        }
    }
}

fn main(){
    let mut env=Env::new();
    env.open("/tmp/test").unwrap();
    // unsafe {
    //     println!("{}",page_size);
    //             let x=std::slice::from_raw_parts(memory as *const u8,10);
    //             println!("Result:{:?}",std::str::from_utf8_unchecked(x));
    //             let v=b"This is a test\n";
    //             copy_nonoverlapping(v.as_ptr(),memory as *mut u8,v.len());
    //         }
    //     }
    // };
}
