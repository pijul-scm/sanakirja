extern crate libc;
use libc::*;

fn main(){
    let page_size = unsafe {sysconf(_SC_PAGESIZE) as u64 };
    if page_size!=4096 && page_size != 8192 {
        panic!("This library is not yet supported on your platform, page size is exotic. Maybe it works, maybe not.")
    };
    //println!("cargo:rustc-flags=-l dylib=lmdb")
}
