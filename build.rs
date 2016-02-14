use std::io::prelude::*;
use std::fs::File;

extern crate libc;
use libc::*;

fn main(){
    let page_size = unsafe {sysconf(_SC_PAGESIZE) as u64 };
    if page_size==4096 {
        let mut f=File::create("src/constants.rs").unwrap();
        writeln!(f,"pub const PAGE_SIZE:usize={};",page_size).unwrap();
    } else {
        panic!("This library is not yet supported on your platform.")
    }
}
