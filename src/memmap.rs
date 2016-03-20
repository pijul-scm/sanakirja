use libc;
use libc::{c_void, size_t, off_t, PROT_WRITE, PROT_READ, MAP_SHARED, MAP_FIXED, c_int};
use std;

pub unsafe fn mmap(fd: c_int, addr: Option<*mut u8>, offset: u64, length: u64) -> *mut u8 {
    let e = libc::mmap(addr.unwrap_or(std::ptr::null_mut()) as *mut c_void,
                       length as size_t,
                       PROT_READ | PROT_WRITE,
                       if addr.is_none() {
                           MAP_SHARED
                       } else {
                           MAP_SHARED | MAP_FIXED
                       },
                       fd,
                       offset as off_t);
    if e == libc::MAP_FAILED {
        std::ptr::null_mut()
    } else {
        e as *mut u8
    }
}
/*
pub unsafe fn munmap(addr: *mut u8, length: u64) {
    libc::munmap(addr as *mut c_void, length as size_t);
}
*/
