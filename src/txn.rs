use super::transaction;
use std;
use std::path::Path;
use super::transaction::{PAGE_SIZE,PAGE_SIZE_16, PAGE_SIZE_64};
use std::fs::File;
use std::io::BufWriter;
use std::collections::HashSet;
use std::ptr::copy_nonoverlapping;
use std::io::Write;
use std::fmt;
use std::cmp::Ordering;
#[cfg(debug_assertions)]
use rustc_serialize::hex::ToHex;

// Guarantee: there are at least 4 bindings per page.
const BINDING_HEADER_SIZE: usize = 16; // each binding on B tree pages requires 16 bytes of header.

pub const MAX_KEY_SIZE: usize = (PAGE_SIZE >> 3);
pub const VALUE_SIZE_THRESHOLD: usize = (PAGE_SIZE >> 3) - BINDING_HEADER_SIZE - 6; // 6 is the page header size (24) divided by 4.

pub const NIL:u16 = 0xffff;
pub const FIRST_HEAD:u16 = 0;
pub const N_LEVELS:usize = 5;
pub const VALUE_HEADER_LEN:usize = 8;

#[derive(Debug)]
/// A database identifier. A `Db` can be reused in any number of transactions belonging to the same environment.
pub struct Db {
    #[doc(hidden)]
    pub root: u64,
    #[doc(hidden)]
    pub root_num: isize
}

impl Db {
    pub unsafe fn clone(&self) -> Db {
        Db { root:self.root, root_num:self.root_num }
    }
    pub unsafe fn from_value(v:&[u8]) -> Db {
        let root = u64::from_le(*(v.as_ptr() as *const u64));
        Db { root:root, root_num: -1 }
    }
}


/// Mutable transaction
pub struct MutTxn<'env,T> {
    #[doc(hidden)]
    pub txn: transaction::MutTxn<'env,T>,
    #[doc(hidden)]
    pub protected_pages: [u64;2],
    #[doc(hidden)]
    pub free_protected: [bool;2]
}

impl<'env,T> Drop for MutTxn<'env,T> {
    fn drop(&mut self) {
        debug!("dropping muttxn");
        std::mem::drop(&mut self.txn)
    }
}


/// Immutable transaction
pub struct Txn<'env> {
    pub txn: transaction::Txn<'env>,
}

type Error = transaction::Error;

const REFERENCE_COUNTS:isize = 0;
// pub const MAIN_ROOT:usize = 1;

impl<'env,T> MutTxn<'env,T> {
    #[doc(hidden)]
    pub fn alloc_page(&mut self) -> Result<MutPage,transaction::Error> {
        let page = try!(self.txn.alloc_page());
        // debug!("txn.alloc_page: {:?}", page.offset);
        Ok(MutPage { page: page })
    }
    #[doc(hidden)]
    pub fn load_cow_page(&mut self, off: u64) -> Cow {
        Cow { cow: self.txn.load_cow_page(off) }
    }
    #[doc(hidden)]
    pub fn set_rc(&mut self, db:Db) {
        self.txn.set_root(REFERENCE_COUNTS, db.root)
    }


    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db: &[&Db], p: P, keys_hex:bool, values_hex:bool) {
        debug(self, db, p, keys_hex, values_hex)
    }
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn debug_concise<P: AsRef<Path>>(&self, db: &[&Db], p: P) {
        debug_concise(self, db, p)
    }
}

impl<'env> Txn<'env> {
    #[cfg(test)]
    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db: &[&Db], p: P, keys_hex:bool, values_hex:bool) {
        debug(self, db, p, keys_hex, values_hex)
    }
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn debug_concise<P: AsRef<Path>>(&self, db: &[&Db], p: P) {
        debug_concise(self, db, p)
    }
}


/// The following structure is meant to iterate through the skip list
/// in a page. More specifically, it goes through all bindings at the
/// specified level.
#[doc(hidden)]
pub struct PageIterator<'a,P:super::txn::P + 'a> {
    pub page:&'a P,
    pub level:usize,
    pub current:u16
}
impl<'a,P:super::txn::P + 'a> PageIterator<'a,P> {
    #[doc(hidden)]
    pub fn new(page:&'a P, level:usize) -> Self {
        unsafe {
            // Skip the first pointer (has no key/value)
            let current = u16::from_le(*(page.offset(FIRST_HEAD as isize) as *const u16));
            PageIterator { page:page, level:level, current:current }
        }
    }
}
impl<'a,P:super::txn::P + 'a> Iterator for PageIterator<'a,P> {
    type Item = (u16, &'a [u8], UnsafeValue, u64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == NIL {
            None
        } else {
            unsafe {
                let current = self.current;
                let (key,value) = read_key_value(self.page.offset(self.current as isize));
                let right_child = u64::from_le(*((self.page.offset(self.current as isize) as *const u64).offset(2)));
                self.current = u16::from_le(*(self.page.offset(self.current as isize) as *const u16));
                Some((current,key,value,right_child))
            }
        }
    }
}


#[derive(Clone,Copy,Debug)]
pub enum UnsafeValue {
    S { p:*const u8,
        len:u32 },
    O { offset: u64,
        len: u32 }
}

/// Iterator over parts of a value. On values of size at most 4096 bytes, the iterator will run exactly once. On larger values, it returns all parts of the value, in order.
#[derive(Clone)]
pub enum Value<'a,T:'a> {
    S { p:*const u8,
        len:u32 },
    O { txn:&'a T,
        offset: u64,
        len: u32 }
}

impl <'a,T:LoadPage>fmt::Debug for Value<'a,T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // let it = Value { txn:self.txn, value:self.value.clone() };
        let it:Value<_> = self.clone();
        try!(write!(f,"Value ({:?}) {{ value: [", self.len()));
        let mut first = true;
        for x in it {
            if !first {
                try!(write!(f, ", {:?}", std::str::from_utf8(x)))
            } else {
                try!(write!(f, "{:?}", std::str::from_utf8(x)));
                first = false;
            }
        }
        try!(write!(f,"] }}"));
        Ok(())
    }
}
impl <'a,T:LoadPage> Iterator for Value<'a,T> {
    type Item = &'a [u8];
    fn next(&mut self)->Option<&'a [u8]> {
        match self {
            &mut Value::O { ref txn, ref mut offset, ref mut len } => {
                debug!("iterator: {:?}, {:?}", offset, len);
                if *len == 0 {
                    None
                } else {
                    if *len <= PAGE_SIZE as u32 {
                        unsafe {
                            let page = txn.load_page(*offset).offset(0);
                            let slice=std::slice::from_raw_parts(page.offset(0), *len as usize);
                            *len = 0;
                            Some(slice)
                        }
                    } else {
                        unsafe {
                            let page = txn.load_page(*offset).offset(0);
                            // change the pointer of "current page" to the next page
                            *offset = u64::from_le(*(page as *const u64));
                            let l = PAGE_SIZE-VALUE_HEADER_LEN;
                            *len -= l as u32;
                            Some(std::slice::from_raw_parts(page.offset(VALUE_HEADER_LEN as isize), l as usize))
                        }
                    }
                }
            },
            &mut Value::S{ref mut p,ref mut len} => {
                if *len == 0 {
                    None
                } else {
                    if *len <= PAGE_SIZE as u32 {
                        let l = *len;
                        *len = 0;
                        unsafe {
                            Some(std::slice::from_raw_parts(*p,l as usize))
                        }
                    } else {
                        let pp = *p;
                        unsafe {
                            let l = PAGE_SIZE - VALUE_HEADER_LEN;
                            *p = ((*p) as *mut u8).offset(l as isize);
                            *len -= l as u32;
                            Some(std::slice::from_raw_parts(pp,l as usize))
                        }
                    }
                }
            }
        }
    }
}



impl UnsafeValue {
    pub fn len(&self) -> u32 {
        match self {
            &UnsafeValue::S{len,..} => len,
            &UnsafeValue::O{len,..} => len,
        }
    }
}
impl<'a,T> Value<'a,T> {
    pub fn len(&self) -> u32 {
        match self {
            &Value::S{len,..} => len,
            &Value::O{len,..} => len,
        }
    }

    pub fn clone(&self) -> Value<'a,T> {
        match self {
            &Value::S{ref p, ref len} => Value::S { len:*len, p:*p },
            &Value::O{ref offset, ref len, ref txn} => Value::O { len:*len, offset:*offset, txn:*txn },
        }
    }

    pub unsafe fn from_unsafe(u:&UnsafeValue, txn: &'a T) -> Value<'a,T> {
        match u {
            &UnsafeValue::S{ref p, ref len} => Value::S { len:*len, p:*p },
            &UnsafeValue::O{ref offset, ref len} => Value::O { len:*len, offset:*offset, txn:txn },
        }
    }
    pub fn from_slice(slice:&'a[u8]) -> Value<'a,T> {
        Value::S { p:slice.as_ptr(), len:slice.len() as u32 }
        // Value { txn: None, value: UnsafeValue::S { p:slice.as_ptr(), len:slice.len() as u32 } }
    }
}


// Difference between mutpage and mutpages: mutpages might also contain just one page, but it is unmapped whenever it goes out of scope, whereas P belongs to the main map. Useful for 32-bits platforms.


#[derive(Debug)]
pub struct MutPage {
    pub page: transaction::MutPage,
}
#[derive(Debug)]
pub struct Page {
    pub page: transaction::Page,
}


pub unsafe fn read_key_value<'a>(p: *const u8) -> (&'a [u8], UnsafeValue) {
    let key_len = u16::from_le(*(p as *const u16).offset(5));
    let val_len = u32::from_le(*(p as *const u32).offset(3));

    if (val_len as usize) < VALUE_SIZE_THRESHOLD {
        let padding = (8 - (val_len & 7)) & 7;
        (std::slice::from_raw_parts((p as *const u8).offset((24 + val_len + padding) as isize), key_len as usize),
         UnsafeValue::S { p:(p as *const u8).offset(24), len:val_len })
    } else {
        (std::slice::from_raw_parts((p as *const u8).offset(32), key_len as usize),
         {
             let offset = u64::from_le(*((p as *const u64).offset(3)));
             UnsafeValue::O {
                 offset: offset,
                 len: val_len,
             }
         })
    }
}

pub trait LoadPage:Sized {
    fn length(&self) -> u64;

    fn root_db_(&self,num:isize) -> Option<Db>;

    fn open_db_<'a>(&'a self, root:&Db, key: &[u8]) -> Option<Db> {
        let page = self.load_page(root.root);
        unsafe {
            let db = self.get_(page, key, None);
            if let Some(UnsafeValue::S{p,..}) = db {
                Some(Db { root_num: -1, root: u64::from_le(*(p as *const u64)) })
            } else {
                None
            }
        }
    }

    fn load_page(&self, off: u64) -> Page;

    fn get_u64(&self, db: &Db, key: u64) -> Option<u64> {
        let page = self.load_page(db.root);
        self.get_u64_(page, key)
    }

    fn get_u64_(&self, page:Page, key: u64) -> Option<u64> {
        unsafe {
            let mut key_:[u8;8] = [0;8];
            *(key_.as_mut_ptr() as *mut u64) = key.to_le();
            self.get_(page, &key_[..], None).and_then(
                |x| {
                    if let UnsafeValue::S { p,.. } = x {
                        Some(u64::from_le(*(p as *const u64)))
                    } else {
                        None
                    }
                })
        }
    }

    unsafe fn get_(&self, page:Page, key: &[u8], value:Option<UnsafeValue>) -> Option<UnsafeValue> {
        debug!("sanakirja::get_");
        //println!("get from page {:?}", page);
        let mut current_off = FIRST_HEAD;
        let mut current = page.offset(current_off as isize) as *const u16;
        let mut level = N_LEVELS-1;
        let next_page;
        let mut equal:Option<UnsafeValue> = None;

        let mut last_compared_offset = 0;

        loop {
            // advance in the list until there's nothing more to do.
            loop {
                debug!("current = {:?}", current);
                let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
                if next == NIL {
                    break
                } else {
                    if next == last_compared_offset {
                        // If we didn't move forward in the previous
                        // list, and we're still comparing with the
                        // same key/value, this key/value is <= to the
                        // next one also in this list.
                        break
                    } else {
                        last_compared_offset = next;
                        let next_ptr = page.offset(next as isize);
                        let (next_key,next_value) = read_key_value(next_ptr);
                        debug!("next_value={:?}", next_value);
                        /*println!("cmp {:?} {:?}",
                        std::str::from_utf8_unchecked(key),
                        std::str::from_utf8_unchecked(next_key));*/
                        match key.cmp(next_key) {
                            Ordering::Less => break,
                            Ordering::Equal =>
                                if let Some(value) = value {
                                    match (Value::from_unsafe(&value, self)).cmp(Value::from_unsafe(&next_value, self)) {
                                        Ordering::Less => break,
                                        Ordering::Equal => {
                                            equal = Some(next_value);
                                            break
                                        },
                                        Ordering::Greater => {
                                            current_off = next;
                                            current = page.offset(current_off as isize) as *const u16;
                                        }
                                    }
                                } else {
                                    equal = Some(next_value);
                                    break
                                },
                            Ordering::Greater => {
                                current_off = next;
                                current = page.offset(current_off as isize) as *const u16;
                            }
                        }
                    }
                }
            }
            if level == 0 {
                next_page = u64::from_le(*((current as *const u64).offset(2)));
                break
            } else {
                level -= 1
            }
        }
        debug!("next_page = {:?}", next_page);
        if next_page > 0 {
            let next_page_ = self.load_page(next_page);
            self.get_(next_page_, key, value).or(equal)
        } else {
            equal
        }
    }

    // In iterators, the page stack stores a list of pages from the
    // top of the tree down, where each page is stored as a full u64:
    // the least significant 12 bits encode the offset in the current
    // page, given by the other bits.
    unsafe fn iter_<'a,'b>(&'a self,
                           initial_page: &Page,
                           key:&[u8],
                           value:Option<UnsafeValue>) -> Iter<'a, Self> {

        let mut iter = Iter { txn:self, page_stack:[0;52], stack_pointer: 0 };
        // page_stack.clear();
        iter.push(initial_page.page_offset() | (FIRST_HEAD as u64));
        
        loop {
            let next_page;
            {
                let (page_offset, current_off):(u64,u16) = offsets(iter.page_stack[iter.stack_pointer-1]);

                let page:Page = self.load_page(page_offset);
                let mut current:*const u16 = page.offset(current_off as isize) as *const u16;
                let mut level = N_LEVELS-1;
                
                // First mission: find first element.
                loop {
                    // advance in the list until there's nothing more to do.
                    // Notice that we never push NIL.
                    loop {
                        let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
                        if next == NIL {
                            break
                        } else {
                            let next_ptr = page.offset(next as isize);
                            let (next_key,next_value) = read_key_value(next_ptr);
                            match key.cmp(next_key) {
                                Ordering::Less => break,
                                Ordering::Equal =>
                                    if let Some(value) = value {
                                        match (Value::from_unsafe(&value, self)).cmp(Value::from_unsafe(&next_value, self)) {
                                            Ordering::Less => break,
                                            Ordering::Equal => break,
                                            Ordering::Greater => {
                                                iter.page_stack[iter.stack_pointer-1] = page_offset | (next as u64);
                                                current = page.offset(next as isize) as *const u16;
                                            }
                                        }
                                    } else {
                                        break
                                    },
                                Ordering::Greater => {
                                    iter.page_stack[iter.stack_pointer-1] = page_offset | (next as u64);
                                    current = page.offset(next as isize) as *const u16;
                                }
                            }
                        }
                    }
                    if level == 0 {
                        let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
                        iter.page_stack[iter.stack_pointer-1] = page_offset | (next as u64);
                        next_page = u64::from_le(*((current as *const u64).offset(2)));
                        break
                    } else {
                        level -= 1
                    }
                }
            }
            if next_page == 0 {
                break
            } else {
                iter.push(next_page | (FIRST_HEAD as u64));
            }
        }
        iter
    }
    
    fn rc(&self) -> Option<Db>;
}

pub struct Iter<'a, T:'a> {
    txn:&'a T,
    page_stack:[u64;52],
    stack_pointer:usize
}

impl<'a,T:'a> Iter<'a,T> {
    fn push(&mut self, x:u64) {
        self.page_stack[self.stack_pointer] = x;
        self.stack_pointer += 1
    }
    fn pop(&mut self) -> u64 {
        self.stack_pointer -= 1;
        self.page_stack[self.stack_pointer]
    }
}

fn offsets(x:u64) -> (u64, u16) {
    let mask:u64 = PAGE_SIZE_64-1;
    (x & !mask, (x&mask) as u16)
}

impl<'a,'b,T:LoadPage+'a> Iterator for Iter<'a, T> {
    type Item = (&'a[u8], Value<'a,T>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.stack_pointer == 0 {
            None
        } else {
            unsafe {
                let (page_off, current_off):(u64,u16) = offsets(self.page_stack[self.stack_pointer-1]);
                // println!("page_off = {:?} {:?}", page_off, current_off);
                // the binding at current_off is the next one to be sent.
                if current_off >= 4095 {
                    // println!("pop");
                    self.pop();
                    self.next()
                } else {
                    let page = self.txn.load_page(page_off);
                    let current:*const u16 = page.offset(current_off as isize) as *const u16;

                    // We set the page stack to the next binding, and return the current one.

                    // Move the top of the stack to the next binding.
                    {
                        let next = u16::from_le(*(current as *const u16));
                        let next = std::cmp::min(next, 4095); // Avoid overflow.
                        self.page_stack[self.stack_pointer-1] = page_off | (next as u64);
                    }
                    // If there's a page below, push it: the next element is there.
                    let next_page = u64::from_le(*((current as *const u64).offset(2)));
                    if next_page != 0 {
                        // println!("push");
                        self.push(next_page | (FIRST_HEAD as u64));
                    }
                    
                    // Now, return the current element. If we're inside the page, there's an element to return.
                    if current_off > FIRST_HEAD {
                        let (key,value) = read_key_value(current as *const u8);
                        Some((key, Value::from_unsafe(&value, self.txn)))
                    } else {
                        // Else, we're at the beginning of the page,
                        // the element is either in the page we just
                        // pushed, or (if there's no page below) the
                        // next element.
                        self.next()
                    }
                }
            }
        }
    }
}


pub trait P:std::fmt::Debug {
    /// offset of the page in the file.
    fn page_offset(&self) -> u64;

    /// pointer to the first word of the page.
    fn data(&self) -> *const u64;

    /// 0 if cannot alloc, valid offset else (offset in bytes from the start of the page)
    fn can_alloc(&self, size: u16) -> u16 {
        assert!(size & 7 == 0); // 64 bits aligned.
        if self.occupied() + size < PAGE_SIZE as u16 {
            self.first_free()
        } else {
            0
        }
    }
    // First free spot in this page (head of the linked list, number of |u32| from the last glue.
    fn first_free(&self) -> u16 {
        unsafe {
            let first_free = u16::from_le(*(self.p_first_free()));
            if first_free > 0 {
                first_free
            } else {
                FIRST_HEAD + 24
            }
        }
    }
    fn p_first_free(&self) -> *mut u16 {
        unsafe { ((self.data() as *mut u8).offset(FIRST_HEAD as isize + 10) as *mut u16) }
    }

    fn occupied(&self) -> u16 {
        unsafe {
            let occupied = u16::from_le(*(self.p_occupied()));
            if occupied > 0 {
                occupied
            } else {
                FIRST_HEAD + 24
            }
        }
    }
    fn p_occupied(&self) -> *mut u16 {
        unsafe { ((self.data() as *mut u8).offset(FIRST_HEAD as isize + 12) as *mut u16) }
    }

    // offset in u32.
    fn offset(&self, off: isize) -> *mut u8 {
        unsafe {
            let p = self.data() as *mut u8;
            p.offset(off)
        }
    }
    fn right_child(&self, off:u16) -> u64 {
        assert!(off < PAGE_SIZE_16);
        unsafe {
            u64::from_le(*((self.offset(off as isize) as *const u64).offset(2)))
        }
    }
    fn level(&mut self, off:u16, level:usize) -> u16 {
        assert!(off <= PAGE_SIZE_16);
        unsafe {
            u16::from_le(*((self.offset(off as isize) as *mut u16).offset(level as isize)))
        }
    }
}

impl P for Cow {
    fn data(&self) -> *const u64 {
        match self.cow {
            transaction::Cow::Page(ref p) => p.data as *const u64,
            transaction::Cow::MutPage(ref p) => p.data as *const u64,
        }
    }
    fn page_offset(&self) -> u64 {
        match self.cow {
            transaction::Cow::Page(ref p) => p.offset,
            transaction::Cow::MutPage(ref p) => p.offset,
        }
    }
}

impl P for Page {
    fn page_offset(&self) -> u64 {
        self.page.offset
    }
    fn data(&self) -> *const u64 {
        self.page.data as *mut u64
    }
}

impl P for MutPage {
    fn page_offset(&self) -> u64 {
        self.page.offset
    }
    fn data(&self) -> *const u64 {
        self.page.data as *mut u64
    }
}


impl MutPage {
    pub fn init(&mut self) {
        debug!("mut page init: {:?}",self);
        unsafe {
            std::ptr::write_bytes(self.page.data as *mut u8, 0, FIRST_HEAD as usize);
            let ptr = (self.page.data as *mut u8).offset(FIRST_HEAD as isize) as *mut u16;
            *(ptr as *mut u16) = NIL.to_le();
            *((ptr as *mut u16).offset(1)) = NIL.to_le();
            *((ptr as *mut u16).offset(2)) = NIL.to_le();
            *((ptr as *mut u16).offset(3)) = NIL.to_le();
            *((ptr as *mut u16).offset(4)) = NIL.to_le();
            *((ptr as *mut u16).offset(5)) = 0;
            *((ptr as *mut u16).offset(6)) = 0;
            *((ptr as *mut u16).offset(7)) = 0;
            *((ptr as *mut u64).offset(2)) = 0; // next_page
        }
    }

    /// Takes a size in bytes, returns an offset from the word before
    /// the beginning of the contents (0 is invalid, 1 is the first
    /// offset).
    pub fn alloc(&mut self, first_free: u16, size: u16) {
        unsafe {
            debug_assert!(size & 7 == 0); // 64 bits aligned.
            *(self.p_first_free()) = (first_free + size).to_le();
        }
    }

    // allocate and write key, value, left and right neighbors.
    pub fn alloc_key_value(&mut self,
                           off_ptr: u16,
                           size: u16,
                           key_ptr:*const u8,
                           key_len:usize,
                           value: UnsafeValue) {
        unsafe {
            *(self.p_occupied()) = (self.occupied() + size).to_le();
            self.alloc(off_ptr, size);
            self.write_key_value(off_ptr, key_ptr, key_len, value)
        }
    }
    // allocate and write key, value, left and right neighbors.
    pub fn write_key_value(&mut self,
                           off_ptr: u16,
                           key_ptr:*const u8,
                           key_len:usize,
                           value: UnsafeValue) {
        unsafe {
            let ptr = self.offset(off_ptr as isize) as *mut u8;
            *((ptr as *mut u16).offset(5)) = (key_len as u16).to_le();
            let target_key_ptr = match value {
                UnsafeValue::S { p,len } => {
                    debug_assert!(len < VALUE_SIZE_THRESHOLD as u32);
                    *((ptr as *mut u32).offset(3)) = len.to_le();
                    copy_nonoverlapping(p,(ptr as *mut u8).offset(24), len as usize);

                    let padding = (8 - (len & 7)) & 7;
                    (ptr as *mut u8).offset((24 + len + padding) as isize)
                },
                UnsafeValue::O { offset,len } => {
                    debug!("write_key_value: {:?}", offset);
                    *((ptr as *mut u32).offset(3)) = len.to_le();
                    *((ptr as *mut u64).offset(3)) = offset.to_le();
                    (ptr as *mut u8).offset(32)
                }
            };
            copy_nonoverlapping(key_ptr, target_key_ptr, key_len);
        }
    }
    pub fn reset_pointers(&mut self, off_ptr:u16) {
        assert!(off_ptr + 24 < PAGE_SIZE as u16);
        // println!("resetting pointers for {:?} at {:?}", self.page_offset(), off_ptr);
        unsafe {
            let ptr = self.offset(off_ptr as isize) as *mut u8;
            *(ptr as *mut u16) = NIL;
            *((ptr as *mut u16).offset(1)) = NIL;
            *((ptr as *mut u16).offset(2)) = NIL;
            *((ptr as *mut u16).offset(3)) = NIL;
            *((ptr as *mut u16).offset(4)) = NIL;
            *((ptr as *mut u64).offset(2)) = 0;
        }
    }
    pub fn set_right_child(&self, off:u16, right_child:u64) {
        assert!(off < PAGE_SIZE_16);
        unsafe {
            *((self.offset(off as isize) as *mut u64).offset(2)) = right_child.to_le();
        }
    }
    pub fn set_level(&mut self, off:u16, level:usize, next:u16) {
        assert!(off <= PAGE_SIZE_16 - 16);
        unsafe {
            *((self.offset(off as isize) as *mut u16).offset(level as isize)) = next.to_le();
        }
    }
}


#[derive(Debug)]
pub struct Cow {
    pub cow: transaction::Cow,
}

impl Cow {

    pub fn from_mut_page(p:MutPage)->Cow {
        Cow{cow:transaction::Cow::MutPage(p.page)}
    }

    #[cfg(test)]
    pub fn unwrap_mut(self) -> MutPage {
        match self.cow {
            transaction::Cow::MutPage(p) => MutPage { page: p },
            transaction::Cow::Page(_) => panic!("unwrap")
        }
    }

    pub fn as_nonmut(self) -> Cow {
        match self.cow {
            transaction::Cow::MutPage(p) => Cow { cow: transaction::Cow::Page(p.as_page()) },
            x => Cow { cow: x }
        }
    }
    pub fn as_page(self) -> Page {
        match self.cow {
            transaction::Cow::Page(p) => Page { page: p },
            transaction::Cow::MutPage(p) => Page { page: p.as_page() },
        }
    }
}

impl<'env,T> LoadPage for MutTxn<'env,T> {
    fn length(&self) -> u64 {
        self.txn.env.length
    }
    fn root_db_(&self,num:isize) -> Option<Db> {
        let root = self.txn.root(num);
        if root == 0 {
            None
        } else {
            Some(Db { root_num:num, root: self.txn.root(num) })
        }
    }
    fn load_page(&self, off: u64) -> Page {
        Page { page: self.txn.load_page(off) }
    }

    fn rc(&self) -> Option<Db> {
        let rc = self.txn.root(REFERENCE_COUNTS);
        if rc == 0 {
            None
        } else {
            Some(Db { root_num:REFERENCE_COUNTS, root: rc })
        }
    }
}
impl<'env> LoadPage for Txn<'env> {
    fn length(&self) -> u64 {
        self.txn.env.length
    }
    fn root_db_(&self,num:isize) -> Option<Db> {
        let root = self.txn.root(num);
        if root == 0 {
            None
        } else {
            Some(Db { root_num:num, root: self.txn.root(num) })
        }
    }
    fn load_page(&self, off: u64) -> Page {
        Page { page: self.txn.load_page(off) }
    }

    fn rc(&self) -> Option<Db> {
        let rc = self.txn.root(REFERENCE_COUNTS);
        if rc == 0 {
            None
        } else {
            Some(Db { root_num:REFERENCE_COUNTS, root: rc })
        }
    }
}

#[cfg(debug_assertions)]
fn debug<P: AsRef<Path>, T: LoadPage + super::Transaction>(t: &T, db: &[&Db], p: P, keys_hex:bool, values_hex:bool) {
    let f = File::create(p.as_ref()).unwrap();
    let mut buf = BufWriter::new(f);
    writeln!(&mut buf, "digraph{{").unwrap();
    let mut h = HashSet::new();
    fn print_page<T: LoadPage + super::Transaction>(txn: &T,
                               keys_hex:bool,values_hex:bool,
                               pages: &mut HashSet<u64>,
                               buf: &mut BufWriter<File>,
                               p: &Page,
                               print_children: bool) {
        if !pages.contains(&p.page.offset) {
            pages.insert(p.page.offset);
            if print_children {
                
                let rc = if let Some(rc) = txn.rc() {
                    txn.get_u64(&rc, p.page.offset).unwrap_or(1)
                } else {
                    0
                };

                writeln!(buf,
                         "subgraph cluster{} {{\nlabel=\"Page {}, first_free {}, occupied {}, rc {}\";\ncolor=black;",
                         p.page.offset,
                         p.page.offset,
                         p.first_free(),
                         p.occupied(),
                         rc
                )
                    .unwrap();
            }
            let root = FIRST_HEAD;
            //debug!("print_page: page {:?}", p.page.offset);
            let mut h = Vec::new();
            let mut edges = Vec::new();
            let mut hh = HashSet::new();
            print_tree(txn, keys_hex, values_hex, &mut hh, buf, &mut edges, &mut h, p, root);
            if print_children {
                writeln!(buf, "}}").unwrap();
            }
            for p in edges.iter() {
                writeln!(buf, "{}", p).unwrap()
            }
            if print_children {
                for p in h.iter() {
                    print_page(txn, keys_hex, values_hex, pages, buf, p, true)
                }
            }
        }
    }

    fn print_tree<T: LoadPage + super::Transaction>(txn: &T,
                               keys_hex:bool,values_hex:bool,
                               nodes: &mut HashSet<u16>,
                               buf: &mut BufWriter<File>,
                               edges: &mut Vec<String>,
                               pages: &mut Vec<Page>,
                               p: &Page,
                               off: u16) {
        unsafe {
            //debug!("print tree:{:?}, off={:?}",p, off);
            let ptr = p.offset(off as isize) as *const u32;
            let (key,value) = {
                if off == FIRST_HEAD {
                    ("root".to_string(),"".to_string())
                } else {
                    let (key, value) = read_key_value(ptr as *const u8);
                    // debug!("key,value = ({:?},{:?})", key.as_ptr(), value);
                    let key =
                        if keys_hex {
                            key.to_hex()
                        } else {
                            let key = std::str::from_utf8_unchecked(&key[0..(std::cmp::min(20,key.len()))]);
                            key.to_string()
                        };
                    let value = {
                        if let UnsafeValue::O { ref offset, .. } = value {
                            format!("{:?}(rc = {:?})", offset, super::put::get_rc(txn, *offset))
                        } else {
                            let mut value_=Vec::new();
                            let mut value = Value::from_unsafe(&value, txn);
                            if values_hex {
                                for i in value {
                                    value_.extend(i)
                                }
                                value_.to_hex()
                            } else {
                                let value = if value.len() > 20 {
                                    let contents = value.next().unwrap();
                                    value_.extend(&contents[0..20]);
                                    value_.extend(b"...");
                                    &value_[..]
                                } else {
                                    value.next().unwrap()
                                };
                                std::str::from_utf8_unchecked(value).to_string()
                            }
                        }
                    };
                    (key,value)
                }
            };
            //debug!("key,value={:?},{:?}",key,value);
            writeln!(buf,
                     "n_{}_{}[label=\"{}: '{}'->'{}'\"];",
                     p.page.offset,
                     off,
                     off,
                     key,
                     value)
                .unwrap();
            if !nodes.contains(&off) {
                let next_page = u64::from_le(*((ptr as *const u64).offset(2)));
                if next_page>0 {
                    //debug!("print_tree, page = {:?}, next_page = {:?}", p.page.offset, next_page);
                    pages.push(txn.load_page(next_page));
                    edges.push(format!(
                             "n_{}_{}->n_{}_{}[color=\"red\"];",
                             p.page.offset,
                             off,
                             next_page,
                             FIRST_HEAD))
                };
                nodes.insert(off);
                for i in 0..5 {
                    let left = u16::from_le(*((ptr as *const u16).offset(i)));
                    //debug!("{:?}",((ptr as *const u16).offset(i)));
                    if left != NIL {
                        writeln!(buf,
                                 "n_{}_{}->n_{}_{}[color=\"blue\", label=\"{}\"];",
                                 p.page.offset,
                                 off,
                                 p.page.offset,
                                 left,i)
                            .unwrap();
                        //debug!("print_tree: recursive call");
                        print_tree(txn,keys_hex, values_hex, nodes,buf,edges,pages,p,left)
                    }
                }
            }
            //debug!("/print tree:{:?}",p);
        }
    }
    for db in db {
        let page = t.load_page(db.root);
        print_page(t, keys_hex, values_hex, &mut h, &mut buf, &page, true /* print children */);
    }
    writeln!(&mut buf, "}}").unwrap();
}







#[cfg(debug_assertions)]
fn debug_concise<P: AsRef<Path>, T: LoadPage>(t: &T, db: &[&Db], p: P) {
    let f = File::create(p.as_ref()).unwrap();
    let mut buf = BufWriter::new(f);
    writeln!(&mut buf, "digraph{{").unwrap();
    let mut h = HashSet::new();
    fn print_page<T: LoadPage>(txn: &T,
                               pages: &mut HashSet<u64>,
                               buf: &mut BufWriter<File>,
                               p: &Page) {
        if !pages.contains(&p.page.offset) {
            pages.insert(p.page.offset);
            let rc = if let Some(rc) = txn.rc() {
                txn.get_u64(&rc, p.page.offset).unwrap_or(1)
            } else {
                0
            };
            writeln!(buf,
                     "page_{}[label=\"{}, ff {}, occ {}, rc {}\"];",
                     p.page.offset,
                     p.page.offset,
                     p.first_free(),
                     p.occupied(),
                     rc
            ).unwrap();

            let root = FIRST_HEAD;
            //debug!("print_page: page {:?}", p.page.offset);
            let mut h = Vec::new();
            let mut edges = Vec::new();
            let mut hh = HashSet::new();
            print_tree(txn, &mut hh, &mut edges, buf, &mut h, p, root);
            for edge in edges.iter() {
                writeln!(buf, "{}", edge).unwrap();
            }
            for p in h.iter() {
                print_page(txn, pages, buf, p)
            }
        }
    }

    fn print_tree<T: LoadPage>(txn: &T,
                               nodes: &mut HashSet<u16>,
                               edges:&mut Vec<String>,
                               buf: &mut BufWriter<File>,
                               pages: &mut Vec<Page>,
                               p: &Page,
                               off: u16) {
        unsafe {
            //debug!("print tree:{:?}, off={:?}",p, off);
            let ptr = p.offset(off as isize) as *const u32;
            //debug!("key,value={:?},{:?}",key,value);
            if !nodes.contains(&off) {
                let next_page = u64::from_le(*((ptr as *const u64).offset(2)));
                if next_page>0 {
                    //debug!("print_tree, page = {:?}, next_page = {:?}", p.page.offset, next_page);
                    pages.push(txn.load_page(next_page));
                    edges.push(format!(
                        "page_{}->page_{}[color=\"red\"];",
                        p.page.offset,
                        next_page))
                };
                nodes.insert(off);
                let next = u16::from_le(*((ptr as *const u16).offset(0)));
                //debug!("{:?}",((ptr as *const u16).offset(i)));
                if next != NIL {
                    print_tree(txn, nodes, edges, buf, pages, p, next)
                }
            }
            //debug!("/print tree:{:?}",p);
        }
    }
    for db in db {
        let page = t.load_page(db.root);
        print_page(t, &mut h, &mut buf, &page);
    }
    writeln!(&mut buf, "}}").unwrap();
}





pub fn record_size(key: usize, value: usize) -> u16 {
    if value < VALUE_SIZE_THRESHOLD {
        let key_padding = (8 - (key & 7)) & 7;
        let value_padding = (8 - (value & 7)) & 7;
        (24 + key + key_padding + value + value_padding) as u16
    } else {
        let key_padding = (8 - (key & 7)) & 7;
        (24 + key + 8 + key_padding) as u16
    }
}
