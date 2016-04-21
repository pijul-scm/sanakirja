use super::transaction;
use std;
use std::path::Path;
use super::transaction::{PAGE_SIZE};
use std::fs::File;
use std::io::BufWriter;
use std::collections::HashSet;
use std::ptr::copy_nonoverlapping;
use std::io::Write;
use std::fmt;
use std::cmp::Ordering;

use rustc_serialize::hex::ToHex;

// Guarantee: there are at least 2 bindings per page.
const BINDING_HEADER_SIZE: usize = 16; // each binding on B tree pages requires 16 bytes of header.
pub const INITIAL_HEADER_SIZE: usize = 24; // The first binding has an extra 4+4 bytes.

pub const MAX_KEY_SIZE: usize = (PAGE_SIZE >> 3);
pub const VALUE_SIZE_THRESHOLD: usize = (PAGE_SIZE >> 3) - BINDING_HEADER_SIZE;

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
        Ok(MutPage { page: page })
    }
    #[doc(hidden)]
    pub fn load_cow_page(&mut self, off: u64) -> Cow {
        Cow { cow: self.txn.load_cow_page(off) }
    }
    #[doc(hidden)]
    pub fn rc(&self) -> Option<Db> {
        let rc = self.txn.root(REFERENCE_COUNTS);
        if rc == 0 {
            None
        } else {
            Some(Db { root_num:REFERENCE_COUNTS, root: rc })
        }
    }
    #[doc(hidden)]
    pub fn set_rc(&mut self, db:Db) {
        self.txn.set_root(REFERENCE_COUNTS, db.root)
    }


    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db: &Db, p: P, keys_hex:bool, values_hex:bool) {
        debug(self, db, p, keys_hex, values_hex)
    }
}

impl<'env> Txn<'env> {
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db: &Db, p: P, keys_hex:bool, values_hex:bool) {
        debug(self, db, p, keys_hex, values_hex)
    }
}

#[derive(Clone,Copy,Debug)]
pub enum UnsafeValue {
    S { p:*const u8,
        len:u32 },
    O { offset: u64,
        len: u32 }
}

/// Iterator over parts of a value. On values of size at most 1024 bytes, the iterator will run exactly once. On larger values, it returns all parts of the value, in order.
pub struct Value<'a,T:'a> {
    #[doc(hidden)]
    pub txn:Option<&'a T>,
    #[doc(hidden)]
    pub value:UnsafeValue
}
impl <'a,T:LoadPage>fmt::Debug for Value<'a,T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let it = Value { txn:self.txn, value:self.value.clone() };
        try!(write!(f,"Value {{ value: ["));
        let mut first = true;
        for x in it {
            if !first {
                try!(write!(f, ", {:?}", x))
            } else {
                try!(write!(f, "{:?}", x));
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
        match self.value {
            UnsafeValue::O { ref mut offset, ref mut len } => {
                debug!("iterator: {:?}, {:?}", offset, len);
                if *len == 0 {
                    None
                } else {
                    unsafe {
                        let page = self.txn.unwrap().load_page(*offset).offset(0);
                        // change the pointer of "current page" to the next page
                        let next_offset = u64::from_le(*(page as *const u64));
                        //println!("current={:?}, next_offset:{:?}", *offset, next_offset);
                        //
                        if next_offset != 0 {
                            *offset = next_offset;
                            *len -= (PAGE_SIZE-VALUE_HEADER_LEN) as u32;
                            Some(std::slice::from_raw_parts(page.offset(VALUE_HEADER_LEN as isize), PAGE_SIZE-VALUE_HEADER_LEN))
                        } else {
                            let slice=std::slice::from_raw_parts(page.offset(VALUE_HEADER_LEN as isize), *len as usize);
                            *len = 0;
                            Some(slice)
                        }
                    }
                }
            },
            UnsafeValue::S{ref mut p,ref mut len} => {
                if (*p).is_null() {
                    None
                } else {
                    let pp = *p;
                    unsafe {
                        let l = if *len > PAGE_SIZE as u32 - VALUE_HEADER_LEN as u32 {
                            *p = ((*p) as *mut u8).offset(PAGE_SIZE as isize - VALUE_HEADER_LEN as isize);
                            *len -= (PAGE_SIZE - VALUE_HEADER_LEN) as u32;
                            PAGE_SIZE-VALUE_HEADER_LEN
                        } else {
                            *p = std::ptr::null_mut();
                            let l = *len;
                            *len = 0;
                            l as usize
                        };
                        Some(std::slice::from_raw_parts(pp,l as usize))
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
        self.value.len()
    }
    pub fn clone(&self) -> Value<'a,T> {
        Value { txn:self.txn, value: self.value.clone() }
    }
    pub fn from_slice(slice:&'a[u8]) -> Value<'a,T> {
        Value { txn: None, value: UnsafeValue::S { p:slice.as_ptr(), len:slice.len() as u32 } }
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
        (std::slice::from_raw_parts((p as *const u8).offset(24 + val_len as isize), key_len as usize),
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

#[derive(PartialEq,Debug)]
pub enum Iterate {
    NotStarted,
    Started,
    Finished
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
        //println!("get from page {:?}", page);
        let mut current_off = FIRST_HEAD;
        let mut current = page.offset(current_off as isize) as *const u16;
        let mut level = N_LEVELS-1;
        let mut next_page = 0;
        let mut equal:Option<UnsafeValue> = None;
        loop {
            // advance in the list until there's nothing more to do.
            loop {
                let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
                if next == NIL {
                    break
                } else {
                    let next_ptr = page.offset(next as isize);
                    let (next_key,next_value) = read_key_value(next_ptr);
                    /*println!("cmp {:?} {:?}",
                             std::str::from_utf8_unchecked(key),
                             std::str::from_utf8_unchecked(next_key));*/
                    match key.cmp(next_key) {
                        Ordering::Less => break,
                        Ordering::Equal =>
                            if let Some(value) = value {
                                match (Value{txn:Some(self),value:value}).cmp(Value{txn:Some(self),value:next_value}) {
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
            if level == 0 {
                next_page = u64::from_le(*((current as *const u64).offset(2)));
                break
            } else {
                level -= 1
            }
        }
        if next_page > 0 {
            let next_page = self.load_page(next_page);
            self.get_(next_page, key, value).or(equal)
        } else {
            equal
        }
    }

    unsafe fn iterate_<'a, F: FnMut(&'a [u8], Value<'a,Self>) -> bool>(&'a self,
                                                                    mut state: Iterate,
                                                                    page: Page,
                                                                    key: &[u8],
                                                                    value: Option<UnsafeValue>,
                                                                    f: &mut F) -> Iterate {
        let mut current_off = FIRST_HEAD;
        let mut current = page.offset(current_off as isize) as *const u16;
        let mut level = N_LEVELS-1;
        let mut next_page = u64::from_le(*((current as *const u64).offset(2)));
        // First mission: find first element.
        if state == Iterate::NotStarted {
            loop {
                // advance in the list until there's nothing more to do.
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
                                    match (Value{txn:Some(self),value:value}).cmp(Value{txn:Some(self),value:next_value}) {
                                        Ordering::Less => break,
                                        Ordering::Equal => break,
                                        Ordering::Greater => {
                                            current_off = next;
                                            current = page.offset(current_off as isize) as *const u16;
                                        }
                                    }
                                } else {
                                    break
                                },
                            Ordering::Greater => {
                                current_off = next;
                                current = page.offset(current_off as isize) as *const u16;
                            }
                        }
                    }
                }
                if level == 0 {
                    break
                } else {
                    level -= 1
                }
            }
        }
        // Here, we know that "key" is smaller than or equal to the next element.
        loop {
            debug!("page {:?}, current: {:?} state: {:?}", page.page_offset(), current_off, state);
            next_page = u64::from_le(*((current as *const u64).offset(2)));
            if next_page>0 {
                let next_page = self.load_page(next_page);
                state = self.iterate_(state, next_page, key, value, f);
            }
            current_off = u16::from_le(*current);
            if current_off == NIL {
                break
            }
            current = page.offset(current_off as isize) as *const u16;
            next_page = u64::from_le(*((current as *const u64).offset(2)));
            state = Iterate::Started;
            // On the first time, the "current" entry must not be included.
            let (key,value) = read_key_value(current as *const u8);
            let continue_ = f(key,Value{ txn:Some(self), value:value });
            if ! continue_ {
                state = Iterate::Finished;
                break
            }
        }
        state
    }

    // In iterators, the page stack stores a list of pages from the
    // top of the tree down, where each page is stored as a couple,
    // with the page offset in the file (u64), and the current
    // position in that page (u16).
    unsafe fn iter_<'a,'b>(&'a self,
                           page_stack: &'b mut Vec<(u64,u16)>,
                           initial_page: &Page,
                           key:&[u8],
                           value:Option<UnsafeValue>) -> Iter<'a, 'b, Self> {

        page_stack.clear();
        page_stack.push((initial_page.page_offset(), FIRST_HEAD));
        loop {
            let mut next_page = 0;
            {
                let &mut (page_offset, ref mut current_off) = page_stack.last_mut().unwrap();
                let page = self.load_page(page_offset);
                let mut current = page.offset(*current_off as isize) as *const u16;
                let mut level = N_LEVELS-1;
                
                // First mission: find first element.
                loop {
                    // advance in the list until there's nothing more to do.
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
                                        match (Value{txn:Some(self),value:value}).cmp(Value{txn:Some(self),value:next_value}) {
                                            Ordering::Less => break,
                                            Ordering::Equal => break,
                                            Ordering::Greater => {
                                                *current_off = next;
                                                current = page.offset(*current_off as isize) as *const u16;
                                            }
                                        }
                                    } else {
                                        break
                                    },
                                Ordering::Greater => {
                                    *current_off = next;
                                    current = page.offset(*current_off as isize) as *const u16;
                                }
                            }
                        }
                    }
                    if level == 0 {
                        let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
                        *current_off = next;
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
                page_stack.push((next_page, FIRST_HEAD));
            }
        }
        Iter { txn:self,page_stack:page_stack }
    }
    
}

pub struct Iter<'a, 'b, T:'a> {
    txn:&'a T, page_stack:&'b mut Vec<(u64, u16)>
}

impl<'a,'b,T:LoadPage+'a> Iterator for Iter<'a,'b,T> {
    type Item = (&'a[u8], Value<'a,T>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.page_stack.len() == 0 {
            None
        } else {
            unsafe {
                let &(page_off, current_off) = self.page_stack.last().unwrap();

                // the binding at current_off is the next one to be sent.
                let page = self.txn.load_page(page_off);
                let current = page.offset(current_off as isize) as *const u16;
                if current_off == NIL {
                    self.page_stack.pop();
                    self.next()
                } else {
                    // We set the page stack to the next binding, and return the current one.
                    
                    // Move the top of the stack to the next binding.
                    {
                        let &mut (_, ref mut current_off) = self.page_stack.last_mut().unwrap();
                        *current_off = u16::from_le(*(current as *const u16));
                    }
                    // If there's a page below, push it: the next element is there.
                    let mut next_page = u64::from_le(*((current as *const u64).offset(2)));
                    if next_page != 0 {
                        self.page_stack.push((next_page,FIRST_HEAD));
                    }

                    // Now, return the current element. If we're inside the page, there's an element to return.
                    if current_off > FIRST_HEAD {
                        let (key,value) = read_key_value(current as *const u8);
                        Some((key, Value { txn:Some(self.txn), value:value }))
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


// Page layout: Starts with a header of 32 bytes.
// - 64 bits: RC
// - 5*16 bits: pointers to all the skip lists.
// - 16 bits: offset of the first free spot, from the byte before
// - 16 bits: how much space is occupied in this page? (controls compaction)
// - 16 bits: padding
// - 64 bits: smaller child
// - beginning of coding space (different encodings in B-nodes and B-leaves)


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

    // Value of the page's reference counter.
    fn rc(&self) -> u64 {
        unsafe { u64::from_le(*(self.data())) }
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
            debug_assert!(size & 7 == 0); // 32 bits aligned.
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
            let ptr = self.offset(off_ptr as isize) as *mut u8;
            *((ptr as *mut u16).offset(5)) = (key_len as u16).to_le();
            let target_key_ptr = match value {
                UnsafeValue::S { p,len } => {
                    *((ptr as *mut u32).offset(3)) = len.to_le();
                    copy_nonoverlapping(p,(ptr as *mut u8).offset(24), len as usize);
                    (ptr as *mut u8).offset(24 + len as isize)
                },
                UnsafeValue::O { offset,len } => {
                    *((ptr as *mut u32).offset(3)) = len.to_le();
                    *((ptr as *mut u64).offset(3)) = offset.to_le();
                    (ptr as *mut u8).offset(32)
                }
            };
            copy_nonoverlapping(key_ptr, target_key_ptr, key_len);
        }
    }
    pub unsafe fn reset_pointers(&mut self, off_ptr:u16) {
        let ptr = self.offset(off_ptr as isize) as *mut u8;
        *(ptr as *mut u16) = NIL;
        *((ptr as *mut u16).offset(1)) = NIL;
        *((ptr as *mut u16).offset(2)) = NIL;
        *((ptr as *mut u16).offset(3)) = NIL;
        *((ptr as *mut u16).offset(4)) = NIL;
        *((ptr as *mut u64).offset(2)) = 0;
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
}

#[cfg(debug_assertions)]
fn debug<P: AsRef<Path>, T: LoadPage>(t: &T, db: &Db, p: P, keys_hex:bool, values_hex:bool) {
    let page = t.load_page(db.root);
    let f = File::create(p.as_ref()).unwrap();
    let mut buf = BufWriter::new(f);
    writeln!(&mut buf, "digraph{{").unwrap();
    let mut h = HashSet::new();
    fn print_page<T: LoadPage>(txn: &T,
                               keys_hex:bool,values_hex:bool,
                               pages: &mut HashSet<u64>,
                               buf: &mut BufWriter<File>,
                               p: &Page,
                               print_children: bool) {
        if !pages.contains(&p.page.offset) {
            pages.insert(p.page.offset);
            if print_children {
                writeln!(buf,
                         "subgraph cluster{} {{\nlabel=\"Page {}, first_free {}, occupied {}\";\ncolor=black;",
                         p.page.offset,
                         p.page.offset,
                         p.first_free(),
                         p.occupied())
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

    fn print_tree<T: LoadPage>(txn: &T,
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
                    //println!("key,value = ({:?},{:?})", key.as_ptr(), value.len());
                    let key =
                        if keys_hex {
                            key.to_hex()
                        } else {
                            let key = std::str::from_utf8_unchecked(&key[0..(std::cmp::min(20,key.len()))]);
                            key.to_string()
                        };
                    let value = {
                        let mut value_ = Vec::new();
                        let mut value = Value { txn:Some(txn),value:value };
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
                    //debug!("print_tree, next_page = {:?}", next_page);
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
    print_page(t, keys_hex, values_hex, &mut h, &mut buf, &page, true /* print children */);
    writeln!(&mut buf, "}}").unwrap();
}

pub fn record_size(key: usize, value: usize) -> u16 {
    if value < VALUE_SIZE_THRESHOLD {
        let size = 24 + key as u16 + value as u16;
        size + ((8 - (size & 7)) & 7) // 64-bit alignment.
    } else {
        let size = 24 + key as u16 + 8;
        size + ((8 - (size & 7)) & 7)
    }
}
