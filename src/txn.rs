use super::transaction;
use std;
use std::path::Path;
use libc::c_int;
use super::memmap;
use super::transaction::{PAGE_SIZE, PAGE_SIZE_64};
use std::cmp::Ordering;
use std::fs::File;
use std::io::BufWriter;
use std::collections::HashSet;
use std::ptr::copy_nonoverlapping;
use std::io::Write;
use std::marker::PhantomData;
use std::fmt;

pub const MAX_KEY_SIZE: usize = PAGE_SIZE >> 2;
pub const VALUE_SIZE_THRESHOLD: usize = PAGE_SIZE >> 2;

#[derive(Debug)]
pub struct Db {
    pub root: u64,
}

/// Mutable transaction
pub struct MutTxn<'env> {
    pub txn: transaction::MutTxn<'env>,
    pub btree_root: u64,
}

/// Immutable transaction
pub struct Txn<'env> {
    pub txn: transaction::Txn<'env>,
    pub btree_root: u64,
}

impl<'env> MutTxn<'env> {
    pub fn alloc_page(&mut self) -> MutPage {
        let page = self.txn.alloc_page().unwrap();
        MutPage { page: page }
    }
    pub fn load_cow_page(&mut self, off: u64) -> Cow {
        Cow { cow: self.txn.load_cow_page(off) }
    }
    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db: &Db, p: P) {
        debug(self, db, p)
    }
}

impl<'env> Txn<'env> {
    #[doc(hidden)]
    pub fn debug<P: AsRef<Path>>(&self, db: &Db, p: P) {
        debug(self, db, p)
    }
}
#[derive(Clone,Copy)]
pub enum UnsafeValue {
    S { p:*const u8,
        len:u32 },
    O { offset: u64,
        len: u32 }
}

pub struct Value<'a,T:'a> {
    pub txn:&'a T,
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
                if *len == 0 {
                    None
                } else {
                    unsafe {
                        let page = self.txn.load_page(*offset).offset(0);
                        let first = u64::from_le(*(page as *const u64));
                        *offset = first;
                        if first != 0 {
                            *len -= (PAGE_SIZE-8) as u32;
                            Some(std::slice::from_raw_parts(page, PAGE_SIZE-8))
                        } else {
                            Some(std::slice::from_raw_parts(page, *len as usize))
                        }
                    }
                }
            },
            UnsafeValue::S{ref mut p,ref mut len} => {
                if (*p).is_null() {
                    None
                } else {
                    let pp = *p;
                    *p = std::ptr::null_mut();
                    Some(unsafe {
                        std::slice::from_raw_parts(pp,*len as usize)
                    })
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
}

pub fn alloc_value(txn:&mut MutTxn, value: &[u8]) -> UnsafeValue {
    let mut len = value.len();
    let mut p_value = value.as_ptr();
    let mut ptr = std::ptr::null_mut();
    let mut first_page = 0;
    unsafe {
        while len > 0 {
            let page = txn.alloc_page();
            if !ptr.is_null() {
                *(ptr as *mut u64) = page.page_offset()
            } else {
                first_page = page.page_offset();
            }
            ptr = page.data() as *mut u64;
            *(ptr as *mut u64) = 0;
            if len > PAGE_SIZE-8 {
                copy_nonoverlapping(p_value, (ptr as *mut u64).offset(1) as *mut u8, PAGE_SIZE-8);
                len -= PAGE_SIZE - 8;
                p_value.offset(PAGE_SIZE as isize-8);
            } else {
                copy_nonoverlapping(p_value, (ptr as *mut u64).offset(1) as *mut u8, len);
                len = 0;
            }
        }
    }
    debug_assert!(first_page > 0);
    UnsafeValue::O { offset: first_page, len: value.len() as u32 }
}











// Difference between mutpage and mutpages: mutpages might also contain just one page, but it is unmapped whenever it goes out of scope, whereas P belongs to the main map. Useful for 32-bits platforms.


#[derive(Debug)]
pub struct MutPage {
    pub page: transaction::MutPage,
}
#[derive(Debug)]
pub struct Page {
    page: transaction::Page,
}



pub trait LoadPage:Sized {
    fn fd(&self) -> c_int;
    fn length(&self) -> u64;
    fn root_db_(&self) -> Db;
    fn open_db_<'a>(&'a self, key: &[u8]) -> Option<Db> {
        let db = self.get_(&self.root_db_(), key, None);
        if let Some(UnsafeValue::S{p,..}) = db {
            unsafe { Some(Db { root: u64::from_le(*(p as *const u64)) }) }
        } else {
            None
        }
    }


    unsafe fn read_key_value<'a>(&'a self, p: *const u8) -> (&'a [u8], UnsafeValue) {
        let p32 = p as *const u8 as *const u32;
        let key_len = u16::from_le(*(p32.offset(5) as *const u16));
        let val_len = u32::from_le(*(p32.offset(4)));
        if (val_len as usize) < VALUE_SIZE_THRESHOLD {
            (std::slice::from_raw_parts((p as *const u8).offset(24 + val_len as isize),
                                        key_len as usize),
             UnsafeValue::S { p:(p as *const u8).offset(24), len:val_len })
        } else {
            (std::slice::from_raw_parts((p as *const u8).offset(32), key_len as usize),
             {
                 let offset = u64::from_le(*((p32 as *const u64).offset(3)));
                 UnsafeValue::O {
                     offset: offset,
                     len: val_len,
                 }
             })
        }
    }

    fn load_page(&self, off: u64) -> Page;

    fn get_<'a>(&'a self, db: &Db, key: &[u8], value: Option<&[u8]>) -> Option<UnsafeValue> {
        debug!("db.root={:?}",db.root);
        let root_page = self.load_page(db.root);
        self.binary_tree_get(&root_page, key, value, root_page.root() as u32)
    }

    fn binary_tree_get<'a>(&'a self,
                           page: &Page,
                           key: &[u8],
                           value: Option<&[u8]>,
                           current: u32)
                           -> Option<UnsafeValue> {
        unsafe {
            //println!("binary_tree_get:{:?}", page);
            let ptr = page.offset(current as isize) as *mut u32;
            
            let (key0, value0) = self.read_key_value(ptr as *const u8);
            /*println!("binary_tree_get:{:?}, {:?}",
            std::str::from_utf8_unchecked(key),
            std::str::from_utf8_unchecked(key0));*/
            let cmp = if let Some(value_) = value {
                let cmp = key.cmp(&key0);
                if cmp == Ordering::Equal {
                    let value = UnsafeValue::S { p:value_.as_ptr(), len:value_.len() as u32 };
                    Value { txn:self,value:value }.cmp(Value { txn:self,value:value0 })
                } else {
                    cmp
                }
            } else {
                key.cmp(&key0)
            };
            match cmp {
                Ordering::Equal | Ordering::Less => {
                    let result = {
                        let left0 = u32::from_le(*(ptr as *const u32));
                        if left0 == 1 {
                            let next = u32::from_le(*(ptr.offset(1)));
                            if next == 0 {
                                None
                            } else {
                                self.binary_tree_get(page, key, value, next)
                            }
                        } else {
                            // Global offset
                            let left = u64::from_le(*(ptr as *const u64));
                            if left == 0 {
                                None
                            } else {
                                // left child is another page.
                                let page_ = self.load_page(left);
                                let root_ = page_.root();
                                self.binary_tree_get(&page_, key, value, root_ as u32)
                            }
                        }
                    };
                    if cmp == Ordering::Equal {
                        result.or(Some(value0))
                    } else {
                        result
                    }
                }
                Ordering::Greater => {
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    if right0 == 1 {
                        let next = u32::from_le(*(ptr.offset(3)));
                        if next == 0 {
                            None
                        } else {
                            self.binary_tree_get(page, key, value, next)
                        }
                    } else {
                        // global offset, follow
                        let right = u64::from_le(*((ptr as *const u64).offset(1)));
                        if right == 0 {
                            None
                        } else {
                            // right child is another page
                            let page_ = self.load_page(right);
                            let root_ = page_.root();
                            self.binary_tree_get(&page_, key, value, root_ as u32)
                        }
                    }
                }
            }
        }
    }





    fn tree_iterate<'a, F: Fn(&'a [u8], Value<'a,Self>) -> bool + Copy>(&'a self,
                                                                  page: &Page,
                                                                  key: &[u8],
                                                                  value: Option<&[u8]>,
                                                                  f: F,
                                                                  current: u32,
                                                                  started: bool)
                                                                  -> Option<bool> {
        unsafe {
            debug!("binary_tree_get:{:?}", page);
            let ptr = page.offset(current as isize) as *mut u32;

            let value_ = value.unwrap_or(b"");
            let (key0, value0) = self.read_key_value(ptr as *const u8);
            let cmp = {
                let cmp = key.cmp(&key0);
                if cmp == Ordering::Equal {
                    if let Some(value) = value {
                        let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
                        Value { txn:self,value:value }.cmp(Value { txn:self,value:value0 })
                            //value.cmp(value0.clone())
                    } else {
                        cmp
                    }
                } else {
                    cmp
                }
            };

            // If we've already started iterating, or else if the key can be found on our left.
            let result_left = if started ||
                                 (!started && (cmp == Ordering::Equal || cmp == Ordering::Less)) {
                let result = {
                    let left0 = u32::from_le(*(ptr as *const u32));
                    if left0 == 1 {
                        let next = u32::from_le(*(ptr.offset(1)));
                        if next == 0 {
                            None
                        } else {
                            self.tree_iterate(page, key, value, f, next, started)
                        }
                    } else {
                        // Global offset
                        let left = u64::from_le(*(ptr as *const u64));
                        if left == 0 {
                            None
                        } else {
                            // left child is another page.
                            let page_ = self.load_page(left);
                            let root_ = page_.root();
                            self.tree_iterate(&page_, key, value, f, root_ as u32, started)
                        }
                    }
                };
                match result {
                    Some(true) => {
                        let value0 = Value { txn:self,value:value0 };
                        Some(f(key0, value0))
                    }
                    None if cmp == Ordering::Equal => {
                        let value0 = Value { txn:self,value:value0 };
                        Some(f(key0, value0))
                    }
                    _ => result, // we've stopped already
                }
            } else {
                None
            };


            if result_left == Some(false) {
                Some(false)
            } else {
                if (result_left.is_none() && cmp == Ordering::Greater) || result_left.is_some() {
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    if right0 == 1 {
                        let next = u32::from_le(*(ptr.offset(3)));
                        if next == 0 {
                            None
                        } else {
                            self.tree_iterate(page,
                                              key,
                                              value,
                                              f,
                                              next,
                                              started || result_left.is_some())
                        }
                    } else {
                        // global offset, follow
                        let right = u64::from_le(*((ptr as *const u64).offset(1)));
                        if right == 0 {
                            None
                        } else {
                            // right child is another page
                            let page_ = self.load_page(right);
                            let root_ = page_.root();
                            self.tree_iterate(&page_,
                                              key,
                                              value,
                                              f,
                                              root_ as u32,
                                              started || result_left.is_some())
                        }
                    }
                } else {
                    result_left
                }
            }
        }
    }
}






// Page layout: Starts with a header of 24 bytes.
// - 64 bits: RC
// - 16 bits: offset of the first free spot, from the byte before
// - 16 bits: offset of the root of the tree, from the byte before
// - 16 bits: how much space is occupied in this page? (controls compaction)
// - 16 bits: padding
// - beginning of coding space (different encodings in B-nodes and B-leaves)


pub trait P {
    /// offset of the page in the file.
    fn page_offset(&self) -> u64;

    /// pointer to the first word of the page.
    fn data(&self) -> *const u64;

    /// 0 if cannot alloc, valid offset else (offset in bytes from the byte before the coding section).
    fn can_alloc(&self, size: u16) -> u16 {
        unsafe {
            assert!(size & 7 == 0); // 64 bits aligned.
            let first_free = self.first_free();

            let next_page = (self.data() as *mut u8).offset(PAGE_SIZE as isize) as *const u8;
            let current = (self.data() as *const u8).offset(15 + first_free as isize);
            if current.offset(size as isize) <= next_page {
                first_free
            } else {
                0
            }
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
            let first_free = if first_free > 0 {
                first_free
            } else {
                1
            };
            first_free
        }
    }
    fn p_first_free(&self) -> *mut u16 {
        unsafe { self.data().offset(1) as *mut u16 }
    }

    fn root(&self) -> u16 {
        unsafe {
            let p_root = (self.data() as *const u16).offset(5);
            u16::from_le(*p_root)
        }
    }
    fn set_root(&self, root: u16) {
        unsafe {
            let p_root = (self.data() as *mut u16).offset(5);
            *p_root = root.to_le()
        }
    }
    // Amount of space occupied in the page
    fn occupied_space(&self) -> u16 {
        unsafe {
            let p_occ = (self.data() as *const u16).offset(6);
            u16::from_le(*p_occ)
        }
    }

    // offset in u32.
    fn offset(&self, off: isize) -> *mut u8 {
        unsafe {
            let p = self.data() as *mut u8;
            p.offset(15 + off)
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
            std::ptr::write_bytes(self.page.data as *mut u8, 0, 16);
            self.incr_rc()
        }
    }
    pub fn incr_rc(&mut self) {
        unsafe {
            *(self.page.data as *mut u64) = (self.rc() + 1).to_le();
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

    // Layout of a node: 24 + |key|+|value|, rounded up to 64-bits.
    // - 64 bits: left, little endian. if the first 32 bits == 1, local offset, else global in bytes.
    // - 64 bits: right, little endian. if the first 32 bits == 1, local offset, else global in bytes.
    // - 32 bits: value length, if >PAGE_SIZE/4, the value is a 64-bits offset of a page.
    // - 16 bits: key length
    // - 16 bits: balance factor (2 LSB).
    // - value
    // - key
    // - padding for 64 bits/8 bytes alignment.

    // allocate and write key, value, left and right neighbors.
    pub fn alloc_key_value(&mut self,
                           off_ptr: u16,
                           size: u16,
                           key_ptr:*const u8,
                           key_len:usize,
                           value: UnsafeValue,
                           l: u64,
                           r: u64) {
        unsafe {
            self.alloc(off_ptr, size);
            // println!("off_ptr={:?}, size = {:?}",off_ptr, size);
            // off is the beginning of a free zone. Write the node there.
            // ///////////////////////////////////////////////
            let ptr = self.offset(off_ptr as isize) as *mut u32;
            // println!("ptr: {} {:?}",off_ptr,ptr0);
            // This is a leaf, so l and r are offsets in the file, not local offsets.
            let ptr = ptr as *mut u64;
            *ptr = l.to_le();
            *(ptr.offset(1)) = r.to_le();
            let ptr = ptr as *mut u32;
            *(ptr.offset(4)) = (value.len() as u32).to_le();

            let ptr = ptr as *mut u16;
            *(ptr.offset(10)) = (key_len as u16).to_le();
            *(ptr.offset(11)) = (1 as u16).to_le();
            // +(if l!=0 { 1 } else { 0 } + if r!=0 { 1 } else { 0 } as u32).to_le(); // balance number
            // println!("alloc_key_value: copying {:?} {:?} to {:?}",key,value,ptr);
            match value {
                UnsafeValue::S{p,len} => {
                    let ptr = ptr as *mut u8;
                    let ptr = ptr.offset(24);
                    copy_nonoverlapping(p, ptr, len as usize);
                    copy_nonoverlapping(key_ptr, ptr.offset(len as isize), key_len);
                }
                UnsafeValue::O{offset,..} => {
                    debug_assert!(offset != 0);
                    *((ptr as *mut u64).offset(3)) = offset.to_le();
                    let ptr = ptr as *mut u8;
                    copy_nonoverlapping(key_ptr, ptr.offset(32), key_len);
                }
            }
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
    // fn is_mutable(&self)->bool {
    // let &Cow(ref s)=self;
    // match s { &transaction::Cow::MutPage(_)=>true, _=>false }
    // }
    //

    /*
    // NOTE: the following function (from_page) should not be used,
    // as they might lead to useless copying when into_mut_page is
    // called. use load_cow_page instead.
    
    pub fn from_page(p:Page)->Cow {
        Cow { cow: transaction::Cow::Page(p.page) }
    }
     */
    pub fn into_mut_page_nonfree(self, txn: &mut MutTxn) -> (MutPage,Option<Page>) {
        match self.cow {
            transaction::Cow::MutPage(p) => (MutPage { page: p },None),
            transaction::Cow::Page(p) => {
                unsafe {
                    let result = txn.txn.alloc_page().unwrap();
                    copy_nonoverlapping(p.data, result.data, PAGE_SIZE);
                    // TODO: decrement and check RC
                    // p.free(&mut txn.txn);
                    (MutPage { page: result }, Some(Page{page:p}))
                }
            }
        }
    }
    pub fn into_mut_page(self, txn: &mut MutTxn) -> MutPage {
        let (a,b) = self.into_mut_page_nonfree(txn);
        if let Some(b) = b {
            b.page.free(&mut txn.txn);
        }
        a
    }
    // fn into_page(self)->Page {
    // let Cow(s)=self;
    // match s {
    // transaction::Cow::Page(p)=> Page { page:p },
    // transaction::Cow::MutPage(p)=> Page { page:p.into_page() }
    // }
    // }
    //
}

impl<'env> LoadPage for MutTxn<'env> {
    fn length(&self) -> u64 {
        self.txn.env.length
    }
    fn root_db_(&self) -> Db {
        Db { root: self.btree_root }
    }
    fn fd(&self) -> c_int {
        self.txn.env.fd
    }
    fn load_page(&self, off: u64) -> Page {
        Page { page: self.txn.load_page(off) }
    }
}
impl<'env> LoadPage for Txn<'env> {
    fn length(&self) -> u64 {
        self.txn.env.length
    }
    fn root_db_(&self) -> Db {
        Db { root: self.btree_root }
    }
    fn fd(&self) -> c_int {
        self.txn.env.fd
    }
    fn load_page(&self, off: u64) -> Page {
        Page { page: self.txn.load_page(off) }
    }
}


fn debug<P: AsRef<Path>, T: LoadPage>(t: &T, db: &Db, p: P) {
    let page = t.load_page(db.root);
    let f = File::create(p.as_ref()).unwrap();
    let mut buf = BufWriter::new(f);
    writeln!(&mut buf, "digraph{{").unwrap();
    let mut h = HashSet::new();
    fn print_page<T: LoadPage>(txn: &T,
                               pages: &mut HashSet<u64>,
                               buf: &mut BufWriter<File>,
                               p: &Page,
                               print_children: bool) {
        if !pages.contains(&p.page.offset) {
            pages.insert(p.page.offset);
            if print_children {
                writeln!(buf,
                         "subgraph cluster{} {{\nlabel=\"Page {}\";\ncolor=black;",
                         p.page.offset,
                         p.page.offset)
                    .unwrap();
            }
            let root = p.root();
            debug!("page root:{:?}", root);
            let mut h = Vec::new();
            let mut edges = Vec::new();
            let mut hh = HashSet::new();
            print_tree(txn, &mut hh, buf, &mut edges, &mut h, p, root as u32);
            if print_children {
                writeln!(buf, "}}").unwrap();
            }
            for p in edges.iter() {
                writeln!(buf, "{}", p).unwrap()
            }
            if print_children {
                for p in h.iter() {
                    print_page(txn, pages, buf, p, true)
                }
            }
        }
    }

    fn print_tree<T: LoadPage>(txn: &T,
                               nodes: &mut HashSet<u32>,
                               buf: &mut BufWriter<File>,
                               edges: &mut Vec<String>,
                               pages: &mut Vec<Page>,
                               p: &Page,
                               off: u32) {
        unsafe {
            // println!("print tree:{:?}",off);
            let ptr = p.offset(off as isize) as *const u32;
            let count = u16::from_le(*(ptr as *const u16).offset(11));
            let (key, mut value) = txn.read_key_value(ptr as *const u8);
            let key = std::str::from_utf8_unchecked(key);
            let mut value_ = Vec::new();
            let mut value = Value { txn:txn,value:value };
            let value = if value.len() > 20 {
                let contents = value.next().unwrap();
                value_.extend(&contents[0..20]);
                value_.extend(b"...");
                &value_[..]
            } else {
                value.next().unwrap()
            };
            let value = std::str::from_utf8_unchecked(value);
            // println!("key,value={:?},{:?}",key,value);
            writeln!(buf,
                     "n_{}_{}[label=\"{}: {}, '{}'->'{}'\"];",
                     p.page.offset,
                     off,
                     off,
                     count,
                     key,
                     value)
                .unwrap();
            if !nodes.contains(&off) {
                nodes.insert(off);

                let left_local = u32::from_le(*(ptr as *const u32));
                // println!("debug, left_local={:?}",left_local);
                if left_local == 1 {
                    let left = u32::from_le(*(ptr.offset(1)));
                    writeln!(buf,
                             "n_{}_{}->n_{}_{}[color=\"red\"];",
                             p.page.offset,
                             off,
                             p.page.offset,
                             left)
                        .unwrap();
                    print_tree(txn, nodes, buf, edges, pages, p, left);
                } else {
                    let page = u64::from_le(*(ptr as *const u64));
                    // println!("debug, page={:?}",u32::from_le(*((ptr as *const u32).offset(1))));
                    // println!("debug, page={:?}",page);
                    if page > 0 && page < txn.length() {
                        let page = txn.load_page(page);
                        let root = page.root();
                        edges.push(format!("n_{}_{}->n_{}_{}[color=\"red\"];",
                                           p.page.offset,
                                           off,
                                           page.page.offset,
                                           root));
                        pages.push(page)
                    } else {
                        if page > 0 {
                            panic!("Wrong page offset:{}", page);
                        }
                    }
                }
                let right_local = u32::from_le(*((ptr as *const u32).offset(2)));
                // println!("debug, right_local={:?}",right_local);
                if right_local == 1 {
                    let right = u32::from_le(*(ptr.offset(3)));
                    edges.push(format!("n_{}_{}->n_{}_{}[color=\"green\"];",
                                       p.page.offset,
                                       off,
                                       p.page.offset,
                                       right));
                    print_tree(txn, nodes, buf, edges, pages, p, right);
                } else {
                    let page = u64::from_le(*((ptr as *const u64).offset(1)));
                    // println!("debug, page={:?}",page);
                    if page > 0 && page < txn.length() {
                        let page = txn.load_page(page);
                        let root = page.root();
                        edges.push(format!("n_{}_{}->n_{}_{}[color=\"green\"];",
                                           p.page.offset,
                                           off,
                                           page.page.offset,
                                           root));
                        pages.push(page)
                    } else {
                        if page > 0 {
                            panic!("Wrong page offset:{}", page);
                        }
                    }
                }
            }
        }
    }
    print_page(t, &mut h, &mut buf, &page, true /* print children */);
    writeln!(&mut buf, "}}").unwrap();
}

pub unsafe fn node_ptr(page: &MutPage,
                   mut length: usize,
                   mut path: u64,
                   mut current: u32)
                   -> u16 {
    while length > 0 {
        let ptr = page.offset(current as isize) as *mut u32;
        // println!("node_ptr:{:?}",if path&1==0 { u32::from_le(*ptr) } else { u32::from_le(*(ptr.offset(2))) });
        // assert!(if path&1==0 { u32::from_le(*ptr)==1 } else { u32::from_le(*(ptr.offset(2))) == 1 });
        current = if path & 1 == 0 {
            u32::from_le(*(ptr.offset(1)))
        } else {
            u32::from_le(*(ptr.offset(3)))
        };
        length -= 1;
        path >>= 1;
    }
    current as u16
}
pub fn value_record_size(key: usize, value: UnsafeValue) -> u16 {
    match value {
        UnsafeValue::S { p,len,.. } if len < (VALUE_SIZE_THRESHOLD as u32) => {
            let size = 28 + key as u16 + len as u16;
            size + ((8 - (size & 7)) & 7)
        }
        UnsafeValue::S{..} | UnsafeValue::O{..} => {
            let size = 28 + key as u16 + 8;
            size + ((8 - (size & 7)) & 7)
        }
    }
}
