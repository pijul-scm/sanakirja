use super::transaction;
use std;
use std::path::Path;
use libc::c_int;
use super::transaction::{PAGE_SIZE};
use std::fs::File;
use std::io::BufWriter;
use std::collections::HashSet;
use std::ptr::copy_nonoverlapping;
use std::io::Write;
use std::fmt;
use std::cmp::Ordering;

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

#[derive(Clone,Copy,Debug)]
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
    let p_value = value.as_ptr();
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


pub trait LoadPage:Sized {
    fn fd(&self) -> c_int;
    fn length(&self) -> u64;
    fn root_db_(&self) -> Db;
    fn open_db_<'a>(&'a self, key: &[u8]) -> Option<Db> {
        let page = self.load_page(self.root_db_().root);
        unsafe {
            let db = self.get_(page, key, None);
            if let Some(UnsafeValue::S{p,..}) = db {
                Some(Db { root: u64::from_le(*(p as *const u64)) })
            } else {
                None
            }
        }
    }

    fn load_page(&self, off: u64) -> Page;

    unsafe fn get_<'a>(&'a self, page:Page, key: &[u8], value:Option<UnsafeValue>) -> Option<UnsafeValue> {
        let mut current_off = 8;
        let mut current = page.offset(current_off as isize) as *const u16;
        let mut level = 4;
        let mut next_page = 0;
        let mut equal:Option<UnsafeValue> = None;
        loop {
            // advance in the list until there's nothing more to do.
            loop {
                let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
                if next == 0 {
                    break
                } else {
                    let next_ptr = page.offset(next as isize);
                    let (next_key,next_value) = read_key_value(next_ptr);
                    match key.cmp(next_key) {
                        Ordering::Less => break,
                        Ordering::Equal =>
                            if let Some(value) = value {
                                match (Value{txn:self,value:value}).cmp(Value{txn:self,value:next_value}) {
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
}




// Page layout: Starts with a header of 32 bytes.
// - 64 bits: RC
// - 5*16 bits: pointers to all the skip lists.
// - 16 bits: offset of the first free spot, from the byte before
// - 16 bits: how much space is occupied in this page? (controls compaction)
// - 16 bits: padding
// - 64 bits: smaller child
// - beginning of coding space (different encodings in B-nodes and B-leaves)


pub trait P {
    /// offset of the page in the file.
    fn page_offset(&self) -> u64;

    /// pointer to the first word of the page.
    fn data(&self) -> *const u64;

    /// 0 if cannot alloc, valid offset else (offset in bytes from the start of the page)
    fn can_alloc(&self, size: u16) -> u16 {
        unsafe {
            assert!(size & 7 == 0); // 64 bits aligned.
            let first_free = self.first_free();

            let next_page = (self.data() as *mut u8).offset(PAGE_SIZE as isize) as *const u8;
            let current = (self.data() as *const u8).offset(first_free as isize);
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
            if first_free > 0 {
                first_free
            } else {
                32
            }
        }
    }
    fn p_first_free(&self) -> *mut u16 {
        unsafe { (self.data() as *mut u16).offset(9) }
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
            std::ptr::write_bytes(self.page.data as *mut u8, 0, 32);
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

    // allocate and write key, value, left and right neighbors.
    pub fn alloc_key_value(&mut self,
                           off_ptr: u16,
                           size: u16,
                           key_ptr:*const u8,
                           key_len:usize,
                           value: UnsafeValue) {
        unsafe {
            self.alloc(off_ptr, size);
            let ptr = self.offset(off_ptr as isize) as *mut u8;
            *(ptr as *mut u64) = 0;
            *((ptr as *mut u64).offset(1)) = 0;
            *((ptr as *mut u16).offset(5)) = (key_len as u16).to_le();
            let target_key_ptr = match value {
                UnsafeValue::S { p,len } => {
                    *((ptr as *mut u32).offset(3)) = len.to_le();
                    copy_nonoverlapping(p,(ptr as *mut u8).offset(24), len as usize);
                    (ptr as *mut u8).offset(24 + len as isize)
                },
                _ => unimplemented!()
            };
            copy_nonoverlapping(key_ptr, target_key_ptr, key_len);
            /*
            for i in 0..5 {
                println!("alloc_key_value:{:?} {:?}", (ptr as *const u16).offset(i),
                         *((ptr as *const u16).offset(i)))
            }*/
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

    pub fn unwrap_mut(self) -> MutPage {
        match self.cow {
            transaction::Cow::MutPage(p) => MutPage { page: p },
            transaction::Cow::Page(_) => panic!("unwrap")
        }
    }
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
            //let root = unsafe { u16::from_le(*(p.data() as *const u16).offset(8)) };
            let root = 8;
            debug!("page root:{:?}", root);
            let mut h = Vec::new();
            let mut edges = Vec::new();
            let mut hh = HashSet::new();
            print_tree(txn, &mut hh, buf, &mut edges, &mut h, p, root);
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
                               nodes: &mut HashSet<u16>,
                               buf: &mut BufWriter<File>,
                               edges: &mut Vec<String>,
                               pages: &mut Vec<Page>,
                               p: &Page,
                               off: u16) {
        unsafe {
            //debug!("print tree:{:?}",off);
            let ptr = p.offset(off as isize) as *const u32;

            let (key,value) = {
                if off == 8 {
                    ("root","".to_string())
                } else {

                    let (key, value) = read_key_value(ptr as *const u8);
                    //println!("key,value = ({:?},{:?})", key.as_ptr(), value.len());
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
                    (key,value.to_string())
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
                //println!("next_page = {:?} {:?}: {:?}", ptr, (ptr as *const u64).offset(2), next_page);
                if next_page>0 {
                    pages.push(txn.load_page(next_page));
                    writeln!(buf,
                             "n_{}_{}->n_{}_{}[color=\"red\"];",
                             p.page.offset,
                             off,
                             next_page,
                             8)
                        .unwrap();
                };

                nodes.insert(off);
                for i in 0..5 {
                    let left = u16::from_le(*((ptr as *const u16).offset(i)));
                    //debug!("{:?}",((ptr as *const u16).offset(i)));
                    if left>0 {
                        writeln!(buf,
                                 "n_{}_{}->n_{}_{}[color=\"blue\", label=\"{}\"];",
                                 p.page.offset,
                                 off,
                                 p.page.offset,
                                 left,i)
                            .unwrap();
                        print_tree(txn,nodes,buf,edges,pages,p,left)
                    }
                }
            }
        }
    }
    print_page(t, &mut h, &mut buf, &page, true /* print children */);
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

