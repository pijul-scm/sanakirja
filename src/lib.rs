/* TODO

- get
- del
- iterate
- several databases
- reference counting
- check that all dereferences are converted to/from little-endian.

*/

extern crate libc;
#[macro_use]
extern crate log;
extern crate fs2;

use std::path::Path;
use std::ptr::copy_nonoverlapping;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufWriter,Write};
mod constants;
mod transaction;

pub use transaction::{Statistics};
use transaction::{PAGE_SIZE,PAGE_SIZE_64};


pub struct MutTxn<'env> {
    txn:transaction::MutTxn<'env>,
    pub btree_root:u64
}
pub struct Txn<'env> {
    txn:transaction::Txn<'env>
}

pub struct Env {
    env:transaction::Env
}

pub type Error=transaction::Error;

impl Env {
    pub fn new<P:AsRef<Path>>(file:P) -> Result<Env,Error> {
        transaction::Env::new(file,13 + 6).and_then(|env| Ok(Env {env:env}))
    }
    pub fn statistics(&self)->Statistics {
        self.env.statistics()
    }
    pub fn txn_begin<'env>(&'env self)->Txn<'env> {
        Txn { txn:self.env.txn_begin() }
    }
    pub fn mut_txn_begin<'env>(&'env self)->MutTxn<'env> {
        let btree_root= unsafe {
            let p_extra=self.env.extra() as *const u64;
            u64::from_le(*p_extra)
        };
        MutTxn {
            txn:self.env.mut_txn_begin(),
            btree_root:btree_root
        }
    }
}




#[derive(Debug)]
pub struct MutPage {
    page:transaction::MutPage,
}
#[derive(Debug)]
pub struct Page {
    page:transaction::Page,
}

impl Page {
    fn skip_glues(&self)->*mut u64 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.skip_glues()
    }
    fn flags(&self)->u16 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.flags()
    }

    fn rc(&self)->u32 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.rc()
    }
    // First free spot in this page (head of the linked list, number of |u32| from the last glue.
    fn first_free(&self)->u32 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.first_free()
    }

    fn root(&self)->u32 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.root()
    }
    // Amount of space occupied in the page
    fn occupied_space(&self)->u32 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.occupied_space()
    }
    // offset in u32.
    fn offset(&self,off:u32)->*mut u32 {
        let p:&MutPage = unsafe { std::mem::transmute(self) };
        p.offset(off)
    }
}

impl MutPage {
    // Page layout: Starts with a header of ((n>=1)*8 bytes + 16 bytes).
    // - 64 bits: glueing number (0 for now), + flags on the 13 least significant bits
    // - 32 bits: RC
    // - 32 bits: offset of the first free spot, from the beginning of the page
    // - 32 bits: offset of the root of the tree, from the beginning of the page
    // - 32 bits: how much space is occupied in this page? (controls compaction)
    // - beginning of coding space (different encodings in B-nodes and B-leaves)

    fn init(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.page.data as *mut u8,0,24);
        }
    }

    // returns a pointer to the last glue number.
    fn skip_glues(&self)->*mut u64 {
        unsafe {
            let p=self.page.data as *mut u64;
            while u64::from_le(*p) >= PAGE_SIZE_64 {
                unimplemented!() // glue pages together.
                    //p=p.offset(1)
                    // len-=8
            }
            p
        }
    }

    fn flags(&self)->u16 {
        unsafe {
            let p=self.skip_glues();
            ((u64::from_le(*p)) & (PAGE_SIZE_64-1)) as u16
        }
    }

    fn rc(&self)->u32 {
        unsafe {
            let p=self.skip_glues().offset(1) as *const u32;
            u32::from_le(*p)
        }
    }
    // First free spot in this page (head of the linked list, number of |u32| from the last glue.
    fn first_free(&self)->u32 {
        unsafe {
            let p=(self.skip_glues() as *const u32).offset(3);
            let f=u32::from_le(*p);
            if f==0 { 1 } else { f }
        }
    }

    fn root(&self)->u32 {
        unsafe {
            let p=self.skip_glues() as *mut u64;
            let p_root=(p as *mut u32).offset(4);
            *p_root
        }
    }
    fn set_root(&self,root:u32) {
        unsafe {
            let p=self.skip_glues() as *mut u64;
            let p_root=(p as *mut u32).offset(4);
            *p_root = root
        }
    }
    // Amount of space occupied in the page
    fn occupied_space(&self)->u32 {
        unsafe {
            let p=(self.skip_glues() as *const u32).offset(5);
            u32::from_le(*p)
        }
    }

    // offset in u32.
    fn offset(&self,off:u32)->*mut u32 {
        unsafe {
            let p=self.skip_glues() as *mut u32;
            p.offset(5+off as isize)
        }
    }

    /// Takes a size in bytes, returns an offset from the word before
    /// the beginning of the contents (0 is invalid, 1 is the first
    /// offset).
    fn alloc(&mut self,size:usize)->Option<u32> {
        unsafe {
            assert!(size&3== 0); // 32 bits aligned.
            let p=self.skip_glues() as *mut u32;
            let first_free={
                // Offset from the word before contents.
                let f=u32::from_le(*(p.offset(3)));
                if f>0 { f } else { 1 }
            };

            let zero=p.offset(5);

            let next_page = self.page.data.offset(PAGE_SIZE as isize) as *mut u32;

            let current=zero.offset(first_free as isize);
            // Always allocate at the end (for now).
            // The head is always the beginning of the free zone at the end
            // The first 32-bits word there is a tail.
            // If the tail == 0, the list is empty.
            //println!("alloc: {:?} {:?}, {:?}",current,size,next_page);
            if current.offset(size as isize) > next_page {
                return None
            } else {
                *(p.offset(3)) = (first_free + ((size as u32) >> 2)).to_le();
                //println!("alloc / {:?}, {:?}, {:?}", first_free,size,u32::from_le(*(p.offset(3))));
                Some(first_free)
                    //Some(Tree::new(p,first_free as isize,key,value))
            }
        }
    }

    // Layout of a node:
    // - 64 bits: left, little endian. if the first 32 bits == 1, local offset, else global in bytes.
    // - 64 bits: right, little endian. if the first 32 bits == 1, local offset, else global in bytes.
    // - 32 bits: key length
    // - 32 bits: value length
    // - 32 bits: cardinal (number of nodes)
    // - contents, |key|+|value|
    // - padding for 32 bits/4 bytes alignment.

    // allocate and write key, value, left and right neighbors.
    fn alloc_key_value(&mut self,key:&[u8],value:&[u8],l:u64,r:u64)->Option<u32> {
        unsafe {
            // size in bytes
            let size=28
                + key.len() + value.len();
            let size = size + ((4-(size&3))&3);

            self.alloc(size).and_then(|off_ptr| {
                //println!("off_ptr={:?}, size = {:?}",off_ptr, size);
                // off is the beginning of a free zone. Write the node there.
                //////////////////////////////////////////////////
                let ptr=self.offset(off_ptr) as *mut u32;
                //println!("ptr: {} {:?}",off_ptr,ptr0);
                // This is a leaf, so l and r are offsets in the file, not local offsets.
                let ptr=ptr as *mut u64;
                *ptr = l.to_le();
                *(ptr.offset(1)) = r.to_le();
                debug!("alloc_key_value: {} {}",*ptr,*(ptr.offset(1)));
                let ptr=ptr as *mut u32;
                *(ptr.offset(4)) = (key.len() as u32).to_le();
                *(ptr.offset(5)) = (value.len() as u32).to_le();
                *(ptr.offset(6)) = 0; // balance number

                let ptr=ptr.offset(7) as *mut u8;
                //println!("alloc_key_value: copying {:?} {:?} to {:?}",key,value,ptr);
                copy_nonoverlapping(key.as_ptr(), ptr, key.len());
                copy_nonoverlapping(value.as_ptr(), ptr.offset(key.len() as isize), value.len());
                Some(off_ptr)
            })
        }
    }
}




const NODE_HEADER_BYTES:isize=28;
fn read_key_value<'a>(p:&'a u8)->(&'a [u8],&'a[u8]) {
    unsafe {
        let p32=p as *const u8 as *const u32;
        let key_len=*(p32.offset(4));
        let val_len=*(p32.offset(5));
        (std::slice::from_raw_parts((p as *const u8).offset(NODE_HEADER_BYTES), key_len as usize),
         std::slice::from_raw_parts((p as *const u8).offset(NODE_HEADER_BYTES + key_len as isize), val_len as usize))
    }
}

#[derive(Debug)]
struct Cow(transaction::Cow);

impl Cow {
    fn into_mut_page(self,txn:&mut MutTxn)->MutPage {
        let Cow(s)=self;
        match s {
            transaction::Cow::MutPage(p)=>MutPage { page:p },
            transaction::Cow::Page(p)=>{
                unsafe {
                    let result=txn.txn.alloc_page().unwrap();
                    copy_nonoverlapping(p.data,result.data,PAGE_SIZE);
                    // TODO: decrement and check RC
                    p.free(&mut txn.txn);
                    if txn.btree_root == p.offset {
                        txn.btree_root = result.offset
                    }
                    MutPage { page:result }
                }
            }
        }
    }
    fn page(self)->Page {
        let Cow(s)=self;
        match s {
            transaction::Cow::Page(p)=> unsafe { std::mem::transmute(p) },
            transaction::Cow::MutPage(p)=> unsafe { std::mem::transmute(p) }
        }
    }
}



impl<'env> MutTxn<'env> {

    pub fn commit(self)->Result<(),transaction::Error> {
        unsafe {
            let extra=self.btree_root.to_le();
            let x64:&[u8]=std::slice::from_raw_parts( std::mem::transmute(&extra), 8);
            self.txn.commit(std::mem::transmute(x64))
        }
    }
    fn load_mut_root(&mut self)->Option<MutPage> {
        debug!("load_root: {:?}",self.btree_root);
        if self.btree_root == 0 {
            None
        } else {
            // Here, go to page and load it.
            unsafe {
                let page = Cow(self.txn.load_mut_page(self.btree_root));
                debug!("cow page: {:?}",page);
                let page=page.into_mut_page(self);
                self.btree_root = page.page.offset;
                let p=page.page.data as *mut u64;
                let glues= *p;
                assert!(glues < PAGE_SIZE_64);
                Some(page)
            }
        }
    }
    fn load_root(&self)->Option<Page> {
        if self.btree_root == 0 {
            None
        } else {
            // Here, go to page and load it.
            Some(Page { page:self.txn.load_page(self.btree_root) })
        }
    }

    pub fn debug<P:AsRef<Path>>(&mut self,p:P) {
        let f=File::create(p.as_ref()).unwrap();
        let mut buf=BufWriter::new(f);
        writeln!(&mut buf,"digraph{{").unwrap();
        if let Some(root)=self.load_root() {

            fn print_page(txn:&MutTxn, buf:&mut BufWriter<File>,p:&Page) {
                writeln!(buf,"subgraph cluster{} {{\ncolor=black;",p.page.offset).unwrap();
                let root=p.root();
                let mut h=Vec::new();
                let mut edges=Vec::new();
                print_tree(txn,buf,&mut edges,&mut h,p,root);
                writeln!(buf,"}}").unwrap();

                for p in edges.iter() {
                    writeln!(buf,"{}",p).unwrap()
                }
                for p in h.iter() {
                    print_page(txn,buf,p)
                }

            }

            fn print_tree(txn:&MutTxn, buf:&mut BufWriter<File>,edges:&mut Vec<String>,pages:&mut Vec<Page>, p:&Page,off:u32) {
                unsafe {
                    let ptr=p.offset(off);
                    let count=u32::from_le(*ptr.offset(6));
                    let (key,value)=read_key_value(&*(ptr as *const u8));
                    let key=std::str::from_utf8_unchecked(key);
                    let value=std::str::from_utf8_unchecked(value);
                    writeln!(buf,"n_{}_{}[label=\"{}, '{}'->'{}'\"];",p.page.offset,off,count,key,value).unwrap();
                    let left_local= u32::from_le(*ptr);
                    if left_local == 1{
                        let left=u32::from_le(*(ptr.offset(1)));
                        writeln!(buf,"n_{}_{}->n_{}_{}[color=\"red\"];",
                                 p.page.offset,off,
                                 p.page.offset,left).unwrap();
                        print_tree(txn,buf,edges,pages,p,left);
                    } else {
                        let page=u64::from_le(*(ptr as *const u64));
                        if page!=0 {
                            let page=txn.txn.load_page(page);
                            let page=Page { page:std::mem::transmute(page) };
                            let root=page.root();
                            edges.push(format!("n_{}_{}->n_{}_{}[color=\"red\"];",p.page.offset,off,page.page.offset,root));
                            pages.push(page)
                        }
                    }
                    let right_local= u32::from_le(*(ptr.offset(2)));
                    if right_local == 1{
                        let right=u32::from_le(*(ptr.offset(3)));
                        edges.push(format!("n_{}_{}->n_{}_{}[color=\"green\"];",
                                           p.page.offset,off,
                                           p.page.offset,right));
                        print_tree(txn,buf,edges,pages,p,right);
                    } else {
                        let page=u64::from_le(*((ptr as *const u64).offset(1)));
                        if page!=0 {
                            let page=txn.txn.load_page(page);
                            let page=Page { page:std::mem::transmute(page) };
                            let root=page.root();
                            edges.push(format!("n_{}_{}->n_{}_{}[color=\"green\"];",p.page.offset,off,page.page.offset,root));
                            pages.push(page)
                        }
                    }
                }
            }

            print_page(self,&mut buf, &root)
        }
        writeln!(&mut buf,"}}").unwrap();
    }



    pub fn put(&mut self,key:&[u8],value:&[u8]) {
        let put_result = if let Some(mut root) = self.load_mut_root() {
            debug!("put root = {:?}",root.page.offset);
            self.insert(&mut root,key,value,0,0)
        } else {
            debug!("put:no root");
            let mut btree = self.alloc_b_leaf(1);
            btree.init();
            let off=btree.page.offset;
            self.btree_root = off;
            self.insert(&mut btree,key,value,0,0)
        };

        if let Some((key0,value0,l,r))=put_result {
            let mut btree = self.alloc_b_leaf(1);
            btree.init();
            let off=btree.page.offset;
            self.btree_root = off;
            self.insert(&mut btree,key0,value0,l,r);
        }
    }
    fn load_page(&self,off:u64)->Page {
        Page { page:self.txn.load_page(off) }
    }
    fn load_mut_page(&mut self,off:u64)->MutPage {
        Cow(self.txn.load_mut_page(off)).into_mut_page(self)
    }

    fn alloc_b_leaf(&mut self,n_pages:usize)->MutPage {
        unsafe {
            assert!(n_pages==1);
            let page=self.txn.alloc_page().unwrap();
            let p=page.data as *mut u64;
            //println!("p:{:?}", p);
            *p = 0; // glue number + "leaf" tag
            let p=page.data as *mut u32;
            *(p.offset(2)) = 1; // reference counter.
            *(p.offset(3)) = 0; // offset of the first free spot.
            *(p.offset(4)) = 0; // offset of the root.
            *(p.offset(5)) = 0; // occupied space.
            MutPage{page:page}
        }
    }

    // Finds binary tree root and calls binary_tree_insert on it.
    fn insert<'a>(&mut self,page:&mut MutPage, key:&[u8],value:&[u8],l:u64,r:u64)->Option<(&'a [u8], &'a[u8], u64, u64)> {
        let root = page.root();
        debug!("insert: root={:?}, {:?},{:?}",root,key,value);
        if root==0 {
            let off=page.alloc_key_value(key,value,l,r).unwrap();
            debug!("inserted {}",off);
            page.set_root(off);
            None
        } else {
            match self.binary_tree_insert(page,key,value,l,r,root) {
                Some(Insert::Split { key,value,left,right })=>{
                    Some((key,value,left,right))
                },
                Some(Insert::Ok(root))=>{
                    page.set_root(root);
                    None
                },
                None => None
            }
        }
    }

    fn split_page<'a>(&mut self, page:&mut MutPage)->Insert<'a> {
        //page.page.free(&mut self.txn);
        self.debug("/tmp/before_split");
        debug!("\n\nsplit page {:?} !\n",page);
        // tree traversal
        fn iter(txn:&mut MutTxn, page:&MutPage, dest:&mut MutPage, current:u32)->u32 {
            unsafe {
                let ptr=page.offset(current);
                let (key,value)=read_key_value(&*(ptr as *const u8));

                let dest_off = dest.alloc_key_value(key,value,0,0).unwrap();
                let dest_ptr:*mut u32 = dest.offset(dest_off);

                let left0 = u32::from_le(*(ptr as *const u32));
                if left0 == 1 {
                    // local offset, follow
                    let left = u32::from_le(*((ptr as *const u32).offset(1)));
                    *(dest_ptr as *mut u32) = 1;
                    *((dest_ptr as *mut u32).offset(1)) = iter(txn,page,dest,left);
                } else {
                    // global offset, copy
                    *(dest_ptr as *mut u64) = *(ptr as *const u64);
                }
                let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                if right0 == 1 {
                    // local offset, follow
                    let right = u32::from_le(*((ptr as *const u32).offset(3)));
                    *((dest_ptr as *mut u32).offset(2)) = 1;
                    *((dest_ptr as *mut u32).offset(3)) = iter(txn,page,dest,right);
                } else {
                    // global offset, copy it
                    *((dest_ptr as *mut u64).offset(1)) = *((ptr as *const u64).offset(1));
                }
                *dest_ptr.offset(6) = *ptr.offset(6);
                dest_off
            }
        }
        unsafe {

            debug!("page root:{}",page.root());

            let mut left_page = MutPage { page:self.txn.alloc_page().unwrap() };
            let mut right_page = MutPage { page:self.txn.alloc_page().unwrap() };
            debug!("left page: {:?}, right page: {:?}",left_page,right_page);
            left_page.init();
            right_page.init();

            let ptr_root=page.offset(page.root()) as *mut u32;
            let count = u32::from_le(*ptr_root.offset(6));
            debug!("count:{:?}",count);
            {
                let left = *ptr_root;
                if left==1 {
                    // local offset
                    let left = u32::from_le(*((ptr_root as *const u32).offset(1)));
                    let left_root=iter(self,page,&mut left_page,left);
                    left_page.set_root(left_root);
                } else {
                    // global offset, the tree is not balanced.
                    info!("not splitting unbalanced tree")
                }
            }
            {
                let right = *(ptr_root.offset(2));
                if right==1 {
                    // local offset
                    let right = u32::from_le(*((ptr_root as *const u32).offset(3)));
                    let right_root = iter(self,page,&mut right_page,right);
                    right_page.set_root(right_root);
                } else {
                    // global offset, the tree is not balanced.
                    info!("not splitting unbalanced tree")
                }
            }
            let (key,value) = read_key_value(&*(ptr_root as *const u8));

            page.page.free(&mut self.txn);
            Insert::Split { key:key,value:value,left:left_page.page.offset,right:right_page.page.offset }
        }
    }

    // Returns None if the changes have been done in a child page, Some(Ok(..)) if this page is a leaf and we inserted something in it, and Some(Split(...)) if this page needs to be split.
    fn binary_tree_insert<'a>(&mut self, page:&mut MutPage, key:&[u8], value:&[u8], l:u64,r:u64,current:u32)->Option<Insert<'a>> {
        unsafe {
            debug!("binary_tree_insert {:?}",current);
            let ptr=page.offset(current) as *mut u32;

            let count = u32::from_le(*(ptr.offset(6)));
            debug!("count:{:?}",count);

            let (key0,value0)=read_key_value(&*(ptr as *const u8));
            //println!("comparing ({:?},{:?})", std::str::from_utf8_unchecked(key0), std::str::from_utf8_unchecked(value0));
            let cmp= (key,value).cmp(&(key0,value0));
            debug!("cmp={:?}",cmp);
            match cmp {
                Ordering::Equal | Ordering::Less => {
                    let left0 = u32::from_le(*(ptr as *const u32));
                    debug!("left0={:?}",left0);
                    if left0 == 1 {
                        // local offset
                        if let Some(off_ptr)= page.alloc_key_value(key,value,l,r) {
                            debug!("left0 allocated");
                            let next=u32::from_le(*(ptr.offset(1)));
                            if next==0 {
                                // free left branch
                                *((ptr as *mut u32).offset(1)) = off_ptr;
                                incr(ptr.offset(6));
                                Some(Insert::Ok(current))
                            } else {
                                let result=self.binary_tree_insert(page,key,value,l,r,next);
                                if let Some(Insert::Ok(root))=result {
                                    *((ptr as *mut u32).offset(1)) = root.to_le();
                                    incr(ptr.offset(6));
                                    Some(Insert::Ok(rebalance(page,current)))
                                } else {
                                    result
                                }
                            }
                        } else {
                            debug!("needs to split");
                            Some(self.split_page(page))
                        }
                    } else {
                        // Global offset
                        let left = u64::from_le(*(ptr as *const u64));
                        if left==0 {
                            // free left child.
                            if let Some(off_ptr)= page.alloc_key_value(key,value,l,r) {
                                *(ptr as *mut u32) = 1;
                                *((ptr as *mut u32).offset(1)) = off_ptr;
                                incr(ptr.offset(6));
                                Some(Insert::Ok(current))
                            } else {
                                Some(self.split_page(page))
                            }
                        } else {
                            // left child is another page.
                            debug!("left, page_: {}",left);
                            let mut page_=self.load_mut_page(left);
                            if let Some((k,v,l,r)) = self.insert(&mut page_,key,value,0,0) {
                                if let Some(off_ptr)= page.alloc_key_value(k,v,l,r) {
                                    *(ptr as *mut u32) = 1;
                                    *((ptr as *mut u32).offset(1)) = off_ptr;
                                    Some(Insert::Ok(current))
                                } else {
                                    Some(self.split_page(page))
                                }
                            } else {
                                None
                            }
                        }
                    }
                },
                Ordering::Greater =>{
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    debug!("right0={:?}",right0);
                    if right0 == 1 {
                        // local offset
                        if let Some(off_ptr)= page.alloc_key_value(key,value,l,r) {
                            debug!("right0 allocated");
                            let next=u32::from_le(*(ptr.offset(3)));
                            if next==0 {
                                // free left branch
                                *((ptr as *mut u32).offset(3)) = off_ptr;
                                incr(ptr.offset(6));
                                Some(Insert::Ok(current))
                            } else {
                                let result=self.binary_tree_insert(page,key,value,l,r,next);
                                if let Some(Insert::Ok(root))=result {
                                    *((ptr as *mut u32).offset(3)) = root.to_le();
                                    incr(ptr.offset(6));
                                    Some(Insert::Ok(rebalance(page,current)))
                                } else {
                                    result
                                }
                            }
                        } else {
                            debug!("needs to split");
                            Some(self.split_page(page))
                        }
                    } else {
                        // global offset, follow
                        let right = u64::from_le(*((ptr as *const u64).offset(1)));
                        if right==0 {
                            // no child, this is a global leaf, we can insert
                            if let Some(off_ptr)= page.alloc_key_value(key,value,l,r) {
                                *((ptr as *mut u32).offset(2)) = 1;
                                *((ptr as *mut u32).offset(3)) = off_ptr;
                                Some(Insert::Ok(current))
                            } else {
                                Some(self.split_page(page))
                            }
                        } else {
                            debug!("right, page_: {}",right);
                            let mut page_=self.load_mut_page(right);
                            if let Some((k,v,l,r)) = self.insert(&mut page_,key,value,0,0) {
                                if let Some(off_ptr)= page.alloc_key_value(k,v,l,r) {
                                    *((ptr as *mut u32).offset(2)) = 1;
                                    *((ptr as *mut u32).offset(3)) = off_ptr;
                                    incr(ptr.offset(6));
                                    Some(Insert::Ok(current))
                                } else {
                                    Some(self.split_page(page))
                                }
                            } else {
                                None
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn get<'a>(&'a self,key:&[u8],value:Option<&[u8]>)->Option<&'a[u8]> {
        if let Some(root_page) = self.load_root() {
            let root=root_page.root();
            self.binary_tree_get(&root_page,key,value,root)
        } else {
            None
        }
    }

    fn binary_tree_get<'a>(&self, page:&Page, key:&[u8], value:Option<&[u8]>, current:u32)->Option<&'a[u8]> {
        unsafe {
            debug!("binary_tree_get:{:?}",page);
            let ptr=page.offset(current) as *mut u32;

            let value_=value.unwrap_or(b"");
            let (key0,value0)=read_key_value(&*(ptr as *const u8));
            let cmp= if let Some(value_)=value {
                (key,value_).cmp(&(key0,value0))
            } else {
                key.cmp(&key0)
            };
            debug!("({:?},{:?}), {:?}, ({:?},{:?})",
                   std::str::from_utf8_unchecked(key),
                   std::str::from_utf8_unchecked(value_),
                   cmp,
                   std::str::from_utf8_unchecked(key0),
                   std::str::from_utf8_unchecked(value0));
            match cmp {
                Ordering::Equal | Ordering::Less=>{
                    let result={
                        let left0 = u32::from_le(*(ptr as *const u32));
                        if left0 == 1 {
                            let next=u32::from_le(*(ptr.offset(1)));
                            if next==0 {
                                None
                            } else {
                                self.binary_tree_get(page,key,value,next)
                            }
                        } else {
                            // Global offset
                            let left = u64::from_le(*(ptr as *const u64));
                            if left==0 {
                                None
                            } else {
                                // left child is another page.
                                let page_=self.load_page(left);
                                let root_=page_.root();
                                self.binary_tree_get(&page_,key,value,root_)
                            }
                        }
                    };
                    if cmp==Ordering::Equal { Some(result.unwrap_or(value0)) } else { result }
                },
                Ordering::Greater =>{
                    let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                    debug!("right0={:?}",right0);
                    if right0 == 1 {
                        let next=u32::from_le(*(ptr.offset(3)));
                        if next==0 {
                            None
                        } else {
                            self.binary_tree_get(page,key,value,next)
                        }
                    } else {
                        // global offset, follow
                        let right = u64::from_le(*((ptr as *const u64).offset(1)));
                        if right==0 {
                            None
                        } else {
                            // right child is another page
                            let mut page_=self.load_page(right);
                            let root_=page_.root();
                            self.binary_tree_get(&page_,key,value,root_)
                        }
                    }
                }
            }
    }
}
}

unsafe fn incr(p:*mut u32) {
    *p = (u32::from_le(*p) + 1).to_le()
}




fn tree_rotate_clockwise(page:&mut MutPage, v:u32)->u32 {
    unsafe {
        let ptr=page.offset(v) as *mut u32;

        let u_is_local = *ptr == 1;
        if u_is_local {
            let off_u = *(ptr.offset(1));
            let ptr_u=page.offset(off_u) as *mut u32;

            // fetch node size
            let v_size=u32::from_le(*ptr.offset(6));
            let u_size = u32::from_le(*(ptr_u.offset(6)));

            let b_size = {
                if u32::from_le(*(ptr_u.offset(2))) == 1 {
                    let ptr_b = page.offset(u32::from_le(*(ptr_u.offset(3))));
                    u32::from_le(*(ptr_b.offset(6)))
                } else {
                    0
                }
            };

            // Change the left of v to b
            *(ptr as *mut u64) = *((ptr_u as *const u64).offset(1));

            // Change the right of u to v
            *(ptr_u.offset(2)) = 1;
            *(ptr_u.offset(3)) = v;
            //
            *ptr.offset(6) = (v_size - u_size + b_size).to_le();
            *ptr_u.offset(6) = v_size;
            //
            off_u
        } else {
            // Cannot rotate
            v
        }
    }
}
fn tree_rotate_anticlockwise(page:&mut MutPage, u:u32)->u32 {
    unsafe {
        let ptr=page.offset(u) as *mut u32;

        let v_is_local = *(ptr.offset(2)) == 1;
        if v_is_local {
            let off_v = *(ptr.offset(3));
            let ptr_v=page.offset(off_v) as *mut u32;

            // fetch node size
            let u_size=u32::from_le(*ptr.offset(6));
            let v_size = u32::from_le(*(ptr_v.offset(6)));
            let b_size = {
                if u32::from_le(*ptr_v) == 1 {
                    let ptr_b = page.offset(u32::from_le(*(ptr_v.offset(1))));
                    u32::from_le(*(ptr_b.offset(6)))
                } else {
                    0
                }
            };

            // Change the right of u to b
            *((ptr as *mut u64).offset(1)) = *(ptr_v as *const u64);
            // Change the left of v to u
            *ptr_v = 1;
            *(ptr_v.offset(1)) = u;
            //
            *ptr.offset(6) = (u_size - v_size + b_size).to_le();
            *ptr_v.offset(6) = u_size;
            //
            off_v
        } else {
            // Cannot rotate
            u
        }
    }
}
fn rebalance(page:&mut MutPage,node:u32)->u32 {
    unsafe {
        let ptr=page.offset(node) as *mut u32;
        let left_local = u32::from_le(*ptr);
        let right_local = u32::from_le(*(ptr.offset(2)));
        let left_cardinal = {
            if left_local==1 {
                let left=u32::from_le(*(ptr.offset(1)));
                let left_ptr=page.offset(left);
                u32::from_le(*(left_ptr.offset(6)))
            } else {
                0
            }
        };
        let right_cardinal = {
            if right_local==1 {
                let right=u32::from_le(*(ptr.offset(3)));
                let right_ptr=page.offset(right);
                u32::from_le(*(right_ptr.offset(6)))
            } else {
                0
            }
        };
        if left_cardinal+2 < right_cardinal {
            tree_rotate_anticlockwise(page,node)
        } else if right_cardinal+2 < left_cardinal {
            tree_rotate_clockwise(page,node)
        } else {
            node
        }
    }
}


enum Insert<'a> {
    Ok(u32),
    Split{key:&'a[u8],value:&'a[u8],left:u64,right:u64}
}
