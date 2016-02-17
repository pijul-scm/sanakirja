extern crate libc;
#[macro_use]
extern crate log;
extern crate fs2;

use std::path::Path;
use std::ptr::copy_nonoverlapping;
use std::cmp::Ordering;
use std::marker::PhantomData;

mod constants;
mod transaction;

pub use transaction::{Statistics};
use transaction::{PAGE_SIZE,PAGE_SIZE_64};


pub struct MutTxn<'env> {
    txn:transaction::MutTxn<'env>
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
        transaction::Env::new(file,13 + 4).and_then(|env| Ok(Env {env:env}))
    }
    pub fn statistics(&self)->Statistics {
        self.env.statistics()
    }
    pub fn txn_begin<'env>(&'env self)->Txn<'env> {
        Txn { txn:self.env.txn_begin() }
    }
    pub fn mut_txn_begin<'env>(&'env self)->MutTxn<'env> {
        MutTxn { txn:self.env.mut_txn_begin() }
    }
}


const LEAF_CONTENTS_OFFSET:isize=8; // in bytes.
const PAGE:u64 = PAGE_SIZE as u64;

struct Page {
    page:transaction::Page,
}

impl Page {
    // Page layout: Starts with a header of ((n>=1)*8 bytes + 16 bytes).
    // - 64 bits: glueing number (0 for now), + flags on the 13 least significant bits
    // - 32 bits: RC
    // - 32 bits: offset of the first free spot, from the beginning of the page
    // - 32 bits: offset of the root of the tree, from the beginning of the page
    // - 32 bits: how much space is occupied in this page? (controls compaction)
    // - beginning of coding space (different encodings in B-nodes and B-leaves)

    // returns a pointer to the last glue number.
    fn skip_glues(&self)->*const u64 {
        unsafe {
            let mut p=self.page.data as *const u64;
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
    // First free spot in this page (head of the linked list, number of |u32| from the start of the coding zone)
    fn first_free(&self)->u32 {
        unsafe {
            let p=(self.skip_glues() as *const u32).offset(3);
            u32::from_le(*p)
        }
    }
    // Root of the binary tree (number of |u32| from the start of the coding zone)
    fn root(&self)->Tree<()> {
        unsafe {
            let p=(self.skip_glues() as *mut u32).offset(4);
            let off=u32::from_le(*p);
            Tree { p:p, node:off as isize,phantom:PhantomData }
        }
    }
    // Amount of space occupied in the page
    fn occupied_space(&self)->u32 {
        unsafe {
            let p=(self.skip_glues() as *const u32).offset(5);
            u32::from_le(*p)
        }
    }
    /*
    fn allocate_tree<T:Value>(&mut self,key:&[u8],value:T)->Tree<T> {
        unimplemented!()
    }
     */
}

trait Value<'a> {
    // ptr is guaranteed to be 32-bit aligned
    fn read(ptr:&'a u32)->(&'a[u8],Self);
    fn write(ptr:*mut u32,key:&[u8],value:Self);
}

struct Tree<T> {
    p:*mut u32, // pointer to the last glue number.
    node:isize, // offset (in u32) from p. Address of the current node: self.p.offset(self.node). 0 is invalid.
    phantom:PhantomData<T>
}

impl<'a> Value<'a> for &'a [u8] {
    fn read(p:&'a u32)->(&'a[u8],&'a[u8]) {
        // Layout of these nodes: |key| (32-bits aligned), |value| (32-bits aligned), key, value
        unsafe {
            let p:*const u32=p as *const u32;
            let key_len=u32::from_le(*p);
            let val_len=u32::from_le(*(p.offset(1)));
            let key_ptr=p.offset(2);
            let val_ptr=(p.offset(2) as *const u8).offset(key_len as isize);
            (std::slice::from_raw_parts(key_ptr as *const u8, key_len as usize),
             std::slice::from_raw_parts(val_ptr as *const u8, val_len as usize))
        }
    }
    fn write(ptr:*mut u32,key:&[u8],value:&[u8]) {
        unimplemented!()
    }
}

impl<'a> Value<'a> for u64 {
    // The issue here is, we're not guaranteed that p is 64-bits aligned, and yet we need to read a 64-bits value.
    fn read(p:&'a u32)->(&'a[u8],u64) {
        // Layout of these nodes: |key| (32-bits aligned), 32 lowest bits of value, 32 highest bits of value, key.
        unsafe {
            let p:*const u32=p as *const u32;
            let key_len=u32::from_le(*p);
            let low_bits=(u32::from_le(*(p.offset(1)))) as u64;
            let high_bits=(u32::from_le(*(p.offset(2)))) as u64;
            let key_ptr=p.offset(3);
            let key=std::slice::from_raw_parts(key_ptr as *const u8, key_len as usize);
            (key,(high_bits<<32)|low_bits)
        }
    }
    fn write(ptr:*mut u32,key:&[u8],value:u64) {
        unimplemented!()
    }
}

const NODE_FLAG:u16=1;
impl <'a,T:Value<'a>>Tree<T> {
    fn left(&self)->Option<Tree<T>> {
        unsafe {
            let left=u32::from_le(*self.p.offset(self.node));
            if left==0 { None } else { Some(Tree { p:self.p, node: left as isize,phantom:PhantomData }) }
        }
    }
    fn set_left(&mut self,left:Option<Tree<T>>) {
        unsafe {
            (*self.p.offset(self.node)) = match left { Some(left)=>(left.node as u32).to_le(), None=>0 };
        }
    }
    fn right(&self)->Option<Tree<T>> {
        unsafe {
            let right=u32::from_le(*(self.p.offset(self.node + 1)));
            if right==0 { None } else { Some(Tree { p:self.p, node: right as isize,phantom:PhantomData }) }
        }
    }
    fn set_right(&mut self,right:Option<Tree<T>>) {
        unsafe {
            (*self.p.offset(self.node+1)) = match right { Some(right)=>(right.node as u32).to_le(), None=>0 };
        }
    }
    fn balance(&self)->i32 {
        unsafe { i32::from_le(*(self.p.offset(self.node+2) as *const i32)) }
    }
    fn set_balance(&mut self,balance:i32) {
        unsafe { *(self.p.offset(self.node+2) as *mut i32) = balance.to_le() }
    }
    fn read(&'a self)->(&'a[u8],T) {
        unsafe { T::read(&*self.p.offset(self.node+3)) }
    }
}




impl<'env> MutTxn<'env> {




    /*
    fn insert_leaf_page<'txn>(&'txn mut self,page_off:u64,key:&[u8],value:&[u8]) {
        unsafe {
            let mutpage=self.txn.load_mut_page(page_off);
            let glues_flags=readle_64(mutpage.data);
            {
                let mut current_glue= glues_flags & (!(PAGE-1));
                while current_glue!=0 {
                    unimplemented!()
                }
            }
            let flags=glues_flags & (PAGE-1);
            // Start looking for free space.
            let first_free=readle_32(mutpage.data.offset(8));
            let root=readle_32(mutpage.data.offset(12));
            let occupied_space = readle_32(mutpage.data.offset(16));
            //
            let pstart=mutpage.data.offset(24);
            let pfree=pstart.offset(first_free as isize);
            if first_free==0 {
                // we're the first key to use this page. No need to explore any tree.
                writele_32(pfree,0); // L
                writele_32(pfree.offset(4),0); // R

                let next_free=32 + key.len() + value.len();
                writele_64(pfree.offset(next_free as isize), 0); // mark next free zone.
            } else {
                // there is already a tree in place.
                // returns Less if key is less than cur.
                struct Node<'txn> {
                    left:u32,
                    right:u32,
                    n_leaves:u16,
                    flags:u16,
                    key:&'txn [u8],
                    value:&'txn [u8],
                }
                impl<'txn> Node<'txn> {
                    fn is_leaf(&self)->bool { self.flags & 1 == 0 }
                }
                fn read_node<'txn>(root:*mut u8,cur:u32)->Node<'txn> {
                    let pcur=root.offset(cur as isize);
                    let left_right_is_leaf=readle_64(pcur);

                    let length_key=readle_64(pcur.offset(LEAF_CONTENTS_OFFSET));
                    let length_value=readle_64(pcur.offset(LEAF_CONTENTS_OFFSET+8));
                    let node_key={
                        std::slice::from_raw_parts(pcur.offset(LEAF_CONTENTS_OFFSET+16), length_key as usize)
                    };
                    let node_value={
                        std::slice::from_raw_parts(pcur.offset(LEAF_CONTENTS_OFFSET+16+length_key as isize),
                                                   length_value as usize)
                    };
                    Node {
                        left:((left_right_is_leaf >> 48) & 0x10000) as u16,
                        right:((left_right_is_leaf >> 32) & 0x10000) as u16,
                        n_leaves:((left_right_is_leaf >> 16) & 0x10000) as u16,
                        flags: (left_right_is_leaf & 0x10000) as u16,
                        key:node_key,
                        value:node_value,
                    }
                }

                // Now find where to insert it in the tree.
                fn find_leaf(root:*mut u8, free:u32, cur:u32, key:&[u8],value:&[u8])->u32 {
                    unsafe {
                        let pcur=root.offset(cur as isize);
                        let node=read_node(root,cur);
                        let ord=(key,value).cmp(&(node.key,node.value));
                        unsafe {
                            if node.is_leaf() {
                                if node.left==0 {
                                    // leaf with free left
                                    if node.right==0 {
                                        // both free, place wherever it fits.
                                        match ord {
                                            Ordering::Equal|Ordering::Less=>{ //writele_64(pcur,free);
                                                cur },
                                            Ordering::Greater=>{ //writele_64(pcur.offset(8),free);
                                                cur }
                                        }
                                    } else {
                                        0
                        /*
                                        // just the LHS free. If
                                        // ord==Less or ord==Equal,
                                        // ok. Else, we need to
                                        // compare and rotate.
                                        match ord {
                                            Ordering::Equal|Ordering::Less=> { writele_64(pcur,free); cur },
                                            Ordering::Greater=>{
                                                match cmp(root.offset(right as isize),key,value) {
                                                    Ordering::Equal|Ordering::Less=>{
                                                        let pfree=root.offset(free as isize);
                                                        // Set pfree as the center node, with children cur and right.
                                                        writele_64(pfree,cur);
                                                        writele_64(pfree.offset(8),right);
                                                        // Now set the children of cur and right to 0;
                                                        writele_64(pcur,0);writele_64(pcur.offset(8),0);
                                                        let pright=root.offset(right as isize);
                                                        writele_64(pright,0);writele_64(pright.offset(8),0);
                                                        // return the new center node
                                                        free
                                                    },
                                                    Ordering::Greater=>{
                                                        let pright=root.offset(right as isize);
                                                        writele_64(pright,cur);
                                                        writele_64(pright.offset(8),free);
                                                        // now set the children of cur and free to 0
                                                        writele_64(pcur,0);writele_64(pcur.offset(8),0);
                                                        let pfree=root.offset(free as isize);
                                                        writele_64(pfree,0);writele_64(pfree.offset(8),0);
                                                        //
                                                        right
                                                    }
                                                }
                                            }
                                        }
                                         */
                                    }
                                } else { // the right child must be empty
                                    0
                                    /*
                                        // leaf with non-free left, and free right. If ord==Greater, ok.
                                    match ord {
                                        Ordering::Greater => {writele_64(pcur.offset(8),free); cur},
                                        Ordering::Equal|Ordering::Less=>{
                                            match cmp(root.offset(left as isize),key,value) {
                                                Ordering::Greater=>{
                                                    let pfree=root.offset(free as isize);
                                                    // Set pfree as the center node, with children left and cur
                                                    writele_64(pfree,left);
                                                    writele_64(pfree.offset(8),cur);
                                                    // Now set the children of left and cur to 0;
                                                    writele_64(pcur,0);writele_64(pcur.offset(8),0);
                                                    let pleft=root.offset(left as isize);
                                                    writele_64(pleft,0);writele_64(pleft.offset(8),0);
                                                    // return the new center node
                                                    free
                                                },
                                                Ordering::Equal|Ordering::Less=>{
                                                    let pleft=root.offset(left as isize);
                                                    writele_64(pleft,free);
                                                    writele_64(pleft.offset(8),cur);
                                                    // now set the children of cur and free to 0
                                                    writele_64(pcur,0);writele_64(pcur.offset(8),0);
                                                    let pfree=root.offset(free as isize);
                                                    writele_64(pfree,0);writele_64(pfree.offset(8),0);
                                                    //
                                                    left
                                                }
                                            }
                                        }
                                    }
                                     */
                                }
                            } else {
                                // None free, recurse
                                match ord {
                                    Ordering::Less |
                                    Ordering::Equal=>{
                                        let next=find_leaf(root,free,node.left,key,value);
                                        //writele_64(pcur,next);
                                        cur
                                    },
                                    Ordering::Greater=>{
                                        let next=find_leaf(root,free,node.right,key,value);
                                        //writele_64(pcur.offset(8),next);
                                        cur
                                    }
                                }
                            }
                        }
                    }
                }
                let next_root=find_leaf(pstart,first_free,root,key,value);

                // Where to put the new node (will be a leaf).
                let next_free={
                    let next_free=readle_64(pfree);
                    if next_free==0 {
                        first_free + (32+key.len()+value.len()) as u32
                    } else {
                        next_free as u32
                    }
                };
                // Finally, copy the key and value to the correct location
                {
                    writele_64(pfree.offset(LEAF_CONTENTS_OFFSET),key.len() as u64); // key len
                    writele_64(pfree.offset(LEAF_CONTENTS_OFFSET+8),value.len() as u64); // value len
                    copy_nonoverlapping(key.as_ptr(),pfree.offset(LEAF_CONTENTS_OFFSET+16), key.len());
                    copy_nonoverlapping(value.as_ptr(),pfree.offset(LEAF_CONTENTS_OFFSET+16 + key.len() as isize), value.len());
                }
                //writele_64(p_pointers, (next_free<<12 | next_root))
            }
        }
    }
    fn insert_node_page(&mut self,page_off:u64,key:&[u8],page_address:u64) {
        /*
        let mutpage=self.load_mut_page(page_off);
        let rc= if page_off==0 { mutpage.data.offset(32) } else { mutpage.data.offset(8) };
        assert!((*rc)&1==0);
        let first_free=*(rc.offset(8));
        if first_free==0 { // uninitialized
            unsafe {
                writele_64(rc.offset(16),0);
                writele_64(rc.offset(24),0);
                writele_64(rc.offset(32),key.len() as u64);
                copy_nonoverlapping(key.as_ptr(), 
        } else {

        }
         */
    }
*/
}
