extern crate libc;
#[macro_use]
extern crate log;
extern crate fs2;

use std::path::Path;
use std::ptr::copy_nonoverlapping;
use std::cmp::Ordering;

mod constants;
mod transaction;

pub use transaction::{Statistics};
use transaction::{MutPage,Page,readle_64,writele_64};
use constants::PAGE_SIZE;

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



/// A return value of 0 means "not found".
fn rec_get_page(root:*const u8,cur:*const u8,key:&[u8])->u64 {
    unsafe {
        let left_child= readle_64(cur);
        let is_leaf = left_child & 1 == 0;
        let left_child = left_child >> 1;
        let right_child = readle_64(cur.offset(8));
        let length = readle_64(cur.offset(16));
        let value=std::slice::from_raw_parts(cur.offset(24),length as usize);
        if is_leaf {
            if key<value { left_child } else { right_child }
        } else {
            rec_get_page(root,root.offset(if key<value { left_child } else { right_child} as isize), key)
        }
    }
}
fn get_page(p:&Page, key:&[u8])->u64 {
    unsafe {
        let p_data=if p.offset==0 { p.data.offset(24) } else { p.data };

        let extra_page=readle_64(p_data);
        assert!(extra_page==0); // we just read the blocker

        let rc=p_data.offset(8); // reference counter
        assert!((*rc)&1 == 0); // assume fixed-length for now.

        let root=p_data.offset(16);
        rec_get_page(p_data,p_data,key)
    }
}

pub unsafe fn readle_16(p0:*const u8)->u16 {
    (*(p0 as *const u16)).to_le()
}

pub unsafe fn writele_16(p:*mut u8,v:u16) {
    *(p as *mut u16) = v.to_le()
}
pub unsafe fn readle_32(p0:*const u8)->u32 {
    (*(p0 as *const u32)).to_le()
}

pub unsafe fn writele_32(p:*mut u8,v:u32) {
    *(p as *mut u32) = v.to_le()
}


const LEAF_CONTENTS_OFFSET:isize=8; // in bytes.
const PAGE:u64 = PAGE_SIZE as u64;
// The difference between fixed and non-fixed size is just the need for compaction.
// Fixed sizes maintain a list of free zones.
impl<'env> MutTxn<'env> {

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
}
