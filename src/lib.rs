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
        transaction::Env::new(file,13 + 4).and_then(|env| Ok(Env {env:env}))
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


const LEAF_CONTENTS_OFFSET:isize=8; // in bytes.
const PAGE:u64 = PAGE_SIZE as u64;

pub struct Page {
    page:transaction::MutPage,
}
const B_NODE_FLAG:u64 = 1;

impl Page {
    // Page layout: Starts with a header of ((n>=1)*8 bytes + 16 bytes).
    // - 64 bits: glueing number (0 for now), + flags on the 13 least significant bits
    // - 32 bits: RC
    // - 32 bits: offset of the first free spot, from the beginning of the page
    // - 32 bits: offset of the root of the tree, from the beginning of the page
    // - 32 bits: how much space is occupied in this page? (controls compaction)
    // - beginning of coding space (different encodings in B-nodes and B-leaves)

    // returns a pointer to the last glue number.
    fn skip_glues(&self)->*mut u64 {
        unsafe {
            let mut p=self.page.data as *mut u64;
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
    /*
    // load the binary tree.
    fn root<'a,'b>(&'a self)->B<'a,'b> {
        unsafe {
            let p=self.skip_glues() as *mut u64;
            let flags=(*p)&(PAGE_SIZE_64-1);
            let p_root=(p as *mut u32).offset(4);
            let off=u32::from_le(*p_root);
            assert!(off>0);
            if flags&B_NODE_FLAG == 0 {
                B::Leaf(Tree { p:p_root, node:off as isize,phantom:PhantomData })
            } else {
                B::Node(Tree { p:p_root, node:off as isize,phantom:PhantomData })
            }
        }
    }
     */

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
            println!("alloc: {:?} {:?}, {:?}",current,size,next_page);
            if current.offset(size as isize) > next_page {
                return None
            } else {
                *(p.offset(3)) = first_free + ((size as u32) >> 2);
                Some(first_free)
                    //Some(Tree::new(p,first_free as isize,key,value))
            }
        }
    }

    fn insert(&mut self,key:&[u8],value:&[u8]) {
        unsafe {
            // size in bytes
            let size=24
                + 4 //balance
                + key.len() + value.len();
            let size = size + (4-(size&3))&3;

            let off_ptr = self.alloc(size).unwrap();
            // off is the beginning of a free zone. Write the node there.
            //////////////////////////////////////////////////
            let ptr=self.offset(off_ptr);
            println!("ptr: {} {:?}",off_ptr,ptr);
            {
                let ptr=ptr as *mut u64;
                *ptr = 0; // left
                *(ptr.offset(1)) = 0; //right
            }

            {
                let ptr=ptr as *mut u32;
                *(ptr.offset(2)) = (key.len() as u32).to_le();
                *(ptr.offset(3)) = (value.len() as u32).to_le();
            }
            let ptr=ptr as *mut u8;
            *(ptr.offset(24)) = 0; // balance number
            copy_nonoverlapping(key.as_ptr(), ptr.offset(NODE_HEADER_BYTES) as *mut u8, key.len());
            copy_nonoverlapping(value.as_ptr(), ptr.offset(NODE_HEADER_BYTES + key.len() as isize), value.len());

            // Maintenant, on doit l'accrocher à l'arbre.
            let root=self.root();
            //
            println!("insert root:{:?} {:?}",root,off_ptr);
            if root==0 {
                self.set_root(off_ptr)
            } else {
                // compare
                let mut current=root;
                loop {
                    let ptr=self.offset(current);
                    let (key0,value0)=read_key_value(&*(ptr as *const u8));
                    println!("{:?},{:?}",key,value);
                    break
                }
            }
        }
    }

}
const NODE_HEADER_BYTES:isize=25;
fn read_key_value<'a>(p:&'a u8)->(&'a [u8],&'a[u8]) {
    unsafe {
        let p32=p as *const u8 as *const u32;
        let key_len=*(p32.offset(4));
        let val_len=*(p32.offset(5));
        (std::slice::from_raw_parts((p as *const u8).offset(NODE_HEADER_BYTES), key_len as usize),
         std::slice::from_raw_parts((p as *const u8).offset(NODE_HEADER_BYTES + key_len as isize), val_len as usize))
    }
}


pub enum BTree {
    Leaf { page:Page },
    Node { page:Page }
}
impl BTree {
    pub fn offset(&self)->u64 {
        match self {
            &BTree::Leaf { ref page }=>page.page.offset,
            &BTree::Node { ref page }=>page.page.offset,
        }
    }
    pub fn insert(&mut self,key:&[u8],value:&[u8]) {
        match self {
            &mut BTree::Leaf { ref mut page } => page.insert(key,value),
            &mut BTree::Node { ref mut page } => unimplemented!()
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
    pub fn load_root(&mut self)->Option<BTree> {
        if self.btree_root == 0 {
            None
        } else {
            // Here, go to page and load it.
            unsafe {
                let page=self.txn.load_mut_page(self.btree_root);
                let p=page.data as *mut u64;
                let glues= *p;
                assert!(glues < PAGE_SIZE_64);
                Some(if glues & 1 == 0 {
                    BTree::Leaf { page:Page { page:page } }
                } else {
                    BTree::Node { page:Page { page:page } }
                })
            }
        }
    }
    pub fn alloc_b_leaf<'a>(&mut self,n_pages:usize)->BTree {
        unsafe {
            assert!(n_pages==1);
            let page=self.txn.alloc_page().unwrap();
            let p=page.data as *mut u64;
            println!("p:{:?}", p);
            *p = 0; // glue number + "leaf" tag
            let p=page.data as *mut u32;
            *(p.offset(2)) = 1; // reference counter.
            *(p.offset(3)) = 0; // offset of the first free spot.
            *(p.offset(4)) = 0; // offset of the root.
            *(p.offset(5)) = 0; // occupied space.
            BTree::Leaf { page:Page{page:page} }
        }
    }

}



pub trait Value<'a> {
    // ptr is guaranteed to be 32-bit aligned
    fn read(ptr:&'a u32)->(&'a[u8],Self);
    fn write(ptr:*mut u32,key:&[u8],value:Self);
    fn node_size(key:&[u8],Self)->usize; // size in bytes of a this node. Must be a multiple of 4
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
        unsafe {
            *ptr = key.len() as u32;
            *(ptr.offset(1)) = value.len() as u32;
            copy_nonoverlapping(key.as_ptr(), ptr.offset(2) as *mut u8, key.len());
            copy_nonoverlapping(value.as_ptr(), (ptr.offset(2) as *mut u8).offset(key.len() as isize), value.len());
        }
    }
    fn node_size(key:&[u8],value:&[u8])->usize {
        let s=12 // header common to all nodes
            + 8 // size of lengths
            + key.len()
            + value.len();
        // round to 32 bits alignment
        if s & 3 == 0 { s } else { 4 + (s&(!3)) }
    }
}





/*
enum B<'b,'a> {
    Node(Tree<'b,u64>),
    Leaf(Tree<'b,&'a[u8]>)
}

*/

/*
const NODE_FLAG:u16=1;
impl <'b,'a,T:Value<'a>>Tree<'b,T> {
    // Layout of a tree:
    // - 32 bits Left, offset from the 32-bits word before the coding zone.
    // - 32 bits Right, offset from the 32-bits word before the coding zone.
    // - 32 bits balance number
    // - (encoding of key and T, must start with a 32-bits word)

    // Writes a tree at the given pointer.
    fn new(p:*mut u32,node:isize,key:&[u8],value:T)->Tree<T> {
        unsafe {
            let pp=p.offset(node);
            *pp=0;
            *(pp.offset(1))=0;
            *(pp.offset(2))=0;
            Value::write(pp.offset(3),key,value);
            Tree { p:p,node:node,phantom:PhantomData }
        }
    }
    fn left(&self)->Option<Tree<T>> {
        unsafe {
            let left=u32::from_le(*self.p.offset(self.node));
            if left==0 { None } else { Some(Tree { p:self.p, node: left as isize,phantom:PhantomData }) }
        }
    }
    fn write_left(&mut self,left:Option<Tree<T>>) {
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
    fn write_right(&mut self,right:Option<Tree<T>>) {
        unsafe {
            (*self.p.offset(self.node+1)) = match right { Some(right)=>(right.node as u32).to_le(), None=>0 };
        }
    }
    fn balance(&self)->i32 {
        unsafe { i32::from_le(*(self.p.offset(self.node+2) as *const i32)) }
    }
    fn write_balance(&mut self,balance:i32) {
        unsafe { *(self.p.offset(self.node+2) as *mut i32) = balance.to_le() }
    }
    fn read(&'a self)->(&'a[u8],T) {
        unsafe { T::read(&*self.p.offset(self.node+3)) }
    }
}

*/






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
