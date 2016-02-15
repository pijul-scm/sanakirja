extern crate libc;
#[macro_use]
extern crate log;
extern crate fs2;

use std::path::Path;

mod constants;
mod transaction;

pub use transaction::{Statistics};
use transaction::{Page,readle_64};

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
        let left_child = left_child & (!1);
        let right_child = readle_64(cur.offset(8));
        let length = readle_64(cur.offset(16));
        if length==0 {
            0
        } else {
            let value=std::slice::from_raw_parts(cur.offset(24),length as usize);
            if is_leaf {
                if key<value { left_child } else { right_child }
            } else {
                rec_get_page(root,root.offset(if key<value { left_child } else { right_child} as isize), key)
            }
        }
    }
}
fn get_page(p:&Page, key:&[u8])->u64 {
    unsafe {
        let p_data=if p.offset==0 { p.data.offset(24) } else { p.data };
        let length_extra_pages= readle_64(p_data);
        // TODO: glue all pages together, resulting in pointer p.
        // Right now let's assume length_extra_pages==0
        assert!(length_extra_pages==0);
        let p=p_data.offset(8);
        rec_get_page(p,p,key)
    }
}
/*
fn insert_into_page(p:&Page,key:&[u8]) {
    // This is a binary tree
    //let root=if p.offset==0 {
}
*/
impl<'env> Txn<'env> {

}
