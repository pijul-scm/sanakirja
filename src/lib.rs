extern crate libc;
#[macro_use]
extern crate log;
extern crate fs2;

use std::path::Path;

mod constants;
mod transaction;

pub use transaction::{Statistics};

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
/*
fn get_page(p:&Page,key:&[u8]) {
    let root=if p.offset==0 { 64 } else { 0 };
    
}

fn insert_into_page(p:&Page,key:&[u8]) {
    // This is a binary tree
    //let root=if p.offset==0 {
}
*/
impl<'env> Txn<'env> {

}
