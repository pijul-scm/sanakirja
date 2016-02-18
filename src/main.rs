extern crate dictionnaire;
#[macro_use]
extern crate log;
extern crate libc;
extern crate fs2;
extern crate env_logger;

use dictionnaire::*;

//mod transaction;
//use transaction::*;

fn main(){
    env_logger::init().unwrap();
    //Env::test_concat_mmap("/tmp/test", &[(0,4096), (20480,4096)]);
    let env=Env::new("/tmp/test").unwrap();
    let mut txn=env.mut_txn_begin();

    println!("root:{:?}",txn.btree_root);
    let mut root=txn.load_root().unwrap_or_else(|| {
        println!("needs alloc");
        let btree=txn.alloc_b_leaf(1);
        let off=btree.offset();
        txn.btree_root = off;
        btree
    });
    root.insert(b"blabla",b"blibli");
    println!("{:?}",txn.btree_root);
    txn.commit().unwrap();

    let mut txn=env.mut_txn_begin();
    println!("root:{:?}",txn.btree_root);

    //let env=std::sync::Arc::new(env);
    /*
    let thr={
        let env=env.clone();
        println!("before spawn statistics: {:?}",env.statistics());
        std::thread::spawn(move | | {

            let mut txn=env.mut_txn_begin();
            let mut page0=txn.alloc_page().unwrap();
            println!("first alloc done");
            {
                let mut p=unsafe { page0.as_mut_slice() };
                let v=b"blabla";
                for i in 0..v.len() {
                    p[i]=v[i]
                }
            }
            let mut page1=txn.alloc_page().unwrap();
            println!("second alloc done");
            {
                let mut p=unsafe { page1.as_mut_slice() };
                let v=b"blibli";
                for i in 0..v.len() {
                    p[i]=v[i]
                }
            }
            page0.free(&mut txn);
            //let pages=[page0,page1];
            //txn.glue_mut_pages(&pages).unwrap();
            println!("free done");
            txn.commit().unwrap();

        })
    };
    thr.join().unwrap();
*/
    //println!("final statistics: {:?}",env.statistics());
}
