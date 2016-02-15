extern crate dictionnaire;
use dictionnaire::*;
extern crate env_logger;
extern crate fs2;
use fs2::FileExt;
use std::fs::File;

fn main(){
    let f=File::create("/tmp/truc").unwrap();
    f.lock_exclusive().unwrap();
    env_logger::init().unwrap();
    //Env::test_concat_mmap("/tmp/test", &[(0,4096), (20480,4096)]);
    let env=Env::new("/tmp/test",100).unwrap();
    let env=std::sync::Arc::new(env);
    let thr={
        let env=env.clone();
        println!("before spawn statistics: {:?}",env.statistics());
        std::thread::spawn(move | | {
            let mut txn=env.txn_mut_begin().unwrap();
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
            //page.free(&mut txn);
            let pages=[page0,page1];
            txn.glue_mut_pages(&pages).unwrap();
            println!("free done");
            txn.commit().unwrap();
        })
    };
    thr.join().unwrap();
    println!("final statistics: {:?}",env.statistics());
}
