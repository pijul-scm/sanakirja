extern crate dictionnaire;
#[macro_use]
extern crate log;
extern crate libc;
extern crate fs2;
extern crate env_logger;

extern crate rand;
use rand::{thread_rng,sample};

use dictionnaire::*;

//mod transaction;
//use transaction::*;

fn main(){
    env_logger::init().unwrap();
    //Env::test_concat_mmap("/tmp/test", &[(0,4096), (20480,4096)]);
    let env=Env::new("/tmp/test").unwrap();
    let mut txn=env.mut_txn_begin();
    let mut bindings=Vec::new();
    for i in 0..100 {
        let x=format!("{}",i);
        let y=format!("{}",(i*i)%17);
        txn.put(x.as_bytes(),y.as_bytes());
        bindings.push((x,y));
        //txn.put(b"blublu",b"blibli");
    }
    let mut rng=thread_rng();
    for &(ref u,ref v) in sample(&mut rng, bindings.iter(), 10) {
        let x= unsafe { txn.get(u.as_bytes(),None).map(|x| std::str::from_utf8_unchecked(x)) };
        println!("{},{},{:?}",u,v,x)
    }
    txn.debug("/tmp/debug");
    txn.commit().unwrap();
    println!("final statistics: {:?}",env.statistics());

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
}
