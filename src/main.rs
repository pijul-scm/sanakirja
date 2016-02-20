extern crate dictionnaire;
#[macro_use]
extern crate log;
extern crate libc;
extern crate fs2;
extern crate env_logger;

extern crate rand;
use rand::{Rng,thread_rng,sample};

use dictionnaire::*;

//mod transaction;
//use transaction::*;
extern crate tempdir;

fn main(){
    env_logger::init().unwrap();
    //let dir = self::tempdir::TempDir::new("dictionnaire").unwrap();
    //let env=Env::new(dir.path()).unwrap();
    let env=Env::new("/tmp/test").unwrap();

    let n=248;
    let m=n/10;

    let mut bindings=Vec::with_capacity(n);
    /*
    {
        let mut txn=env.mut_txn_begin();
        for i in 0..100 {
            let x=format!("{}",i);
            let y=format!("{}",(i*i)%17);
            txn.put(x.as_bytes(),y.as_bytes());
            bindings.push((x,y,false));
        }
        // txn not commited = cancelled
    }
     */
    let mut rng=thread_rng();
    {
        let mut txn=env.mut_txn_begin();
        for i in 0..n {
            //let x=rng.gen::<i32>();
            //let y=rng.gen::<i32>();
            let sx=format!("{}",i);
            let sy=format!("{}",(i*i)%17);
            println!("\n\n{}: {},{}\n",i,sx,sy);
            txn.put(sx.as_bytes(),sy.as_bytes());
            //txn.debug("/tmp/debug.tmp");
            std::fs::rename("/tmp/debug.tmp","/tmp/debug");
            bindings.push((sx,sy,true));
        }
        println!("debug");
        txn.debug("/tmp/debug.tmp");
        std::fs::rename("/tmp/debug.tmp","/tmp/debug");
        println!("commit");
        txn.commit().unwrap();
    }

    let txn=env.txn_begin();
    for &(ref sx,ref sy,ref b) in bindings.iter() {//sample(&mut rng, bindings.iter(), m) {
        //println!("getting {:?}",sx);
        if let Some(y)=txn.get(sx.as_bytes(),None) {
            assert!(*b && y==sy.as_bytes())
        } else {
            assert!(! *b)
        }
    }

    //txn.debug("/tmp/debug");
    //println!("final statistics: {:?}",env.statistics());

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
