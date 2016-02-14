extern crate dictionnaire;
use dictionnaire::*;
extern crate env_logger;
fn main(){
    env_logger::init().unwrap();
    //Env::test_concat_mmap("/tmp/test", &[(0,4096), (20480,4096)]);
    let env=Env::new("/tmp/test").unwrap();
    let env=std::sync::Arc::new(env);
    let thr={
        let env=env.clone();
        env.debug();
        std::thread::spawn(move | | {
            let mut txn=env.txn_mut_begin().unwrap();
            let mut page=txn.alloc_page().unwrap();
            println!("first alloc done");
            {
                let mut p=unsafe { page.as_mut_slice() };
                let v=b"blabla";
                for i in 0..v.len() {
                    p[i]=v[i]
                }
            }
            let mut page=txn.alloc_page().unwrap();
            println!("second alloc done");
            {
                let mut p=unsafe { page.as_mut_slice() };
                let v=b"blibli";
                for i in 0..v.len() {
                    p[i]=v[i]
                }
            }
            txn.free_page(page);
            println!("free done");
            txn.commit().unwrap();
        })
    };
    thr.join().unwrap();
    env.debug();
}
