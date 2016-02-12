extern crate dictionnaire;
use dictionnaire::*;

fn main(){
    let env=Env::new("/tmp/test").unwrap();
    let env=std::sync::Arc::new(env);
    let thr={
        let env=env.clone();
        std::thread::spawn(move | | {
            let mut txn=env.txn_mut_begin().unwrap();
            {
                let mut page=txn.alloc_page().unwrap();
                let mut p=unsafe { page.as_mut_slice() };
                let v=b"blabla";
                for i in 0..v.len() {
                    p[i]=v[i]
                }
            }
            let mut page=txn.alloc_page().unwrap();
            {
                let mut p=unsafe { page.as_mut_slice() };
                let v=b"blibli";
                for i in 0..v.len() {
                    p[i]=v[i]
                }
            }
            txn.free_page(page);
            txn.commit().unwrap();
        })
    };
    thr.join().unwrap();
}
