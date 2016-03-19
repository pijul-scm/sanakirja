use super::txn::*;
use super::put;
use super::transaction;
use std;
use super::rebalance::*;
use std::cmp::Ordering;

#[derive(Debug)]
enum Result {
    Ok {
        page: MutPage,
        off: u16,
        decr: bool
    },
    NotFound
}

#[derive(Debug)]
struct Reinsert {
    page: u64,
    off:u16,
    left:u64,
    right:u64
}

// Del returns none if no page below its argument was modified.
// If it returns not none, then modify the current page

pub fn del(txn: &mut MutTxn, db: Db, key: &[u8], value: Option<&[u8]>) -> Db {
    let page = txn.load_cow_page(db.root);
    let current = page.root();
    let value = value.unwrap();
    let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
    let (result,reinsert)=delete(txn, page, current, C::KV { key_ptr:key.as_ptr(), key_len:key.len(), value:value }, 0, 0);
    let page = match result {
        Result::Ok{page,off,..} => {
            // Maybe off==0, but every database needs to have at least one page.
            page.set_root(off);
            page
        },
        Result::NotFound => {
            return db
        }
    };
    if let Some(reinsert)=reinsert {
        unsafe {

            let reins_page = txn.load_cow_page(reinsert.page);
            let reins_page = reins_page.into_mut_page(txn);
            let ptr = reins_page.offset(reinsert.off as isize) as *mut u32;
            let (key_ptr,key_len,value) = {
                let (key, value) = txn.read_key_value(ptr as *const u8);
                (key.as_ptr(),key.len(),value)
            };
            let db = put::put_lr(txn,Cow::from_mut_page(page),
                                 key.as_ptr(),
                                 key.len(),
                                 value,
                                 reinsert.left,
                                 reinsert.right);
            // Done copying the value, we can safely free its page.
            if reinsert.page>0 {
                transaction::free(&mut txn.txn, reinsert.page)
            }
            db
        }
    } else {
        Db { root:page.page_offset() }
    }
}

// The kind of comparison we want
enum C {
    KV { key_ptr:*const u8, key_len:usize, value:UnsafeValue },
    Less
}
impl C {
    fn is_less(&self)->bool {
        match self {
            &C::Less => true,
            _ => false
        }
    }
}


/// The mechanics is non-trivial here. Sometimes during deletion, it
/// might happen that we need to delete a key whose children are both
/// other pages. In such a case, we might need to rebalance the B-tree
/// by splitting or merging pages. Since most of the mechanics needed
/// to do that is already written in put, we simply return arguments
/// for put in an option, when this happens.
fn delete(txn: & mut MutTxn,
          page: Cow,
          current: u16,
          comp:C,
          path: u64,
          depth: usize)
          -> (Result,Option<Reinsert>) {
    unsafe {
        let ptr = page.offset(current as isize) as *mut u32;
        let cmp = match comp {
            C::KV { key_ptr, key_len, value } => {
                let (key0, value0) = txn.read_key_value(ptr as *const u8);
                match std::slice::from_raw_parts(key_ptr,key_len).cmp(key0) {
                    Ordering::Equal => {
                        Value{txn:txn,value:value}.cmp(Value{txn:txn,value:value0})
                    },
                    c => c
                }
            },
            C::Less => {
                let left0 = u32::from_le(*ptr);
                let left1 = u32::from_le(*(ptr.offset(1)));
                if left0 <= 1 && left1 == 0 {
                    //println!("found smallest: {:?}", current);
                    Ordering::Equal
                } else {
                    Ordering::Less
                }
            }
        };
        match cmp {
            Ordering::Equal => {
                let (page,free) = page.into_mut_page_nonfree(txn);
                let off = node_ptr(&page,depth,path,page.root() as u32);
                let ptr = page.offset(off as isize) as *mut u32;

                let left0 = u32::from_le(*ptr);
                let left1 = u32::from_le(*(ptr.offset(1)));
                let right0 = u32::from_le(*(ptr.offset(2)));
                let right1 = u32::from_le(*(ptr.offset(3)));


                let reins =
                    if comp.is_less() && (left0 <= 1 && left1 == 0) {
                        //let (key0, value0) = read_key_value(&*(ptr as *const u8));
                        Some(Reinsert {
                            page: page.page_offset(), off: off, left:0, right:0
                        })
                    } else {
                        None
                    };
                if left0 <= 1 && left1 == 0 {
                    println!("deleting, left");
                    //println!("{:?}", page);
                    if right0<=1 && right1 == 0 { // no other son (i.e. this is a leaf)
                        (Result::Ok { page: page,
                                      off: 0,
                                      decr: true },
                         reins)
                    } else {
                        // There is exactly one child, we just delete
                        (Result::Ok { page:page,
                                      off: right1 as u16,
                                      decr:true },
                         reins)
                    }
                } else if right0<=1 && right1==0 {
                    // No right child (but one left child)
                    println!("deleting, right");
                    //println!("{:?}", page);
                    (Result::Ok { page: page,
                                  off: left1 as u16,
                                  decr:true },
                     reins)
                        //free_page: if let Some(p) = free { p.page_offset() } else { 0 } }, None)
                } else {
                    // Take the smallest element of the right child.
                    if right0 != 1 {
                        // The right child is a page.
                        let r = u64::from_le(*((ptr as *const u64).offset(1)));
                        let right_page = txn.load_cow_page(r);
                        let right_root = right_page.root();
                        let (result,mut reins) = delete(txn, right_page, right_root, C::Less, 0, 0);
                        let reins = if comp.is_less() {
                            reins
                        } else {
                            println!("reins: {:?}",reins);
                            if let Some(ref mut reins) = reins {
                                let l = u64::from_le(*(ptr as *const u64));
                                reins.left = l;
                                reins.right = r;
                                *(ptr as *mut u64) = 0;
                                *((ptr as *mut u64).offset(1)) = 0;
                            }
                            reins
                        };
                        match result {
                            Result::Ok { page:right_page, off:right_off, .. } => {
                                //println!("{:?}", right_page);
                                if right_off == 0 {
                                    // The right page disappeared.
                                    transaction::free(&mut txn.txn,right_page.page_offset());
                                    *((ptr as *mut u64).offset(1)) = 0;
                                    (Result::Ok { page:page, off:off, decr:false }, reins)
                                } else {
                                    right_page.set_root(right_off);
                                    if *((ptr as *mut u64).offset(1)) != 0 {
                                        *((ptr as *mut u64).offset(1)) = right_page.page_offset().to_le();
                                    }
                                    (Result::Ok { page:page, off:0, decr:false }, reins)
                                }
                            },
                            Result::NotFound => unreachable!()
                        }
                    } else {
                        // The right child is local.
                        let previous_balance = u16::from_le(*((ptr as *mut u16).offset(11)));
                        let (result,reins) = delete(txn, Cow::from_mut_page(page), right1 as u16, C::Less,
                                                    path|(1<<depth), depth+1);
                        if let (Result::Ok { mut page, off:right_off, decr },Some(reins)) = (result,reins) {
                            // reins is the smallest element of the right child.
                            println!("reins: {:?}, decr: {:?}",reins, decr);
                            if comp.is_less() {
                                // If we're currently looking for the smallest descendant, just update the child and forward the result up.
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                if right_off == 0 {
                                    *((ptr as *mut u64).offset(1)) = 0;
                                } else {
                                    *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                                    *((ptr as *mut u32).offset(3)) = (right_off as u32).to_le();
                                }
                                if decr {
                                    println!("balance = {:?}", previous_balance);
                                    if previous_balance == 0 {
                                        (rebalance_right(page, ptr_off, ptr), Some(reins))
                                    } else {
                                        *((ptr as *mut u16).offset(11)) = (previous_balance-1).to_le();
                                        (Result::Ok { page:page, off:ptr_off, decr:previous_balance==2 }, Some(reins))
                                    }
                                } else {
                                    (Result::Ok { page:page, off:ptr_off, decr:false }, Some(reins))
                                }
                            } else {
                                // Else, two cases: either the smallest descendant is on the same page as ptr, or not.
                                // - If it is, set its children to be the current node's children, and return it.
                                // - Else, allocate a new node, set its children to the current node's children, and return it.
                                if reins.page == page.page_offset() {
                                    let p = page.offset(reins.off as isize) as *mut u64;
                                    *p = *(ptr as *const u64);
                                    if right_off>0 {
                                        *((p as *mut u32).offset(2)) = (1 as u32).to_le();
                                        *((p as *mut u32).offset(3)) = (right_off as u32).to_le();
                                    } else {
                                        *(p.offset(1)) = 0
                                    }
                                    println!("balance = {:?}, decr = {:?}", previous_balance,decr);
                                    if decr {
                                        if previous_balance == 0 {
                                            // On a supprimé un truc à droite, rotation.
                                            (rebalance_right(page,reins.off,ptr),None)
                                        } else {
                                            *((p as *mut u16).offset(11)) = (previous_balance-1).to_le();
                                            (Result::Ok { page:page, off:reins.off, decr:previous_balance==2 },None)
                                        }
                                    } else {
                                        (Result::Ok { page:page, off:reins.off, decr:false },None)
                                    }
                                } else {
                                    // Either there's space, or split.
                                    let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                    let ptr = page.offset(ptr_off as isize) as *mut u32;

                                    let ptr_ = txn.load_cow_page(reins.page).offset(reins.off as isize);
                                    let (key,value) = txn.read_key_value(ptr_);
                                    let size = value_record_size(key.len(), value);
                                    debug!("size={:?}", size);
                                    let off_ptr = page.can_alloc(size);
                                    if off_ptr > 0 {
                                        page.alloc_key_value(off_ptr, size, key.as_ptr(), key.len(), value,
                                                             u64::from_le(*(ptr as *const u64)),
                                                             u64::from_le(*((ptr as *const u64).offset(1))));
                                        (Result::Ok { page:page, off:off_ptr, decr:false },None)
                                    } else {
                                        // split
                                        unimplemented!()
                                    }
                                }
                            }
                        } else {
                            unreachable!()
                        }
                    }
                }
            },
            Ordering::Less => {
                let left0 = u32::from_le(*ptr);
                //println!("{:?}", left0);
                if left0 == 1 {
                    let left1 = u32::from_le(*(ptr.offset(1)));
                    if left1 > 0 {
                        let (result,reins) = delete(txn, page, left1 as u16, comp, path, depth + 1);
                        match result {
                            Result::Ok { page,off,decr }=>{
                                //println!("less returned {:?}", off);
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                if off == 0 {
                                    *(ptr as *mut u64) = 0;
                                } else {
                                    *(ptr.offset(0)) = 1;
                                    *(ptr.offset(1)) = (off as u32).to_le();
                                }
                                if decr {
                                    (rebalance_left(page,ptr_off,ptr), reins)
                                } else {
                                    (Result::Ok { page:page, off:ptr_off, decr:false }, reins)
                                }
                            },
                            Result::NotFound => (Result::NotFound, reins),
                        }
                    } else {
                        // not found
                        (Result::NotFound, None)
                    }
                } else {
                    // Page child
                    let left = u64::from_le(*(ptr as *const u64));
                    if left == 0 {
                        (Result::NotFound, None)
                    } else {
                        let left_page = txn.load_cow_page(left);
                        let left_root = left_page.root();
                        let (result,reins) = delete(txn,left_page,left_root,comp,0,0);
                        match result {
                            Result::Ok { page:left_page, off:left_root, decr } => {
                                left_page.set_root(left_root);
                                let page = page.into_mut_page(txn);
                                let off = node_ptr(&page, depth,path,page.root() as u32);
                                let ptr = page.offset(off as isize);
                                *(ptr as *mut u64) = left_page.page_offset().to_le();
                                (Result::Ok { page:page, off: off, decr:decr }, reins)
                            },
                            Result::NotFound => (Result::NotFound, reins)
                        }
                    }
                }
            }
            Ordering::Greater => {
                let right0 = u32::from_le(*(ptr.offset(2)));
                if right0 == 1 {
                    let right1 = u32::from_le(*(ptr.offset(3)));
                    if right1 > 0 {
                        let (result,reins) = delete(txn, page, right1 as u16, comp, path | (1<<depth), depth + 1);
                        /*{
                            let (key0, value0) = read_key_value(&*(ptr as *const u8));
                            println!("ptr = {:?}, right1={:?}, key:{:?}, result = {:?}",current, right1,
                                     std::str::from_utf8(key0).unwrap(),result);
                        }*/
                        match result {
                            Result::Ok { page,off,decr }=>{
                                //println!("greater returned {:?}", off);
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                if off == 0 {
                                    *((ptr as *mut u64).offset(1)) = 0;
                                } else {
                                    *(ptr.offset(2)) = 1;
                                    *(ptr.offset(3)) = (off as u32).to_le();
                                }
                                if decr {
                                    (rebalance_right(page,ptr_off,ptr), reins)
                                } else {
                                    (Result::Ok { page:page,off:ptr_off, decr:false }, reins)
                                }
                            },
                            Result::NotFound => (Result::NotFound,reins)
                        }
                    } else {
                        // not found
                        (Result::NotFound, None)
                    }
                } else {
                    // Page child
                    let right = u64::from_le(*((ptr as *const u64).offset(1)));
                    if right == 0 {
                        (Result::NotFound, None)
                    } else {
                        let right_page = txn.load_cow_page(right);
                        let right_root = right_page.root();
                        let (result, reins) = delete(txn,right_page,right_root,comp,0,0);
                        match result {
                            Result::NotFound => (Result::NotFound, reins),
                            Result::Ok { page:right_page, off:right_root, decr } => {
                                right_page.set_root(right_root);
                                let page = page.into_mut_page(txn);
                                let off = node_ptr(&page, depth, path, page.root() as u32);
                                let ptr = page.offset(off as isize);
                                *((ptr as *mut u64).offset(1)) = right_page.page_offset().to_le();
                                (Result::Ok { page:page, off: off, decr:decr }, reins)
                            },
                        }
                    }
                }
            }
        }
    }
}

unsafe fn rebalance_right(mut page:MutPage, ptr_off:u16, ptr:*mut u32)->Result {
    let current_balance = u16::from_le(*((ptr as *const u16).offset(11)));
    if current_balance == 0 {
        // fetch the balance factor of left child.
        let left_off = u32::from_le(*((ptr as *const u32).offset(1)));
        let left_ptr = page.offset(left_off as isize);
        let left_balance = u16::from_le(*((left_ptr as *const u16).offset(11)));
        println!("left, left_balance=={:?}",left_balance);
        if left_balance == 2 {
            let left_root=tree_rotate_anticlockwise(&mut page,left_off as u16);
            *((ptr as *mut u32).offset(1)) = (left_root as u32).to_le();
            let left_right_ptr = page.offset(left_root as isize);
            let left_right_balance = u16::from_le(*((left_right_ptr as *mut u16).offset(11)));
            if left_right_balance == 0 {
                *((ptr as *mut u16).offset(11)) = (2 as u16).to_le();
                *((left_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
            } else if left_right_balance == 1 {
                *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                *((left_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
            } else {
                *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                *((left_ptr as *mut u16).offset(11)) = (0 as u16).to_le();
            }
            *((left_right_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
        } else if left_balance == 0 {
            *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
            *((left_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
        } else {
            *((ptr as *mut u16).offset(11)) = (0 as u16).to_le();
            *((left_ptr as *mut u16).offset(11)) = (2 as u16).to_le();
        }
        let root=tree_rotate_clockwise(&mut page,ptr_off);
        Result::Ok { page:page, off:root, decr:false }
    } else if current_balance == 2 {
        *((ptr as *mut u16).offset(11)) = (1 as u16).to_le(); // balanced
        Result::Ok { page:page, off:ptr_off, decr:true }
    } else {
        *((ptr as *mut u16).offset(11)) = 0; // leans to the left
        Result::Ok { page:page, off:ptr_off, decr:false }
    }
}

unsafe fn rebalance_left(mut page:MutPage, ptr_off:u16, ptr:*mut u32)->Result {
    let current_balance = u16::from_le(*((ptr as *const u16).offset(11)));
    if current_balance == 2 {
        let right_off = u32::from_le(*((ptr as *const u32).offset(3)));
        let right_ptr = page.offset(right_off as isize);
        let right_balance = u16::from_le(*((right_ptr as *const u16).offset(11)));
        if right_balance == 0 {
            let right_root=tree_rotate_clockwise(&mut page,right_off as u16);
            *((ptr as *mut u32).offset(3)) = (right_root as u32).to_le();
            let right_left_ptr = page.offset(right_root as isize);
            let right_left_balance = u16::from_le(*((right_left_ptr as *mut u16).offset(11)));
            if right_left_balance == 2 {
                *((ptr as *mut u16).offset(11)) = (0 as u16).to_le();
                *((right_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
            } else if right_left_balance == 1 {
                *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                *((right_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
            } else {
                *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                *((right_ptr as *mut u16).offset(11)) = (2 as u16).to_le();
            }
            *((right_left_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
        } else if right_balance == 2 {
            *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
            *((right_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
        } else {
            *((ptr as *mut u16).offset(11)) = (2 as u16).to_le();
            *((right_ptr as *mut u16).offset(11)) = (0 as u16).to_le();
        }
        let root=tree_rotate_anticlockwise(&mut page,ptr_off);
        Result::Ok { page:page, off:root, decr:false }
    } else if current_balance == 0 {
        *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
        Result::Ok { page:page, off:ptr_off, decr:true }
    } else {
        *((ptr as *mut u16).offset(11)) = 2;
        Result::Ok { page:page, off:ptr_off, decr:false }
    }

}
