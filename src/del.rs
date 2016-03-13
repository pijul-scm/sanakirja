use super::transaction;
use super::txn::*;
use super::put;
use std;
use super::rebalance::*;
use std::cmp::Ordering;

#[derive(Debug)]
enum Result {
    Ok {
        page: MutPage,
        off: u16,
    },
    NotFound,
}

#[derive(Debug)]
struct Reinsert {
    page: u64,
    off:u16
}

// Del returns none if no page below its argument was modified.
// If it returns not none, then modify the current page

pub fn del(txn: &mut MutTxn, db: Db, key: &[u8], value: Option<&[u8]>) -> Db {
    let page = txn.load_cow_page(db.root);
    let current = page.root();
    let value = Value::S(value.unwrap());
    let (result,reinsert)=delete(txn, page, current, C::KV { key:key, value:value }, 0, 0);
    let page = match result {
        Result::Ok{mut page,off} => {
            //panic!("Free not implemented");
            if off == 0 {
                // the page was deleted
                unimplemented!()
            } else {
                //let off = rebalance(&mut page,off);
                page.set_root(off);
                page
            }
        },
        Result::NotFound => {
            return db
        }
    };
    /*if let Some(reinsert)=reinsert {
        let db = put::put_lr(txn,Cow::from_mut_page(page),
                             reinsert.key,
                             reinsert.value,
                             reinsert.left,
                             reinsert.right);
        if reinsert.free_page>0 {
            unsafe { transaction::free(&mut txn.txn, reinsert.free_page) }
        }
        db
    } else {
        Db { root:page.page_offset() }
    }*/
    Db { root:page.page_offset() }
}

// The kind of comparison we want
#[derive(Debug,Clone,Copy)]
enum C<'a> {
    KV { key:&'a[u8], value:Value<'a> },
    Eq,
    Less
}
impl<'a> C<'a> {
    fn is_less(&self)->bool {
        match self {
            &C::Less => true,
            _ => false
        }
    }
}
fn delete<'a>(txn: &mut MutTxn,
              page: Cow,
              current: u16,
              comp:C<'a>,
              path: u64,
              depth: usize)
              -> (Result,Option<Reinsert>) {
    unsafe {
        println!("delete: current={:?}, {:?}",current, comp);
        let ptr = page.offset(current as isize) as *mut u32;
        let cmp = match comp {
            C::Eq => Ordering::Equal,
            C::KV { key,value } => {
                let (key0, value0) = read_key_value(&*(ptr as *const u8));
                let value_ = txn.load_value(&value);
                let value_ = value_.contents();
                let value0_ = txn.load_value(&value0);
                let value0_ = value0_.contents();
                println!("delete: current={:?}, key={:?}, value={:?}",current,
                         std::str::from_utf8_unchecked(key0),
                         std::str::from_utf8_unchecked(value0_)
                );
                (key, value_).cmp(&(key0, value0_))
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
                let (mut page,free) = page.into_mut_page_nonfree(txn);
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
                            page: page.page_offset(), off: off
                        })
                    } else {
                        None
                    };
                if left0 <= 1 && left1 == 0 {
                    println!("deleting, left");
                    //println!("{:?}", page);
                    if right0<=1 && right1 == 0 {
                        // Else, delete the current node and update its parent.
                        (Result::Ok { page: page,
                                      off: 0 },
                         reins)
                    } else {
                        (Result::Ok { page:page,
                                      off: right1 as u16 },
                         reins)
                        //free_page: if let Some(p) = free { p.page_offset() } else { 0 } }, None)
                    }
                } else if right0<=1 && right1==0 {
                    println!("deleting, right");
                    //println!("{:?}", page);
                    (Result::Ok { page: page,
                                  off: left1 as u16 },
                     reins)
                        //free_page: if let Some(p) = free { p.page_offset() } else { 0 } }, None)
                } else {
                    if left0>1 && right0>1 {
                        // Both children are pages.  Take the
                        // smallest (largest) descendant of the
                        // right (left) page, and copy it in place
                        // of the current node, with (malloc new)
                        // + (free current).
                        //
                        // Then, recursively delete the page from
                        // which the key was taken, and update the
                        // pointer here:
                        // - If the page was deleted, CoW, write 0, return new current page.
                        // - If it wasn't deleted, update
                        // - If NotFound, unreachable!()
                        //let l = *(ptr as *const u64);
                        let r = u64::from_le(*((ptr as *const u64).offset(1)));
                        let right_page = txn.load_cow_page(r);
                        let right_root = right_page.root();
                        let (result,mut reins) = delete(txn, right_page, right_root, C::Less, 0, 0);
                        let reins = if comp.is_less() {
                            reins
                        } else {
                            unimplemented!()
                        };
                        match result {
                            Result::Ok { page:right_page, off:right_off } => {
                                //println!("{:?}", right_page);
                                right_page.set_root(right_off);
                                *((ptr as *mut u64).offset(1)) = right_page.page_offset().to_le();
                                (Result::Ok { page:page, off:0 }, reins)
                            },
                            Result::NotFound => unreachable!()
                        }
                    } else {
                        // Both children are taken, at least one is not a page.
                        // Take the smallest element of the right child.
                        let (result,reins) = delete(txn, Cow::from_mut_page(page), right1 as u16, C::Less, path|(1<<depth), depth+1);
                        if let (Result::Ok { page:page, off:right_off },Some(reins)) = (result,reins) {
                            // reins is the smallest element of the right child.
                            println!("reins: {:?}",reins);
                            if comp.is_less() {
                                // If we're currently looking for the smallest descendant, just forward up.
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                if right_off == 0 {
                                    *((ptr as *mut u64).offset(1)) = 0;
                                } else {
                                    *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                                    *((ptr as *mut u32).offset(3)) = (right_off as u32).to_le();
                                }
                                (Result::Ok { page:page, off:ptr_off }, Some(reins))
                            } else {
                                // Else, two cases: either the smallest descendant is on the same page as ptr, or not.
                                // - If it is, set its children to be the current node's children, and return it.
                                // - Else, allocate a new node, set its children to the current node's children, and return it.
                                (if reins.page == page.page_offset() {
                                    let p = page.offset(reins.off as isize) as *mut u64;
                                    *p = *(ptr as *const u64);
                                    if right_off>0 {
                                        *((p as *mut u32).offset(2)) = (1 as u32).to_le();
                                        *((p as *mut u32).offset(3)) = (right_off as u32).to_le();
                                    } else {
                                        *(p.offset(1)) = 0
                                    }
                                    Result::Ok { page:page, off:reins.off }
                                } else {
                                    unimplemented!()
                                }, None)
                            }
                        } else {
                            unreachable!()
                        }
                        /*
                        match result {
                                println!("{:?} {:?}", page, right_off);
                                //right_page.set_root(right_off);
                            },
                            Result::NotFound => unreachable!()
                        }
                         */
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
                            Result::Ok { mut page,off }=>{
                                //println!("less returned {:?}", off);
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                //*((ptr as *mut u16).offset(11)) = (u16::from_le(*((ptr as *mut u16).offset(11))) - 1).to_le();
                                if off == 0 {
                                    *(ptr as *mut u64) = 0;
                                } else {
                                    *(ptr.offset(0)) = 1;
                                    *(ptr.offset(1)) = (off as u32).to_le();
                                }
                                //let ptr_off = rebalance(&mut page,ptr_off);
                                (Result::Ok { page:page,off:ptr_off }, reins)
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
                            Result::Ok { page:left_page, off:left_root } => {
                                let mut page = page.into_mut_page(txn);
                                let off = node_ptr(&page, depth,path,page.root() as u32);
                                let ptr = page.offset(off as isize);
                                *(ptr as *mut u64) = left_page.page_offset().to_le();
                                (Result::Ok { page:page, off: off }, reins)
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
                        match result {
                            Result::Ok { mut page,off }=>{
                                //println!("greater returned {:?}", off);
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                //*((ptr as *mut u16).offset(11)) = (u16::from_le(*((ptr as *mut u16).offset(11))) - 1).to_le();
                                if off == 0 {
                                    *((ptr as *mut u64).offset(1)) = 0;
                                } else {
                                    *(ptr.offset(2)) = 1;
                                    *(ptr.offset(3)) = (off as u32).to_le();
                                }
                                //let ptr_off = rebalance(&mut page,ptr_off);
                                (Result::Ok { page:page,off:ptr_off }, reins)
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
                            Result::Ok { page:right_page, off:right_root } => {
                                let mut page = page.into_mut_page(txn);
                                let off = node_ptr(&page, depth, path, page.root() as u32);
                                let ptr = page.offset(off as isize);
                                *((ptr as *mut u64).offset(1)) = right_page.page_offset().to_le();
                                (Result::Ok { page:page, off: off }, reins)
                            },
                        }
                    }
                }
            }
        }
    }
}
