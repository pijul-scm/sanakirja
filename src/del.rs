use super::txn::*;
use std;
use super::rebalance::*;
use std::cmp::Ordering;


// Del returns none if no page below its argument was modified.
// If it returns not none, then modify the current page

pub fn del(txn: &mut MutTxn, db: Db, key: &[u8], value: Option<&[u8]>) -> Db {
    let page = txn.load_cow_page(db.root);
    let current = page.root();
    let value = value.unwrap();
    match delete(txn, page, current, key, value, 0, 0, false) {
        Delete::NewPage{mut page,off} => {
            if off == 0 {
                // the page was deleted
                unimplemented!()
            } else {
                let off = rebalance(&mut page,off);
                page.set_root(off);
                Db { root:page.page_offset() }
            }
        },
        Delete::NotFound => {
            db
        }
    }
}

enum Delete {
    NotFound,
    NewPage { page:MutPage, off: u16 },
}


fn delete(txn: &mut MutTxn,
          page: Cow,
          current: u16,
          key: &[u8],
          value: &[u8],
          path: u64,
           depth: usize,
          eq:bool)
          -> Delete {
    unsafe {
        println!("delete: current={:?}",current);
        let ptr = page.offset(current as isize) as *mut u32;
        let (key0, value0) = read_key_value(&*(ptr as *const u8));

        let value0 = txn.load_value(&value0);
        let value0 = value0.contents();

        println!("delete: current={:?}, key={:?}, value={:?}",current,
                 std::str::from_utf8_unchecked(key0),
                 std::str::from_utf8_unchecked(value0)
        );
        let cmp = if eq { Ordering::Equal } else { (key, value).cmp(&(key0, value0)) };
        match cmp {
            Ordering::Equal => {
                // Find the smallest children in the right subtree, or the largest one in the left subtree

                // De toute façon, il va se passer un truc, on CoW maintenant.
                let mut page = page.into_mut_page(txn);
                let (off,ptr) = if eq {
                    (current,ptr)
                } else {
                    let off = node_ptr(&page,depth,path,page.root() as u32);
                    let ptr = page.offset(off as isize) as *mut u32;
                    (off,ptr)
                };

                let left0 = u32::from_le(*ptr);
                let left1 = u32::from_le(*(ptr.offset(1)));
                let right0 = u32::from_le(*(ptr.offset(2)));
                let right1 = u32::from_le(*(ptr.offset(3)));
                if left0 <= 1 && left1 == 0 {
                    println!("deleting, left");
                    if right0<=1 && right1 == 0 {
                        // Else, delete the current page and update its parent.
                        Delete::NewPage { page: page,
                                          off: 0 }
                    } else {
                        Delete::NewPage { page:page,
                                          off: right1 as u16 }
                    }
                } else if right0<=1 && right1==0 {
                    println!("deleting, right");
                    Delete::NewPage { page: page,
                                      off: left1 as u16 }
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
                        unimplemented!()
                    } else {
                        // Both children are taken, at least one is not a page. Rotate.
                        println!("rotating");
                        let (result,root) =
                            if right0==1 {
                                let root = tree_rotate_anticlockwise(&mut page, off);
                                (delete(txn,
                                        Cow::from_mut_page(page),
                                        off,
                                        key,
                                        value,
                                        (path << 1),
                                        depth + 1,
                                        true),root)
                            } else {
                                let root = tree_rotate_clockwise(&mut page, off);
                                (delete(txn,
                                        Cow::from_mut_page(page),
                                        off,
                                        key,
                                        value,
                                        (path << 1)|1,
                                        depth + 1,
                                        true), root)
                            };
                        match result {
                            Delete::NewPage { mut page, off } => {
                                println!("rotated, off={:?}",off);
                                let ptr = page.offset(root as isize) as *mut u32;
                                *((ptr as *mut u16).offset(11)) = (u16::from_le(*((ptr as *mut u16).offset(11))) - 1).to_le();
                                if right0==1 {
                                    if off==0 {
                                        *(ptr as *mut u64) = 0;
                                    } else {
                                        *(ptr.offset(0)) = 1;
                                        *(ptr.offset(1)) = (off as u32).to_le();
                                    }
                                } else {
                                    if off==0 {
                                        *((ptr as *mut u64).offset(1)) = 0;
                                    } else {
                                        *(ptr.offset(2)) = 1;
                                        *(ptr.offset(3)) = (off as u32).to_le();
                                    }
                                }
                                let root = rebalance(&mut page,root);
                                Delete::NewPage { page: page, off: root }
                            },
                            Delete::NotFound => unreachable!()
                        }
                    }
                }
            },
            Ordering::Less => {
                let left0 = u32::from_le(*ptr);
                if left0 == 1 {
                    let left1 = u32::from_le(*(ptr.offset(1)));
                    if left1 > 0 {
                        match delete(txn, page, left1 as u16, key, value, path, depth + 1, false) {
                            Delete::NewPage { mut page,off }=>{
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                *((ptr as *mut u16).offset(11)) = (u16::from_le(*((ptr as *mut u16).offset(11))) - 1).to_le();
                                if off == 0 {
                                    *(ptr as *mut u64) = 0;
                                } else {
                                    *(ptr.offset(0)) = 1;
                                    *(ptr.offset(1)) = (off as u32).to_le();
                                }
                                let ptr_off = rebalance(&mut page,ptr_off);
                                Delete::NewPage { page:page,off:ptr_off }
                            },
                            Delete::NotFound => Delete::NotFound,
                        }
                    } else {
                        // not found
                        Delete::NotFound
                    }
                } else {
                    // Page child
                    let left = u64::from_le(*(ptr as *const u64));
                    let left_page = txn.load_cow_page(left);
                    let left_root = left_page.root();
                    match delete(txn,left_page,left_root,key,value,0,0,false) {
                        Delete::NotFound => Delete::NotFound,
                        Delete::NewPage { page:left_page, off:left_root } => {
                            let mut page = page.into_mut_page(txn);
                            let off = node_ptr(&page, depth,path,page.root() as u32);
                            let ptr = page.offset(off as isize);
                            *(ptr as *mut u64) = left_page.page_offset().to_le();
                            Delete::NewPage { page:page, off: off }
                        }
                    }
                }
            }
            Ordering::Greater => {
                let right0 = u32::from_le(*(ptr.offset(2)));
                if right0 == 1 {
                    let right1 = u32::from_le(*(ptr.offset(3)));
                    if right1 > 0 {
                        match delete(txn, page, right1 as u16, key, value, (path << 1) | 1, depth + 1, false) {
                            Delete::NewPage { mut page,off }=>{
                                let ptr_off = node_ptr(&page,depth,path,page.root() as u32);
                                let ptr = page.offset(ptr_off as isize) as *mut u32;
                                *((ptr as *mut u16).offset(11)) = (u16::from_le(*((ptr as *mut u16).offset(11))) - 1).to_le();
                                if off == 0 {
                                    *((ptr as *mut u64).offset(1)) = 0;
                                } else {
                                    *(ptr.offset(2)) = 1;
                                    *(ptr.offset(3)) = (off as u32).to_le();
                                }
                                let ptr_off = rebalance(&mut page,ptr_off);
                                Delete::NewPage { page:page,off:ptr_off }
                            },
                            Delete::NotFound => Delete::NotFound,
                        }
                    } else {
                        // not found
                        Delete::NotFound
                    }
                } else {
                    // Page child
                    let right = u64::from_le(*((ptr as *const u64).offset(1)));
                    let right_page = txn.load_cow_page(right);
                    let right_root = right_page.root();
                    match delete(txn,right_page,right_root,key,value,0,0,false) {
                        Delete::NotFound => Delete::NotFound,
                        Delete::NewPage { page:right_page, off:right_root } => {
                            let mut page = page.into_mut_page(txn);
                            let off = node_ptr(&page, depth,path,page.root() as u32);
                            let ptr = page.offset(off as isize);
                            *((ptr as *mut u64).offset(1)) = right_page.page_offset().to_le();
                            Delete::NewPage { page:page, off: off }
                        }
                    }
                }
            }
        }
    }
}

// Removes and returns the maximal or minimal child from a tree, returning the updated page and the key/value
// fn take_maximal_child(txn: &mut MutTxn,
// page: Cow,
// current: u32,
// direction: bool) {
// unsafe {
// let ptr = page.offset(current as isize) as *mut u32;
// if direction {
// go right
// let right0 = u32::from_le(*(ptr.offset(2)));
// if right0==1 {
// let right1 = u32::from_le(*(ptr.offset(3)));
// if right1 == 0 {
// See explanation for left node.
// unimplemented!()
// } else {
// maximal_child(txn,page,right1,direction)
// }
// } else {
// unimplemented!()
// }
// } else {
// go left
// let left0 = u32::from_le(*ptr);
// if left0==1 {
// let left1 = u32::from_le(*(ptr.offset(1)));
// if left1 == 0 {
// No left child, this is the smallest node.
// However, it might have a right child.
// Return this node, and update its parent to its right node.
// unimplemented!()
// } else {
// maximal_child(txn,page,left1,direction)
// }
// } else {
// unimplemented!()
// }
// }
// }
// }
//
