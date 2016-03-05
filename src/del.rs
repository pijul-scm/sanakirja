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
    match delete(txn, page, current as u32, key, value, 0, 0) {
        Delete::NewPage{page} => {
            Db { root:page }
        },
        Delete::NotFound => {
            db
        },
        Delete::PageDeleted => {
            // The database has become empty
            unimplemented!()
        }
    }
}

enum Delete {
    NotFound,
    NewPage { page:u64 },
    PageDeleted
}


fn delete(txn: &mut MutTxn,
          page: Cow,
          current: u32,
          key: &[u8],
          value: &[u8],
          mut path: u64,
          mut depth: usize)
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
        let cmp = (key, value).cmp(&(key0, value0));
        match cmp {
            Ordering::Equal => {
                // Find the smallest children in the right subtree, or the largest one in the left subtree

                // De toute façon, il va se passer un truc, on CoW maintenant.
                let mut page = page.into_mut_page(txn);
                let off = node_ptr(&page,depth,path,page.root() as u32);
                let ptr = page.offset(off as isize) as *mut u32;
                let mut parent_ptr = if depth>0 {
                    let off = node_ptr(&page,depth-1,path>>1,page.root() as u32);
                    let ptr = page.offset(off as isize);
                    if path&1 == 0 {
                        ptr as *mut u64
                    } else {
                        (ptr as *mut u64).offset(1)
                    }
                } else {
                    std::ptr::null_mut()
                };
                // This loop either deletes and returns, or rotates
                // and repeats until the current node has at least one
                // (page or null) child.
                loop {
                    let left0 = u32::from_le(*ptr);
                    let left1 = u32::from_le(*(ptr.offset(1)));
                    let right0 = u32::from_le(*(ptr.offset(2)));
                    let right1 = u32::from_le(*(ptr.offset(3)));
                    if (left0 <= 1 && left1 == 0) || (right0<=1 && right1==0) {
                        // If the current node has at least one non-local child, we can delete it and return.
                        let (child,child0) = if left0<=1 && left1==0 {
                            (*((ptr as *const u64).offset(1)), right0)
                        } else {
                            (*(ptr as *const u64), left0)
                        };
                        if parent_ptr.is_null() {
                            // If the current node is the root, change the page's root.

                            // Either there is a local node to replace the root, or not
                            if child0==1 || child==0 {
                                // If ther is a local node
                                if left0<=1 && left1==0 {
                                    page.set_root(node_ptr(&page,depth+1,(path<<1)|1,page.root() as u32));
                                } else {
                                    page.set_root(node_ptr(&page,depth+1,(path<<1),page.root() as u32));
                                }
                                return Delete::NewPage { page:page.page_offset() }
                            } else {
                                // Else, delete the current page and update its parent.
                                unimplemented!();
                                return Delete::PageDeleted
                            }
                        } else {
                            // Else (i.e. if the current node is not the root), 
                            if child0!=1 {
                                *parent_ptr = child
                            } else {
                                *(parent_ptr as *mut u32) = (1 as u32).to_le();
                                if left0<=1 && left1==0 {
                                    *((parent_ptr as *mut u32).offset(1)) = (node_ptr(&page,depth+1,(path<<1)|1,page.root() as u32) as u32).to_le()
                                } else {
                                    *((parent_ptr as *mut u32).offset(1)) = (node_ptr(&page,depth+1,(path<<1),page.root() as u32) as u32).to_le()
                                }
                            };
                            return Delete::NewPage { page:page.page_offset() }
                        }
                    } else {
                        // Both children are taken. Rotate.
                        if left0!=1 && right0!=1 {
                            // Both children are pages.  Take the
                            // smallest (largest) descendant of the
                            // right (left) page, and copy it in place
                            // of the current node, with (malloc new)
                            // + (free current).
                            //
                            // Then, recursively delete the page from
                            // which the key was taken, and update the
                            // pointer here.
                            unimplemented!()
                        } else {
                            println!("rotating");
                            if parent_ptr.is_null() {
                                let root=tree_rotate_anticlockwise(&mut page, off);
                                parent_ptr = page.offset(root as isize) as *mut u64;
                                page.set_root(root);
                            } else {
                                *(parent_ptr as *mut u32) = (1 as u32).to_le();
                                let p=tree_rotate_anticlockwise(&mut page, off);
                                *((parent_ptr as *mut u32).offset(1)) = (p as u32).to_le();
                                parent_ptr = page.offset(p as isize) as *mut u64;
                            }
                            depth+=1;
                            path = path<<1;
                        }
                    }
                }
            }
            Ordering::Less => {
                let left0 = u32::from_le(*ptr);
                if left0 == 1 {
                    let left1 = u32::from_le(*(ptr.offset(1)));
                    if left1 > 0 {
                        delete(txn, page, left1, key, value, path, depth + 1)
                    } else {
                        // not found
                        Delete::NotFound
                    }
                } else {
                    // Page child
                    unimplemented!()
                }
            }
            Ordering::Greater => {
                let right0 = u32::from_le(*(ptr.offset(2)));
                if right0 == 1 {
                    let right1 = u32::from_le(*(ptr.offset(3)));
                    if right1 > 0 {
                        delete(txn,
                               page,
                               right1,
                               key,
                               value,
                               (path << 1) | 1,
                               depth + 1)
                    } else {
                        // not found
                        Delete::NotFound
                    }
                } else {
                    // Page child
                    unimplemented!()
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
