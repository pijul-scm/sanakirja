use super::txn::*;
use std;
use super::rebalance::*;
use std::cmp::Ordering;
use super::transaction;

#[derive(Debug)]
pub enum Result<'a> {
    Ok {
        page: MutPage,
        off: u16,
        incr: bool
    },
    Split {
        key: &'a [u8],
        value: Value<'a>,
        left: MutPage,
        right: MutPage,
        free_page: u64,
    },
}


// Finds binary tree root and calls binary_tree_insert on it.
pub fn insert<'a>(txn: &mut MutTxn,
              page: Cow,
              key: &[u8],
              value: Value<'a>,
              l: u64,
              r: u64,
              max_rc: u64)
              -> Result<'a> {
    // [u8], Value<'a>, u64, u64, u64)> {
    let root = page.root();
    debug!("insert: root={:?}, {:?},{:?}", root, key, value);
    if root == 0 {
        let mut page = page.into_mut_page(txn);
        let size = value_record_size(key, value);
        let off = page.can_alloc(size);
        debug_assert!(off > 0);

        let value = alloc_value(txn,value);
        page.alloc_key_value(off, size, key, value, l, r);
        debug!("inserted {}", off);
        page.set_root(off);
        debug!("root set 0");
        Result::Ok {
            page: page,
            off: off,
            incr: false
        }
    } else {
        let rc = std::cmp::max(page.rc(), max_rc);
        let result = binary_tree_insert(txn, page, key, value, l, r, rc, 0, 0, root as u32);
        debug!("result {:?}", result);
        match result {
            Result::Ok{page,off,incr} => {
                page.set_root(off as u16);
                // unsafe {
                // let ptr=page.offset(root);
                // incr(ptr.offset(6));
                // }
                debug!("root set");
                Result::Ok {
                    page: page,
                    off: off,
                    incr: incr
                }
            }
            result => result,
        }
    }
}


// Returns None if the changes have been done in one of the children of "page", Some(Result::Ok(..)) if "page" is a B-leaf or a B-node and we inserted something in it, and Some(Result::Split(...)) if page was split.
fn binary_tree_insert<'a>(txn: &mut MutTxn,
                          page: Cow,
                          key: &[u8],
                          value: Value<'a>,
                          l: u64,
                          r: u64,
                          max_rc: u64,
                          depth: usize,
                          path: u64,
                          current: u32)
                          -> Result<'a> {
    unsafe {
        debug!("binary tree insert:{} {}", depth, path);
        let ptr = page.offset(current as isize) as *mut u32;
        unsafe fn has_local_son(ptr:*const u32)->bool {
            let a = u32::from_le(*((ptr as *mut u32)));
            let b = u32::from_le(*((ptr as *mut u32).offset(1)));
            a == 1 && b>0
        }

        // Inlining this (former) closure takes the whole thing from 2.33 to 1.7 (ratio (sanakirja put time)/(lmdb put time)).
        unsafe fn continue_local<'a>(txn:&mut MutTxn, page:Cow, key:&[u8], value:Value<'a>, l:u64, r:u64, max_rc:u64,  depth:usize, path:u64, left:bool, ptr:*mut u32) -> Result<'a> {
            let next = u32::from_le(*(ptr.offset(if left { 1 } else { 3 })));
            if next == 0 {
                // free branch.
                let size = value_record_size(key, value);
                debug!("size={:?}", size);
                let off_ptr = page.can_alloc(size);
                if off_ptr > 0 {
                    let mut page = page.into_mut_page(txn);
                    let value = alloc_value(txn,value);
                    debug!("continue_local, value={:?}", value);
                    page.alloc_key_value(off_ptr, size, key, value, l, r);
                    let current = node_ptr(&page, depth, path, page.root() as u32);
                    let ptr = page.offset(current as isize);

                    // Decide whether this insertion increments the height
                    let has_brother = if left {
                        *((ptr as *mut u32)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(1)) = (off_ptr as u32).to_le();
                        has_local_son(ptr.offset(2) as *mut u32)
                    } else {
                        *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(3)) = (off_ptr as u32).to_le();
                        has_local_son(ptr as *mut u32)
                    };
                    debug!("has_brother: {:?}",has_brother);
                    if !has_brother {
                        // This increases the height
                        if left {
                            // left lean
                            *((ptr as *mut u16).offset(11)) = 0;
                        } else {
                            // right lean
                            *((ptr as *mut u16).offset(11)) = (2 as u16).to_le();
                        }
                    }
                    Result::Ok {
                        off: current,
                        page: page,
                        incr: !has_brother
                    }
                } else {
                    // No more space in this page
                    split_and_insert(txn, &page, key, value, l, r, 0)
                }
            } else {
                let result = binary_tree_insert(txn,
                                                page,
                                                key,
                                                value,
                                                l,
                                                r,
                                                max_rc,
                                                depth + 1,
                                                if left { path } else { path | (1<<depth) },
                                                next);
                if let Result::Ok{off,mut page,incr} = result {
                    let current = node_ptr(&page, depth, path, page.root() as u32);
                    let ptr = page.offset(current as isize);
                    if left {
                        *((ptr as *mut u32).offset(0)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(1)) = (off as u32).to_le();
                    } else {
                        *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(3)) = (off as u32).to_le();
                    }

                    // If the balance is <0 or >2, do something, and return incr to tell whether the height has really increased.
                    let current_balance = u16::from_le(*((ptr as *const u16).offset(11)));
                    let (off,incr) = if incr {
                        if current_balance == 1 { // balanced
                            if left {
                                *((ptr as *mut u16).offset(11)) = 0
                            } else {
                                *((ptr as *mut u16).offset(11)) = 2
                            }
                            (current,true)
                        } else {
                            if left {
                                debug!("left, current_balance=={:?}",current_balance);
                                if current_balance == 0 {
                                    // fetch the balance factor of left child.
                                    let left_ptr = page.offset(off as isize);
                                    let left_balance = u16::from_le(*((left_ptr as *const u16).offset(11)));
                                    debug!("left, left_balance=={:?}",left_balance);
                                    if left_balance == 2 {
                                        let left_root=tree_rotate_anticlockwise(&mut page,off);
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
                                    } else {
                                        // left_balance == 0
                                        *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                                        *((left_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                                    }
                                    let root=tree_rotate_clockwise(&mut page,current);
                                    (root, false)
                                } else if current_balance == 2 {
                                    *((ptr as *mut u16).offset(11)) = (1 as u16).to_le(); // balanced
                                    (current,false)
                                } else {
                                    *((ptr as *mut u16).offset(11)) = 0; // leans to the left
                                    (current,true)
                                }
                            } else {
                                debug!("right, current_balance=={:?}",current_balance);
                                if current_balance == 2 {
                                    // fetch the balance factor of left child.
                                    let right_ptr = page.offset(off as isize);
                                    let right_balance = u16::from_le(*((right_ptr as *const u16).offset(11)));
                                    if right_balance == 0 {
                                        let right_root=tree_rotate_clockwise(&mut page,off);
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
                                    } else {
                                        // right_balance == 2
                                        *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                                        *((right_ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                                    }
                                    let root=tree_rotate_anticlockwise(&mut page,current);
                                    {
                                        let ptr = page.offset(root as isize);
                                        *((ptr as *mut u16).offset(11)) = (1 as u16).to_le();
                                    }
                                    (root, false)
                                } else if current_balance == 0 {
                                    *((ptr as *mut u16).offset(11)) = (1 as u16).to_le(); // balanced
                                    (current,false)
                                } else {
                                    *((ptr as *mut u16).offset(11)) = (2 as u16).to_le(); // leans to the right
                                    (current,true)
                                }
                            }
                        }
                    } else {
                        (current,false)
                    };
                    Result::Ok {
                        off: off,
                        page: page,
                        incr: incr
                    }
                } else {
                    result
                }
            }
        }

        let continue_global = |txn: &mut MutTxn, page: Cow, left:bool| {
            debug!("continue_global");
            // Global offset
            let child_ptr = if left {
                ptr as *const u64
            } else {
                (ptr as *const u64).offset(1)
            };
            let child = u64::from_le(*child_ptr);
            if child == 0 {
                // free branch.
                let size = value_record_size(key, value);
                debug!("size={:?}", size);
                let off_ptr = page.can_alloc(size);
                if off_ptr > 0 {
                    let mut page = page.into_mut_page(txn);
                    let value = alloc_value(txn,value);
                    debug!("continue_local, value={:?}", value);
                    page.alloc_key_value(off_ptr, size, key, value, l, r);
                    //let current = node_ptr(&page, depth, path, page.root() as u32);
                    //let ptr = page.offset(current as isize);

                    // Decide whether this insertion increments the height
                    let has_brother = if left {
                        *((ptr as *mut u32)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(1)) = (off_ptr as u32).to_le();
                        has_local_son(ptr.offset(2) as *mut u32)
                    } else {
                        *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                        *((ptr as *mut u32).offset(3)) = (off_ptr as u32).to_le();
                        has_local_son(ptr as *mut u32)
                    };
                    debug!("has_brother: {:?}",has_brother);
                    if !has_brother {
                        // This increases the height
                        if left {
                            // left lean
                            *((ptr as *mut u16).offset(11)) = 0;
                        } else {
                            // right lean
                            *((ptr as *mut u16).offset(11)) = (2 as u16).to_le();
                        }
                    } else {
                        *((ptr as *mut u16).offset(11)) = 1;
                    }
                    Result::Ok {
                        off: current as u16,
                        page: page,
                        incr: !has_brother
                    }
                } else {
                    // No more space in this page
                    split_and_insert(txn, &page, key, value, l, r, 0)
                }
            } else {
                let page_ = txn.load_cow_page(child);
                let max_rc = std::cmp::max(max_rc, page_.rc());
                let result = insert(txn, page_, key, value, l, r, max_rc);
                match result {
                    Result::Split { key:k0,value:v0,left:l0,right:r0,free_page:fr0 } => {
                        let size = value_record_size(k0, v0);
                        let off = page.can_alloc(size);
                        if off > 0 {
                            let mut page = page.into_mut_page(txn);
                            // page_ split, we need to insert the resulting key here.
                            page.alloc_key_value(off, size, k0, v0, l0.page_offset(), r0.page_offset());
                            // Either there's room
                            let current = node_ptr(&page, depth, path, page.root() as u32);
                            let ptr = page.offset(current as isize);
                            // Either there's room for it.
                            let has_brother = if left {
                                *((ptr as *mut u32).offset(0)) = (1 as u32).to_le();
                                *((ptr as *mut u32).offset(1)) = (off as u32).to_le();
                                let has = has_local_son((ptr as *const u32).offset(2));
                                if !has {
                                    *((ptr as *mut u16).offset(11)) = (0 as u16).to_le();
                                }
                                has
                            } else {
                                *((ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                                *((ptr as *mut u32).offset(3)) = (off as u32).to_le();
                                let has = has_local_son(ptr as *const u32);
                                if !has {
                                    *((ptr as *mut u16).offset(11)) = (2 as u16).to_le();
                                }
                                has
                            };
                            //incr((ptr as *mut u16).offset(11));
                            transaction::free(&mut txn.txn, fr0);
                            Result::Ok {
                                page: page,
                                off: current,
                                incr: !has_brother
                            }
                        } else {
                            // debug!("Could not find space for child pages {} {}",l0,r0);
                            // page_ was split and there is no space here to keep track of its replacement.
                            split_and_insert(txn, &page, k0, v0, l0.page_offset(), r0.page_offset(), fr0)
                        }
                    },
                    Result::Ok { page:next_page,.. } => {
                        let page = page.into_mut_page(txn);
                        let current = node_ptr(&page, depth, path, page.root() as u32);
                        let ptr = page.offset(current as isize);
                        // Either there's room for it.
                        if left {
                            *((ptr as *mut u64)) = next_page.page_offset().to_le();
                        } else {
                            *((ptr as *mut u64).offset(1)) = next_page.page_offset().to_le();
                        }
                        Result::Ok { page:page, off:current, incr:false }
                    },
                }
            }
        };


        // let count = u32::from_le(*(ptr.offset(6)));
        let cmp = {
            let (key0, value0) = read_key_value(&*(ptr as *const u8));
            let cmp = key.cmp(&key0);
            if cmp == Ordering::Equal {
                let value = txn.load_value(&value);
                let value = value.contents();
                let value0 = txn.load_value(&value0);
                let value0 = value0.contents();
                value.cmp(value0)
            } else {
                cmp
            }
        };
        match cmp {
            Ordering::Less | Ordering::Equal => {
                let left0 = u32::from_le(*(ptr as *const u32));
                debug!("left0={:?}", left0);
                if left0 == 1 {
                    // continue_local(self, page,ptr,1,path,key,value,l,r,depth,path)
                    //(txn:&mut MutTxn, page:Cow, key:&[u8], value:Value<'a>, l:u64, r:u64, max_rc:u64,  depth:usize, path:u64, current:u32, left:bool, ptr:*mut u32) -> Result<'a> {
                    continue_local(txn, page, key, value, l, r, max_rc, depth, path, true, ptr)
                } else {
                    continue_global(txn, page, true)
                }
            }
            _ => {
                let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
                debug!("right0={:?}", right0);
                if right0 == 1 {
                    continue_local(txn, page, key, value, l, r, max_rc, depth, path, false, ptr)
                } else {
                    continue_global(txn, page, false)
                }
            }
        }
    }
}

// Split the page given in argument, inserting k and v into one of the children, returning Result::Split with the correct parameters. fr is the page on which k and v live.
fn split_and_insert<'a>(txn: &mut MutTxn,
                        page: &Cow,
                        k: &[u8],
                        v: Value<'a>,
                        l: u64,
                        r: u64,
                        fr: u64)
                        -> Result<'a> {
    unsafe {
        debug!("split_and_insert: {:?},{:?},{:?}",
               std::str::from_utf8_unchecked(k),
               l,
               r)
    };
    match split(txn,page) {
        Result::Split { key,value,left,right,free_page } => {
            unsafe {
                debug!("split_and_insert, reinserting: {:?},{:?},{:?}",
                       std::str::from_utf8_unchecked(k),
                       l,
                       r);
            }
            let cmp = k.cmp(key);
            let cmp = match cmp {
                Ordering::Less | Ordering::Greater => cmp,
                Ordering::Equal => {
                    let v = txn.load_value(&v);
                    let value = txn.load_value(&value);
                    v.contents().cmp(&value.contents())
                }
            };
            match cmp {
                Ordering::Less | Ordering::Equal => {
                    let root = left.root();
                    let left_page = Cow::from_mut_page(left);
                    let result = binary_tree_insert(txn, left_page, k, v, l, r, 1, 0, 0, root as u32);
                    if fr > 0 { unsafe { transaction::free(&mut txn.txn, fr) } }
                    if let Result::Ok{page,off,incr} = result {
                        page.set_root(off as u16);
                        Result::Split { key:key,value:value,left:page,right:right,free_page:free_page }
                    } else {
                        panic!("problem left: {:?}", result)
                    }
                }
                _ => {
                    let root = right.root();
                    let right_page = Cow::from_mut_page(right);
                    let result = binary_tree_insert(txn, right_page, k, v, l, r, 1, 0, 0, root as u32);
                    if fr > 0 { unsafe { transaction::free(&mut txn.txn, fr) } }
                    if let Result::Ok{page,off,incr} = result {
                        page.set_root(off as u16);
                        Result::Split { key:key,value:value,left:left,right:page,free_page:free_page }
                    } else {
                        panic!("problem right: {:?}", result)
                    }
                }
            }
        },
        Result::Ok{..} => unreachable!(),
    }
}

// Split the page given in argument, inserting k and v into one of the children, returning Result::Split with the correct parameters. fr is the page on which k and v live.
pub fn split<'a>(txn: &mut MutTxn, page: &Cow) -> Result<'a> {
    // fr is the page where k and v live, if they're not from a lifetime larger than self.

    // page.page.free(&mut self.txn);
    // self.debug("/tmp/before_split", 0);
    // println!("split {:?}",page);

    debug!("\n\nsplit page {:?} !\n", page);
    // tree traversal
    fn iter(txn: &mut MutTxn, page: &Cow, dest: &mut MutPage, current: u32) -> u16 {
        unsafe {
            let ptr = page.offset(current as isize);
            let (key, value) = read_key_value(&*(ptr as *const u8));
            // set with lr=00 for now, will update immediately after.
            let size = value_record_size(key, value);
            let dest_off = dest.can_alloc(size);
            debug_assert!(dest_off > 0);
            dest.alloc_key_value(dest_off, size, key, value, 0, 0);
            let dest_ptr: *mut u32 = dest.offset(dest_off as isize) as *mut u32;

            let left0 = u32::from_le(*(ptr as *const u32));
            if left0 == 1 {
                // local offset, follow
                let left = u32::from_le(*((ptr as *const u32).offset(1)));
                *(dest_ptr as *mut u32) = (1 as u32).to_le();
                let left = iter(txn, page, dest, left);
                *((dest_ptr as *mut u32).offset(1)) = (left as u32).to_le();
            } else {
                // global offset, copy
                let child = u64::from_le(*((ptr as *const u64).offset(1)));
                *((dest_ptr as *mut u64).offset(1)) = if child != page.page_offset() {
                    *(ptr as *const u64)
                } else {
                    0
                }
            }
            let right0 = u32::from_le(*((ptr as *const u32).offset(2)));
            if right0 == 1 {
                // local offset, follow
                let right = u32::from_le(*((ptr as *const u32).offset(3)));
                *((dest_ptr as *mut u32).offset(2)) = (1 as u32).to_le();
                let right = iter(txn, page, dest, right);
                *((dest_ptr as *mut u32).offset(3)) = (right as u32).to_le();
            } else {
                // global offset, copy it
                let child = u64::from_le(*((ptr as *const u64).offset(1)));
                *((dest_ptr as *mut u64).offset(1)) = if child != page.page_offset() {
                    *((ptr as *const u64).offset(1))
                } else {
                    0
                }
            }
            // make counts equal
            let dest_ptr = dest_ptr as *mut u16;
            *dest_ptr.offset(11) = *(ptr as *const u16).offset(11);
            //
            dest_off
        }
    }
    unsafe {

        debug!("page root:{}", page.root());
        let mut left_page = MutPage { page: txn.txn.alloc_page().unwrap() };
        let mut right_page = MutPage { page: txn.txn.alloc_page().unwrap() };
        debug!("left page: {:?}, right page: {:?}",
               left_page.page_offset(),
               right_page.page_offset());
        left_page.init();
        right_page.init();

        let ptr_root = page.offset(page.root() as isize) as *mut u32;
        debug!("filling left page");
        {
            let left = u32::from_le(*ptr_root);
            if left == 1 {
                // local offset
                let left = u32::from_le(*((ptr_root as *const u32).offset(1)));
                let left_root = iter(txn, page, &mut left_page, left);
                left_page.set_root(left_root as u16);
            } else {
                // global offset, the tree is not balanced.
                // let path = "/tmp/before_split";
                // self.debug(path, 0);
                // panic!("not splitting unbalanced tree, dumped into {}", path)
                unreachable!()
            }
        }
        debug!("filling right page");
        {
            let right = u32::from_le(*(ptr_root.offset(2)));
            if right == 1 {
                // local offset
                let right = u32::from_le(*((ptr_root as *const u32).offset(3)));
                let right_root = iter(txn, page, &mut right_page, right);
                right_page.set_root(right_root as u16);
            } else {
                // global offset, the tree is not balanced.
                // let path = "/tmp/before_split";
                // self.debug(path, 0);
                // panic!("not splitting unbalanced tree, dumped into {}", path)
                unreachable!()
            }
        }
        debug!("done filling");
        let (key, value) = read_key_value(&*(ptr_root as *const u8));
        let left_offset = left_page.page_offset();
        let right_offset = right_page.page_offset();

        Result::Split {
            key: key,
            value: value,
            left: left_page,
            right: right_page,
            free_page: page.page_offset(),
        }
    }
}



#[inline]
unsafe fn incr(p: *mut u16) {
    *p = (u16::from_le(*p) + 1).to_le()
}

pub fn put(txn: &mut MutTxn, db: Db, key: &[u8], value: &[u8]) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = Value::S(value);
    put_lr(txn,root_page,key,value,0,0)
}

/// General version of put allowing to insert page children (no children is encoded by 0).
pub fn put_lr(txn: &mut MutTxn, root_page:Cow, key: &[u8], value: Value, left:u64, right:u64) -> Db {
    let put_result = insert(txn, root_page, key, value, left, right, 1);
    match put_result {
        Result::Split { key:key0,value:value0,left:l,right:r,free_page:fr } => {
            // the root page has split, we need to allocate a new one.
            let mut btree = txn.alloc_page();
            debug!("new root page:{:?}", btree);
            btree.init();
            let btree_off = btree.page_offset();
            let size = value_record_size(key0, value0);
            let off = btree.can_alloc(size);
            debug_assert!(off > 0);
            btree.alloc_key_value(off, size, key0, value0, l.page_offset(), r.page_offset());
            if fr > 0 {
                unsafe { transaction::free(&mut txn.txn, fr) }
            }
            btree.set_root(off);
            Db { root: btree_off }
        }
        Result::Ok { page,.. } => Db { root: page.page_offset() },
    }
}
