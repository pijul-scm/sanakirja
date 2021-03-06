use super::txn::*;
use super::transaction::{PAGE_SIZE,Error};
use std;
use std::cmp::Ordering;
use super::transaction;
use rand::{Rng};

extern crate log;

#[derive(Debug)]
pub enum Res {
    Ok { page: MutPage },
    Underfull {
        page: Cow, // The page where we want to delete something.
        delete: [u16;N_LEVELS], // The binding before the one we want to delete.
        merged: u64, // The updated left child of the deleted binding.
        must_be_dup: bool // This page is referenced at least twice (used when rebalancing fails)
    },
    Split {
        key_ptr:*const u8,
        key_len:usize,
        value: UnsafeValue,
        left: MutPage,
        right: MutPage,
        free_page: u64, // Former version of the page, before the
 // split. Free after the split is performed. Might be 0 if no page
 // needs to be freed / decremented.
    },
    Nothing { page:Cow }
}


pub fn fork_db<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64) -> Result<(),Error> {
    try!(incr_rc(rng,txn,off));
    Ok(())
}

/// Increase the reference count of a page.
pub fn incr_rc<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64)->Result<(),Error> {
    debug!(">>>>>>>>>>>> incr_rc");
    let mut rc = if let Some(rc) = txn.rc() { rc } else { try!(txn.create_db()) };
    let count = txn.get_u64(&rc, off).unwrap_or(1);
    debug!("incrementing page {:?} to {:?}", off, count+1);
    try!(txn.replace_u64(rng, &mut rc, off, count+1));
    txn.set_rc(rc);
    debug!("<<<<<<<<<<<< incr_rc");
    Ok(())
}

/// Increase the reference count of a page.
pub fn decr_rc<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64)->Result<(),Error> {
    let mut rc = if let Some(rc) = txn.rc() { rc } else { try!(txn.create_db()) };
    let count = txn.get_u64(&rc, off).unwrap_or(1);
    debug!(">>>>>>>>>>>> decr_rc {:?} {:?}", off, count);
    if count-1 <= 1 {
        try!(txn.del_u64(rng, &mut rc, off));
    } else {
        try!(txn.replace_u64(rng, &mut rc, off, count-1));
    }
    txn.set_rc(rc);
    debug!("<<<<<<<<<<<< decr_rc");
    Ok(())
}

/// Get the reference count of a page. Returns 0 if the page is not reference-counted.
pub fn get_rc<T:super::Transaction>(txn:&T, off:u64) -> u64 {
    if let Some(rc) = txn.rc() {
        txn.get_u64(&rc, off).unwrap_or(1)
    } else {
        0
    }
}


/// Decrease the reference count of a page, freeing it if it's no longer referenced.
pub fn free<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64) -> Result<(),Error> {
    //println!("freeing {:?}", off);
    debug_assert!(off != 0);
    let really_free = {
        if let Some(mut rc) = txn.rc() {
            if let Some(count) = txn.get_u64(&rc, off) {
                if count>1 {
                    debug!("rc: {:?}, off: {:?}, count: {:?}", rc, off, rc);
                    if count > 2 {
                        try!(txn.replace_u64(rng, &mut rc, off, count-1));
                    } else {
                        try!(txn.del_u64(rng, &mut rc, off));
                    };
                    txn.set_rc(rc);
                    false
                } else {
                    try!(txn.del_u64(rng,&mut rc,off));
                    txn.set_rc(rc);
                    true
                }
            } else {
                true
            }
        } else {
            true
        }
    };
    if really_free {
        let mut index = 3;
        if txn.protected_pages[0] == off {
            index = 0
        } else if txn.protected_pages[1] == off {
            index = 1
        }
        if index < 3 {
            debug!("not freeing protected {:?}", off);
            txn.free_protected[index] = true
        } else {
            debug!("really freeing {:?}", off);
            unsafe { transaction::free(&mut txn.txn, off) }
        }
    }
    Ok(())
}



/// Allocate one large values, spanning over at least one page.
pub fn alloc_value<T>(txn:&mut MutTxn<T>, value: &[u8]) -> Result<UnsafeValue,Error> {
    debug!("alloc_value");
    let mut len = value.len();
    let mut p_value = value.as_ptr();
    let mut page = try!(txn.alloc_page());
    let first_page = page.page_offset();
    unsafe {
        loop {
            if len <= PAGE_SIZE {
                std::ptr::copy_nonoverlapping(p_value, page.offset(0), len);
                break
            } else {
                std::ptr::copy_nonoverlapping(p_value, page.offset(8), PAGE_SIZE-8);
                p_value = p_value.offset((PAGE_SIZE-8) as isize);
                len -= PAGE_SIZE - 8;
                let next_page = try!(txn.alloc_page());
                *(page.offset(0) as *mut u64) = next_page.page_offset().to_le();
                page = next_page
            }
        }
    }
    debug_assert!(first_page > 0);
    debug!("/alloc_value");
    Ok(UnsafeValue::O { offset: first_page, len: value.len() as u32 })
}



pub fn free_value<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, mut offset:u64, mut len:u32)->Result<(),Error> {
    debug!(">>>>>>>>>>>>>>>>>>>>> freeing value {:?}", offset);
    let really_free =
        if let Some(mut rc) = txn.rc() {
            if let Some(count) = txn.get_u64(&mut rc, offset) {
                debug!("count = {:?}", count);
                if count>1 {
                    try!(txn.replace_u64(rng, &mut rc, offset, count-1));
                    txn.set_rc(rc);
                    false
                } else {
                    try!(txn.del_u64(rng, &mut rc, offset));
                    txn.set_rc(rc);
                    true
                }
            } else {
                true
            }
        } else {
            true
        };
    if (!cfg!(feature="no_free")) && really_free {
        debug!("really freeing value {:?}", offset);
        unsafe {
            loop {
                if len <= PAGE_SIZE as u32 {
                    transaction::free(&mut txn.txn, offset);
                    break
                } else {
                    let page = txn.load_cow_page(offset).data();
                    let next_offset = u64::from_le(*(page as *const u64));
                    transaction::free(&mut txn.txn, offset);

                    len -= (PAGE_SIZE-8) as u32;
                    offset = next_offset;
                }
            }
        }
    }
    debug!("<<<<<<<<<<<<<<<<<<<<< free_value");
    Ok(())
}


/// Returns a mutable copy of the page, possibly forgetting the next binding (and then possibly also freeing the associated value), and possibly incrementing the reference counts of child pages.
/// If translate_right > 0, replaces the next child page by translate_right.
///
/// For performance reasons, we don't copy anything on the way to the
/// leaves, instead copying on the way back.
///
/// Therefore, we might need to copy pages without freeing the
/// previous one, since their reference count is not yet updated.
///
pub fn copy_page<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, p:&Page, old_levels:&[u16], pinpoints:&mut [u16],
                      forgetting_next: bool, forgetting_value:bool,
                      translate_right: u64, incr_children_rc:bool) -> Result<MutPage,Error> {
    unsafe {
        // Reset all pinpoints.
        for i in 0.. N_LEVELS {
            pinpoints[i] = FIRST_HEAD;
        }
        //

        let forget = if forgetting_next {
            u16::from_le(*(p.offset(old_levels[0] as isize) as *const u16))
        } else {
            NIL
        };

        let mut page = try!(txn.alloc_page());
        debug!("copy_page: allocated {:?}", page.page_offset());
        page.init();
        let mut n = 0;
        let mut levels:[u16;N_LEVELS] = [FIRST_HEAD;N_LEVELS];
        
        let right_page =
            if old_levels[0]==FIRST_HEAD && translate_right > 0 {
                translate_right
            } else {
                let r = u64::from_le(*((p.offset(FIRST_HEAD as isize) as *mut u64).offset(2)));
                if incr_children_rc && r > 0 {
                    try!(incr_rc(rng, txn, r))
                }
                r
            };
        *((page.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = right_page.to_le();

        for (current, key, value, right) in PageIterator::new(p, 0) {

            let right = if current == old_levels[0] && translate_right > 0 {
                translate_right
            } else {
                right
            };
            if current != forget {

                if right > 0 && right != translate_right && incr_children_rc {
                    debug!("copy, incr {:?}", right);
                    try!(incr_rc(rng, txn, right))
                }

                // Increase count of value if the previous
                // page is not freed at the end of this
                // function.
                if incr_children_rc {
                    if let UnsafeValue::O { offset,.. } = value {
                        try!(incr_rc(rng, txn, offset))
                    }
                }
                debug!("copy_page: {:?}", std::str::from_utf8(key));
                let size = record_size(key.len(), value.len() as usize);
                let off = page.can_alloc(size);
                debug!("size={:?}, off = {:?}", size, off);
                debug_assert!(off > 0);
                page.reset_pointers(off);
                page.alloc_key_value(off, size, key.as_ptr(), key.len(), value);
                *((page.offset(off as isize) as *mut u64).offset(2)) = right.to_le();

                for level in 0..N_LEVELS {
                    if n & ((1 << level)-1) == 0 { // always true for level = 0
                        debug!("link from {:?} to {:?} at level {:?}", levels[level], off, level);
                        *((page.offset(levels[level] as isize) as *mut u16).offset(level as isize)) = off.to_le();
                        levels[level] = off;
                        // If the pinpointed offset has not passed yet, update the pinpoint at this level.
                        if pinpoints[0] == FIRST_HEAD && level > 0 && old_levels[0] != FIRST_HEAD {
                            pinpoints[level] = off
                        }
                    }
                }

                if old_levels[0] == current {
                    pinpoints[0] = off
                }
            } else {
                debug!("copy: forgetting");
                if forgetting_value {
                    // Here, maybe we need to forget
                    if let UnsafeValue::O { offset, len } = value {
                        //println!("cow_pinpointing: freeing value {:?}", offset);
                        try!(free_value(rng, txn, offset, len))
                    }
                }
            }
            n+=1;
        }
        Ok(page)
    }
}

/// Turn a Cow into a MutPage, copying it if it's not already mutable. In the case a copy is needed, and argument 'pinpoint' is non-zero, a non-zero offset (in bytes) to the equivalent element in the new page is returned. This can happen for instance because of compaction.
pub fn cow_pinpointing<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, old_levels:&[u16], pinpoints:&mut [u16],
                                forgetting_next: bool, forgetting_value:bool,
                                translate_right:u64) -> Result<MutPage,Error> {
    unsafe {
        match page.cow {
            transaction::Cow::Page(p0) => {
                let p0_offset = p0.offset;
                let page_rc = get_rc(txn, p0_offset);
                let p = Page { page:p0 };
                
                let page = try!(copy_page(rng, txn, &p, old_levels, pinpoints, forgetting_next,
                                          forgetting_value, translate_right, false)); // never increase the counter of child pages
                if page_rc <= 1 {
                    if page_rc == 1 {
                        let mut rc = txn.rc().unwrap();
                        try!(txn.del_u64(rng, &mut rc, p0_offset));
                        txn.set_rc(rc);
                    }
                    //println!("free cow: {:?}", page_offset);
                    if !cfg!(feature="no_free") {
                        transaction::free(&mut(txn.txn), p0_offset)
                    }
                } else {
                    let mut rc = txn.rc().unwrap();
                    try!(txn.replace_u64(rng, &mut rc, p0_offset, page_rc-1));
                    txn.set_rc(rc);
                }
                Ok(page)
            }
            transaction::Cow::MutPage(p) => {
                let p = MutPage { page:p };
                std::ptr::copy_nonoverlapping(old_levels.as_ptr(), pinpoints.as_mut_ptr(), old_levels.len());
                if forgetting_next {
                    let next = u16::from_le(*(p.offset(old_levels[0] as isize) as *const u16));
                    debug!("next = {:?}", next);
                    debug_assert!(next > 0);
                    // We forget an entry, register the freed memory.
                    let (key,value) = read_key_value(p.offset(next as isize));
                    if forgetting_value {
                        if let UnsafeValue::O { offset, len } = value {
                            // println!("cow_pinpointing: freeing value {:?}", offset);
                            try!(free_value(rng, txn, offset, len))
                        }
                    }
                    // Mark the freed space on the page.
                    let size = record_size(key.len(),value.len() as usize);
                    *(p.p_occupied()) = (p.occupied() - size).to_le();


                    // Now, really delete!
                    for l in 0..N_LEVELS {
                        debug_assert!(old_levels[l] != NIL);
                        let next_l = u16::from_le(*((p.offset(old_levels[l] as isize) as *const u16).offset(l as isize)));
                        if next_l == next && next != NIL {
                            // Replace the next one with the next-next-one, at this level.
                            let next_next =  u16::from_le(*((p.offset(next_l as isize) as *const u16).offset(l as isize)));
                            debug!("copy {:?}, creating {:?} -> {:?} at level {:?}",
                                   p.page_offset(),
                                   old_levels[l],
                                   next_next, l);
                            *((p.offset(old_levels[l] as isize) as *mut u16).offset(l as isize)) =
                                next_next.to_le()
                        } else {
                            debug!("copy {:?}, no link at level {:?}, old_levels[l]={:?}, next_l={:?}, next={:?}",
                                   p.page_offset(),
                                   l, old_levels[l], next_l, next);
                        }
                    }
                }
                if translate_right > 0 {
                    // Translate the right page.
                    *((p.offset(old_levels[0] as isize) as *mut u64).offset(2)) = translate_right.to_le();
                }
                Ok(p)
            }
        }
    }
}


#[cfg(test)]
fn test_insert(value_size:usize) {
    extern crate tempdir;
    extern crate rand;
    extern crate env_logger;
    use super::{Env, Transaction};

    use rand::{Rng};
    let mut rng = rand::thread_rng();

    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 1000).unwrap();
    let mut txn = env.mut_txn_begin().unwrap();

    let mut page = txn.alloc_page().unwrap();
    page.init();

    let mut random:Vec<(String,String)> = Vec::new();
    
    for i in 0..200 {
        println!("i={:?}", i);
        let key: String = rng
            .gen_ascii_chars()
            .take(200)
            .collect();
        println!("key = {:?}", key);
        let value: String = rng
            .gen_ascii_chars()
            .take(value_size)
            .collect();
        {
            let key = key.as_bytes();
            let value = value.as_bytes();
            let value = if value.len() > VALUE_SIZE_THRESHOLD {
                alloc_value(&mut txn,value).unwrap()
            } else {
                UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
            };

            match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0, false) {
                Ok(Res::Ok { page:page_,.. }) => {
                    page = page_
                },
                Ok(Res::Nothing { page:page_ }) => {
                    //println!("already present");
                    page = page_.unwrap_mut()
                },
                Ok(x) => {
                    page = root_split(&mut rng, &mut txn, x).unwrap()
                },
                _ => panic!("")
            }

            let db = Db { root_num: -1, root: page.page_offset() };
            debug!("debugging");
            txn.debug(&[&db], format!("/tmp/after_{}",i), false, false);
            for &(ref key, _) in random.iter() {
                assert!(txn.get(&db, key.as_bytes(), None).is_some())
            }

        }
        random.push((key,value));
    }

    let db = Db { root_num: -1, root: page.page_offset() };
    txn.debug(&[&db], format!("/tmp/debug"), false, false);
    for &(ref key, _) in random.iter() {
        assert!(txn.get(&db, key.as_bytes(), None).is_some())
    }
}


#[test]
fn test_insert_small() {
    test_insert(50)
}

#[test]
fn test_insert_large() {
    test_insert(2000)
}



/// Changes the value of levels and eq, so that all items in levels are offsets to the largest entry in the list strictly smaller than (key,value).
pub fn set_levels<T,P:super::txn::P>(txn:&MutTxn<T>, page:&P, key:&[u8], value:Option<UnsafeValue>, levels:&mut [u16], eq:&mut bool) {
    let mut level = N_LEVELS-1;
    let mut current_off = FIRST_HEAD;
    let mut current = page.offset(FIRST_HEAD as isize) as *const u16;
    let mut last_compared_offset = 0;
    loop {
        // advance in the list until there's nothing more to do.
        loop {
            let next = u16::from_le(unsafe { *(current.offset(level as isize)) }); // next in the list at the current level.
            //println!("first loop, next = {:?}", next);
            if next == NIL {
                debug!("next=NIL, current_off={:?}", current_off);
                levels[level] = current_off;
                break
            } else {
                debug_assert!(next!=0);
                if next == last_compared_offset {
                    // We're going to get the same result as last
                    // time, and this wasn't Ordering::Greater. It it
                    // was Ordering::Equal, we already set eq.
                    break
                } else {
                    last_compared_offset = next;
                    let next_ptr = page.offset(next as isize);
                    let (next_key,next_value) = unsafe { read_key_value(next_ptr) };
                    // debug!("compare: {:?} {:?}", std::str::from_utf8(key), std::str::from_utf8(next_key));
                    match key.cmp(next_key) {
                        Ordering::Less => break,
                        Ordering::Equal =>
                            if let Some(value) = value {
                                /*if cfg!(test) {
                                    unsafe {
                                        if (Value::from_unsafe(&value, txn)).cmp(Value::from_unsafe(&next_value, txn)) != Ordering::Equal {
                                            debug!("differ on value {:?}", next_value);
                                            let mut s0 = Vec::new();
                                            for i in Value::from_unsafe(&value, txn) {
                                                s0.extend(i)
                                            }
                                            let mut s1 = Vec::new();
                                            for i in Value::from_unsafe(&next_value, txn) {
                                                s1.extend(i)
                                            }
                                            debug!("{:?}", std::str::from_utf8(&s0));
                                            debug!("{:?}", std::str::from_utf8(&s1));
                                        }
                                    }
                                }*/
                                match unsafe { (Value::from_unsafe(&value, txn)).cmp(Value::from_unsafe(&next_value, txn)) } {
                                    Ordering::Less => break,
                                    Ordering::Equal => {
                                        *eq = true;
                                        break
                                    },
                                    Ordering::Greater => {
                                        current_off = next;
                                        current = page.offset(current_off as isize) as *const u16;
                                    }
                                }
                            } else {
                                // If no value was given, set at the smallest value, hence here.
                                *eq = true;
                                break
                            },
                        Ordering::Greater => {
                            current_off = next;
                            current = page.offset(current_off as isize) as *const u16;
                        }
                    }
                }
            }
        }
        levels[level] = current_off;
        if level == 0 {
            break
        } else {
            level -= 1;
            levels[level] = levels[level+1]
        }
    }
}



pub fn insert<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, key:&[u8], value:UnsafeValue, right_page:u64, parent_will_be_dup:bool) -> Result<Res,Error> {
    debug!("insert page = {:?}", page.page_offset());
    let mut eq = false;
    let mut levels = [0;N_LEVELS];
    set_levels(txn, &page, key, Some(value), &mut levels[..], &mut eq);
    debug!("levels={:?}", levels);
    if eq {
        Ok(Res::Nothing{page:page})
    } else {
        let child_page = page.right_child(levels[0]);
        let page_rc = get_rc(txn, page.page_offset());
        let page_will_be_dup = parent_will_be_dup || (page_rc > 1);
        debug!("page_rc = {:?} {:?}", parent_will_be_dup, page_rc);
        if child_page > 0 && right_page == 0 {
            debug!("inserting in child page {:?}", child_page);
            // Insert in the page below.
            let next_page = txn.load_cow_page(child_page);

            match try!(insert(rng, txn, next_page, key, value, right_page, page_will_be_dup)) {
                Res::Nothing{..} => Ok(Res::Nothing { page:page }),
                Res::Ok { page:next_page } => {
                    debug!("Child returned ok: {:?}", next_page);

                    // The page below was updated. Update the reference in the current page
                    let mut new_levels = [0;N_LEVELS];
                    
                    if !page_will_be_dup {
                        let page = try!(cow_pinpointing(rng, txn, page, &levels[..], &mut new_levels[..], false, false,
                                                        next_page.page_offset()));
                        Ok(Res::Ok { page:page })
                    } else {
                        // Decrement the counter for the first page with RC>1 on the path from the root.
                        if !parent_will_be_dup && page_rc > 1 {
                            try!(decr_rc(rng, txn, page.page_offset()))
                        }
                        let page =
                            try!(copy_page(rng, txn, &page.as_page(), &levels[..], &mut new_levels[..], false, false,
                                           next_page.page_offset(), true));
                        Ok(Res::Ok { page: page })
                    }
                },
                Res::Split { key_ptr,key_len,value:value_,left,right,free_page } => {
                    debug_assert!(free_page == child_page || free_page == 0);
                    // The page below split. Update the child to the
                    // left half of the split, and insert the middle
                    // element returned by the split in the current
                    // page.

                    // Now reinsert the element here.
                    let key_ = unsafe {std::slice::from_raw_parts(key_ptr, key_len)};
                    let result = unsafe {
                        full_local_insert(rng, txn, page, key_, value_, right.page_offset(),
                                          &mut levels, left.page_offset(), parent_will_be_dup,
                                          page_will_be_dup)
                    };
                    if !page_will_be_dup && free_page > 0 {
                        try!(free(rng, txn, free_page));
                    }
                    result
                },
                Res::Underfull {..} => unreachable!()
            }
        } else {
            debug!("inserting here");
            // No child page, insert on this page.
            unsafe {
                full_local_insert(rng, txn, page, key, value, right_page, &mut levels, 0, parent_will_be_dup, page_will_be_dup)
            }
        }
    }
}

pub unsafe fn full_local_insert<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, key:&[u8], value:UnsafeValue, right_page:u64, levels:&mut [u16], left_page:u64, parent_will_be_dup: bool, page_will_be_dup:bool) -> Result<Res, Error> {
    let size = record_size(key.len(), value.len() as usize);
    let mut new_levels = [0;N_LEVELS];
    if !page_will_be_dup {

        let off = page.can_alloc(size);
        if off > 0 {
            let (mut page,off) =
                if off + size < PAGE_SIZE as u16 && get_rc(txn, page.page_offset()) <= 1 {
                    // No need to copy nor compact the page, the value can be written right away.
                    (try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels,
                                          false, false, left_page)),
                     off)
                } else {
                    // Here, we need to compact the page, which is equivalent to considering it non mutable and CoW it.

                    let page = try!(cow_pinpointing(rng, txn, page.as_nonmut(),
                                                    &levels[..],
                                                    &mut new_levels[..], false, false,
                                                    left_page));
                    let off = page.can_alloc(size);
                    (page, off)
                };
            local_insert_at(rng, &mut page, key, value, right_page,
                            off, size, &mut new_levels[..]);
            Ok(Res::Ok { page:page })
        } else {
            debug!("splitting, key = {:?}", std::str::from_utf8(key));
            if left_page > 0 {
                Ok(try!(split_page(rng, txn, &page, key, value, right_page, page_will_be_dup, NIL, levels[0], left_page)))
            } else {
                Ok(try!(split_page(rng, txn, &page, key, value, right_page, page_will_be_dup, NIL, NIL, 0)))
            }
        }

    } else {
        if !parent_will_be_dup {
            try!(decr_rc(rng, txn, page.page_offset()))
        }
        let off = page.can_alloc(size);
        if off > 0 {
            let p = txn.load_page(page.page_offset());
            let mut page = try!(copy_page(rng, txn, &p, levels, &mut new_levels, false, false, left_page, true));
            local_insert_at(rng, &mut page, key, value, right_page,
                            off, size, &mut new_levels[..]);
            Ok(Res::Ok { page:page })
        } else {
            debug!("splitting, key = {:?}", std::str::from_utf8(key));
            if left_page > 0 {
                Ok(try!(split_page(rng, txn, &page, key, value, right_page, page_will_be_dup, NIL, levels[0], left_page)))
            } else {
                Ok(try!(split_page(rng, txn, &page, key, value, right_page, page_will_be_dup, NIL, NIL, 0)))
            }
        }
    }
}



/// If the "levels" (pointers to the current elements of each of the
/// lists) are known, allocate an element of size size at offset off,
/// updates the lists on the page, and update the levels accordingly.
pub fn local_insert_at<R:Rng>(rng:&mut R, page:&mut MutPage, key:&[u8], value:UnsafeValue, right_page:u64, off:u16, size:u16, levels:&mut [u16]) {
    debug!("entering local_insert_at");
    debug_assert!(off + size <= PAGE_SIZE as u16);
    page.reset_pointers(off);
    page.alloc_key_value(off, size, key.as_ptr(), key.len(), value);
    page.set_right_child(off, right_page);
    for i in 0..N_LEVELS {
        let next = page.level(levels[i], i);
        debug!("{:?} levels[{:?}]={:?}, next={:?}", page.page_offset(), i, levels[i], next);
        // debug_assert!(next != 0);
        /*if let UnsafeValue::O { ref offset,.. } = value {
        debug!("local_insert_at: UnsafeValue::O {:?}", offset);
    }*/
        page.set_level(off, i, next);
        // *((page.offset(off as isize) as *mut u16).offset(i as isize)) = next;
        page.set_level(levels[i], i, off);
        // *((page.offset(levels[i] as isize) as *mut u16).offset(i as isize)) = off.to_le();
        debug!("local_insert_at: link from {:?}.{:?} to {:?}, at level {:?}", page.page_offset(), levels[i], off, i);
        levels[i] = off;
        if rng.gen() {
            break
        }
    }
    debug!("exiting local_insert_at");
}


/// The arguments to split_page are non-trivial. This function takes a
/// page and an element to insert, too large to fit in the page. It
/// splits the page, inserts the new element, and returns the middle
/// element of the split as a Res::Split { .. }.
///
/// Moreover, this function guarantees that before reinserting the
/// binding given as argument, each of the two sides of the split can
/// hold at least two more bindings (this is required for deletions).
pub unsafe fn split_page<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>,page:&Cow,
                              // (key, value, right_page) of the record to insert.
                              key:&[u8], value:UnsafeValue, right_page:u64,
                              // Sometimes, a split propagates upwards:
                              // more precisely, inserting the middle
                              // element into the page upwards causes it
                              // to split. If the page upwards was
                              // non-mutable, we could not write the
                              // page to the left of the middle element
                              // before the split (without copying the
                              // whole soon-to-be-freed page, of
                              // course). translate_index and
                              // translate_right_page are meant for this
                              // purpose: the pointer to the page that
                              // split is "translated" to a pointer to the
                                  // left page of the split.
                                  page_will_be_dup:bool,
                                  forgetting:u16,
                                  translate_index:u16, translate_right_page:u64)->Result<Res,Error> {

    debug!("split {:?} {:?}", page.page_offset(), page_will_be_dup);
    debug!("split {:?}", std::str::from_utf8(key));
    let mut left = try!(txn.alloc_page());
    left.init();
    let mut right = try!(txn.alloc_page());
    right.init();
    debug!("split allocated {:?} {:?}", left.page_offset(), right.page_offset());
    *((left.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) =
        if translate_index == 0 {
            translate_right_page.to_le()
        } else {
            let r = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
            if page_will_be_dup && r > 0 { try!(incr_rc(rng, txn, r)) }
            r.to_le()
        };

    // Loop through the values of the page, in order, and insert them to left in order.
    // Stop whenever both pages can include one extra entry after inserting the input entry to this function.

    let mut left_bytes = 24;
    let mut left_levels = [FIRST_HEAD;N_LEVELS];
    let mut right_levels = [FIRST_HEAD;N_LEVELS];
    let mut middle = None;

    let mut extra_on_lhs = false;
    
    for (current, key_, value_, r) in PageIterator::new(page,0) {
        debug!("split key_ = {:?} {:?}", current, std::str::from_utf8(key_));
        if current == forgetting {
            // Only used in rebalance, which already frees values.
            /*if !page_will_be_dup {
                if let UnsafeValue::O { offset, len } = value_ {
                    try!(free_value(rng, txn, offset, len));
                }
            }*/
            continue
        }
        let r = if current == translate_index {
            translate_right_page
        } else {
            if page_will_be_dup && r > 0 {
                try!(incr_rc(rng, txn, r))
            }
            r
        };
        if page_will_be_dup {
            if let UnsafeValue::O { offset, .. } = value_ {
                try!(incr_rc(rng, txn, offset))
            }
        }
        let next_size = record_size(key_.len(),value_.len() as usize);
        if middle.is_none() { // Insert in left page.
            if left_bytes + next_size <= (PAGE_SIZE as u16) / 2 {
                // insert in left page.
                let off = left.can_alloc(next_size);
                local_insert_at(rng, &mut left, key_, value_, r, off, next_size, &mut left_levels);
                left_bytes += next_size;
            } else {
                // Maybe we won't insert the new key here, in which case we can go one more step.
                if left_bytes <= (PAGE_SIZE as u16) / 2 {
                    extra_on_lhs = match key.cmp(key_) {
                        Ordering::Less => true,
                        Ordering::Greater => false,
                        Ordering::Equal =>
                            match (Value::from_unsafe(&value, txn)).cmp(Value::from_unsafe(&value_, txn)) {
                                Ordering::Less | Ordering::Equal => true,
                                Ordering::Greater => false
                            }
                    };
                    debug!("one more key ? {:?}", extra_on_lhs);
                    if !extra_on_lhs {
                        // The next key is larger than all elements on
                        // the left page, but smaller than the extra key.
                        // This is the separator.
                        middle = Some((key_.as_ptr(),key_.len(),value_,r))
                    } else {
                        // We insert the extra key on the left-hand side now. and save (key_,value_) for later.
                        let mut levels = [0;N_LEVELS];
                        let mut eq = false;
                        set_levels(txn, &left, key, Some(value), &mut levels[..], &mut eq);

                        let size = record_size(key.len(), value.len() as usize);
                        let off = left.can_alloc(size);
                        local_insert_at(rng, &mut left, key, value, right_page, off, size, &mut levels);
                        left_bytes += size;
                        middle = Some((key_.as_ptr(),key_.len(),value_,r))
                    }
                } else {
                    middle = Some((key_.as_ptr(),key_.len(),value_,r))
                }
            }
        } else {
            // insert in right page.
            let off = right.can_alloc(next_size);
            local_insert_at(rng, &mut right, key_, value_, r, off, next_size, &mut right_levels);
        }
    }

    // If the extra entry was not added to the left-hand side, add it to the right-hand side.
    debug!("extra_on_lhs: {:?}", extra_on_lhs);
    if !extra_on_lhs {

        if cfg!(test) {
            if let Some((key_ptr, key_len, _, _)) = middle {
                // check that we're inserting on the right side.
                let key_ = std::slice::from_raw_parts(key_ptr, key_len);
                debug_assert!( key >= key_ )
            }
        }

        let mut levels = [0;N_LEVELS];
        let mut eq = false;
        set_levels(txn, &right, key, Some(value), &mut levels[..], &mut eq);

        let size = record_size(key.len(), value.len() as usize);
        let off = right.can_alloc(size);
        local_insert_at(rng, &mut right, key, value, right_page, off, size, &mut levels);
    }
    if let Some((key_ptr, key_len, value_, right_child)) = middle {
        *((right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = right_child.to_le();
        Ok(Res::Split {
            key_ptr: key_ptr,
            key_len: key_len,
            value: value_,
            left: left,
            right: right,
            free_page: if page_will_be_dup { 0 } else { page.page_offset() }
        })
    } else {
        unreachable!()
    }
}



// This function deals with the case where the main page split, either during insert, or during delete.
pub fn root_split<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, x:Res) -> Result<MutPage,Error> {
    debug!("ROOT SPLIT");
    if let Res::Split { left,right,key_ptr,key_len,value,free_page } = x {
        let mut page = try!(txn.alloc_page());
        page.init();
        page.set_right_child(FIRST_HEAD, left.page_offset());
        let mut levels = [0;N_LEVELS];
        let size = record_size(key_len, value.len() as usize);
        let off = page.can_alloc(size);
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        local_insert_at(rng, &mut page, key, value, right.page_offset(), off, size, &mut levels);
        debug!("root split, freeing {:?}", free_page);
        try!(free(rng, txn, free_page));
        Ok(page)
    } else {
        unreachable!()
    }
}


pub fn put<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, db: &mut Db, key: &[u8], value: &[u8])->Result<bool,Error> {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = if value.len() > VALUE_SIZE_THRESHOLD {
        try!(alloc_value(txn,value))
    } else {
        UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
    };
    debug!("key = {:?}", std::str::from_utf8(key));
    unsafe { debug!("value = {:?}", Value::from_unsafe(&value, txn)) }
    match try!(insert(rng, txn, root_page, key, value, 0, false)) {
        Res::Nothing { .. } => Ok(false),
        Res::Ok { page,.. } => { db.root = page.page_offset(); Ok(true) }
        x => {
            db.root = try!(root_split(rng,txn,x)).page_offset();
            Ok(true)
        }
    }
}
