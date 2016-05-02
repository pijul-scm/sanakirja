use super::txn::*;
use super::transaction::{PAGE_SIZE,Error};
use std;
use rand::{Rng};
use super::Transaction;

extern crate log;
use super::put::*;
use super::merge;
use super::rebalance;

// This type is an instruction to page_delete below.
#[derive(Copy,Clone,Debug)]
enum C<'a> {
    KV { key:&'a [u8], value:UnsafeValue }, // delete by comparing the key and value.
    K { key:&'a[u8] }, // delete the smallest binding of that key.
    Smallest // delete the smallest element of a B-tree (used to replace the root of a B-tree).
}
impl<'a> C<'a> {
    fn is_smallest(&self)->bool {
        match self {
            &C::Smallest => true,
            _ => false
        }
    }
}

// Return type of the smallest (key,value).
pub struct Smallest {
    // smallest key
    pub key_ptr:*const u8,
    pub key_len:usize,
    // smallest of its values
    pub value:UnsafeValue,
    pub free_page: u64,
}


// Handle an Res::Underfull result from the current page.
//
// - child_page is the page that was just edited. We still need to
// delete according to the "delete" argument, and replace the left
// child of that element with "merged".
//
// - The levels are at the element whose right child is child_page.
//
fn handle_underfull<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, mut page:Cow, levels:[u16;N_LEVELS],
                                     child_page:Cow,
                                     delete:[u16;N_LEVELS], merged:u64) -> Result<Res, Error> {
    debug!("handle_underfull");
    let mut new_levels = [0;N_LEVELS];
    unsafe {
        std::ptr::copy_nonoverlapping(levels.as_ptr(), new_levels.as_mut_ptr(), N_LEVELS)
    }

    // First try to merge with our right sibling.
    let next_offset = unsafe { u16::from_le(*(page.offset(levels[0] as isize) as *const u16)) };
    if next_offset != NIL {
        match try!(merge::merge_children_right(rng, txn, page, levels, &child_page, &delete, merged)) {

            Res::Nothing { page:page_ } => {
                // If we couldn't merge:
                if levels[0] == FIRST_HEAD {
                    // If we're at the first binding, and there's no
                    // left sibling to try to merge with. In this
                    // case, the child page is the left child of the
                    // key to rebalance.
                    debug!("first case of rebalancing: {:?}", levels[0]);
                    let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
                    let mut new_levels = [0;N_LEVELS];
                    unsafe {
                        std::ptr::copy_nonoverlapping(levels.as_ptr(), new_levels.as_mut_ptr(), N_LEVELS)
                    }
                    match try!(rebalance::rebalance_left(rng, txn, page_, levels, &child_page, forgetting, merged)) {
                        Res::Nothing { page:page_ } => {
                            let child_page = txn.load_cow_page(child_page.page_offset());
                            return rebalance::handle_failed_left_rebalancing(rng, txn, page_, levels, child_page, delete, merged)
                        },
                        x => return Ok(x)
                    }
                } else {
                    // Or there's a left sibling to merge with, and
                    // it's appropriate to merge to the left
                    // (i.e. we've not deleted here).
                    page = page_
                }
            },
            res => return Ok(res)
        }
    }
    // If we haven't find a solution so far, move to the previous element, and merge the child page with its left sibling.

    // Move back by one
    debug!("trying to merge to left");
    set_pred(&page, &mut new_levels);
    match try!(merge::merge_children_left(rng, txn, page, new_levels, &child_page, &delete, merged)) {
        Res::Nothing { page } => {
            // we couldn't merge. rebalance.
            debug!("second case of rebalancing: {:?}", child_page);
            let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
            let result = match try!(rebalance::rebalance_right(rng, txn, page, new_levels, None, &child_page, forgetting, merged)) {
                Res::Nothing { page:page_ } => {
                    let child_page = txn.load_cow_page(child_page.page_offset());
                    return rebalance::handle_failed_right_rebalancing(rng, txn, page_, new_levels, None, child_page, delete, merged)
                },
                x => Ok(x)
            };
            debug!("rebalancing done");
            result
        },
        res => Ok(res)
    }
}
/// Move back to the predecessor of levels[0]. If levels[0] appears in
/// other lists, move back on them too.
fn set_pred(page:&Cow, levels:&mut [u16]) {
    //trace!("set_pred");
    let level0 = levels[0];
    debug_assert!(level0 != FIRST_HEAD && level0 != NIL);
    let mut l = 1;
    // Go up in levels until we find an entry different from level0.
    while l < N_LEVELS && levels[l] == level0 {
        l += 1
    }
    //trace!("!set_pred");
    // Now we go down by one step, staying at the same (non-level0) place in that new list.
    if l == N_LEVELS {
        l -=1;
        levels[l] = FIRST_HEAD;
    } else {
        l -=1;
        levels[l] = levels[l+1]
    }
    //trace!("?set_pred");
    // and advance in all the lists until we find level0 (level0 is in all the lists of level l or below).
    loop {
        loop {
            let next = u16::from_le(unsafe { *((page.offset(levels[l] as isize) as *const u16).offset(l as isize)) });
            if next == level0 {
                break
            } else {
                levels[l] = next
            }
        }
        if l==0 {
            break
        } else {
            l-=1;
            levels[l] = levels[l+1]
        }
    }
    //trace!("/set_pred");
}





fn handle_underfull_replace<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
                                     child_page:Cow,
                                     replacement:&Smallest,
                                     delete:[u16;N_LEVELS], merged:u64) -> Result<Res, Error> {
    debug!("handle_underfull_replace");
    // First try to merge with our right sibling.
    match try!(merge::merge_children_replace(
        rng, txn, page, levels, &child_page,
        replacement,
        &delete, merged)) {
        
        Res::Nothing { page:page_ } => {
            // If we couldn't merge:
            debug!("rebalancing: {:?}", levels[0]);
            let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
            match try!(rebalance::rebalance_right(rng, txn, page_, levels, Some(replacement), &child_page, forgetting, merged)) {
                Res::Nothing { page:page_} => {
                    let child_page = txn.load_cow_page(child_page.page_offset());
                    return rebalance::handle_failed_right_rebalancing(rng, txn, page_, levels, Some(replacement), child_page, delete, merged)
                },
                x => Ok(x)
            }
        },
        res => Ok(res)
    }
}






fn delete_at_internal_node<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS]) -> Result<Res,Error> {
    debug!("delete_at_internal_node {:?}", page);
    // Not found below, but we can delete something here.

    // Find the matching element, and the page to its right.
    let next_off = {
        let current = page.offset(levels[0] as isize) as *mut u16;
        unsafe { u16::from_le(*current) }
    };
    let next = page.offset(next_off as isize);
    let child_page = unsafe { u64::from_le(*((next as *const u64).offset(2))) };
    let child_page = txn.load_cow_page(child_page);

    // Delete the smallest element of the current binding's right child.
    debug!("delete smallest, levels = {:?}", &levels[..]);
    match try!(delete(rng,txn, child_page, C::Smallest)) {
        (Res::Ok { page: child_page }, Some(smallest)) => {
            debug!("internal: ok");
            // Set the child page here, regardless of whether a merge is coming after this.
            debug!("not underfull");

            let key = unsafe { std::slice::from_raw_parts(smallest.key_ptr, smallest.key_len) };
            let size = record_size(smallest.key_len, smallest.value.len() as usize);
            let off = page.can_alloc(size);
            let result = if off > 0 {


                let mut new_levels = [0;N_LEVELS];
                let mut page = if off + size < PAGE_SIZE as u16 {
                    try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, false, true, 0))
                } else {
                    try!(cow_pinpointing(rng, txn, page.as_nonmut(), &levels, &mut new_levels, true, false, true, 0))
                };
                let off = page.can_alloc(size);

                debug_assert!(off + size <= PAGE_SIZE as u16);
                unsafe {
                    local_insert_at(rng, &mut page, key, smallest.value, child_page.page_offset(), off, size, &mut new_levels)
                }
                Res::Ok { page:page }

            } else {
                // split page.
                unsafe {
                    try!(split_page(rng, txn, &page,
                                    key, smallest.value, child_page.page_offset(),
                                    next_off, 0))
                }
                                
            };
            if smallest.free_page > 0 {
                try!(free(rng, txn, smallest.free_page, false));
            }
            Ok(result)
        },
        (Res::Underfull { page: child_page, delete, merged }, Some(smallest)) => {

            debug!("internal: underfull");
            handle_underfull_replace(rng, txn, page, levels, child_page, &smallest, delete, merged)

        },
        (Res::Split { key_len,key_ptr,value, left, right, free_page }, Some(smallest)) => {

            debug!("internal: split");
            let middle_key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
            let middle_size = record_size(key_len, value.len() as usize);

            let smallest_key = unsafe { std::slice::from_raw_parts(smallest.key_ptr, smallest.key_len) };
            let smallest_size = record_size(smallest.key_len, smallest.value.len() as usize);

            // We need to insert middle_key -> right and smallest_key -> left to the page.

            let result = if page.occupied() + middle_size + smallest_size <= PAGE_SIZE as u16 {
                let middle_off = page.can_alloc(middle_size);
                debug_assert!(middle_off + middle_size <= PAGE_SIZE as u16);

                let mut new_levels = [0;N_LEVELS];
                // Delete the current element.
                let mut page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, false, true, 0));
                // Reinsert the left page with the smallest key.
                unsafe {
                    local_insert_at(rng, &mut page, middle_key, value, right.page_offset(), middle_off, middle_size, &mut new_levels);
                }

                let smallest_off = page.can_alloc(smallest_size);
                debug_assert!(smallest_off + smallest_size <= PAGE_SIZE as u16); // TODO: compact in the call to cow_pinpointing above, if necessary.
                unsafe {
                    local_insert_at(rng, &mut page, smallest_key, smallest.value, left.page_offset(), smallest_off, smallest_size, &mut new_levels);
                }

                Ok(Res::Ok { page:page })
            } else {
                // split.
                unsafe {
                    split_page(rng, txn, &page,
                               middle_key, value, right.page_offset(),
                               levels[0], left.page_offset())
                }
            };
            try!(free(rng, txn, free_page, false));
            result
        },
        (Res::Ok { .. }, None) |
        (Res::Split { .. }, None) |
        (Res::Underfull { .. }, None) |
        (Res::Nothing { .. }, _) => {
            if cfg!(debug_assertions) {
                panic!("Child page {:?} was empty when trying to remove its smallest element.", page)
            } else {
                unreachable!()
            }
        }
    }
}


fn delete<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, comp:C) -> Result<(Res,Option<Smallest>), Error> {

    debug!("delete = {:?}", page);
    let mut levels:[u16;N_LEVELS] = [FIRST_HEAD;N_LEVELS];
    let mut eq = false;
    match comp {
        C::KV { key, value } => set_levels(txn, &page, key, Some(value), &mut levels, &mut eq),
        C::K { key } => set_levels(txn, &page, key, None, &mut levels, &mut eq),
        C::Smallest => { eq = true }
    }
    let child_page = u64::from_le(unsafe { *((page.offset(levels[0] as isize) as *const u64).offset(2)) });
    debug!("next_page = {:?}, {:?}", child_page, eq);
    let del = if child_page > 0 {
        let next_page = txn.load_cow_page(child_page);
        Some(try!(delete(rng, txn, next_page, comp)))
    } else {
        None
    };
    match del {
        None if eq => {
            let (next_key,next_value) = {
                let cur_ptr = page.offset(levels[0] as isize) as *const u16;
                let next_off = u16::from_le(unsafe { *cur_ptr });
                debug_assert!(next_off > 0 && next_off != NIL);
                let next_ptr = page.offset(next_off as isize);
                unsafe { read_key_value(next_ptr) }
            };
            let deleted_size = record_size(next_key.len(), next_value.len() as usize);

            let will_be_underfull = page.occupied() - deleted_size < (PAGE_SIZE as u16)/2;

            if comp.is_smallest() {
                let smallest =
                    Some(Smallest {
                        key_ptr: next_key.as_ptr(),
                        key_len: next_key.len(),
                        value: next_value,
                        free_page: if will_be_underfull { page.page_offset() } else { 0 }
                    });
                // We're deleting next_key,next_value, and returning it to its ancestors.
                if will_be_underfull {
                    Ok((Res::Underfull { page:page, delete: levels, merged:0 }, smallest))
                } else {
                    // Else, we need to actually delete something, but
                    // since we're returning something from this page,
                    // we must not free it.
                    let mut new_levels = [0;N_LEVELS];
                    let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, false, false, 0));
                    Ok((Res::Ok { page:page }, smallest))
                }
            } else {
                debug!("{:?} {:?}", will_be_underfull, levels);
                if will_be_underfull {
                    Ok((Res::Underfull { page:page, delete: levels, merged:0 }, None))
                } else {
                    let mut new_levels = [0;N_LEVELS];
                    let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, true, true, 0));
                    Ok((Res::Ok { page:page }, None))
                }
            }
        },
        Some((Res::Nothing { .. }, _)) if eq => {
            // Find smallest, etc.
            Ok((try!(delete_at_internal_node(rng, txn, page, levels)), None))
        }
        Some((Res::Ok { page:child_page }, smallest)) => {
            debug!("ok, back to page {:?} with child {:?}", page.page_offset(), child_page.page_offset());
            // Update the pointer here
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels,
                                            false, false, true, child_page.page_offset()));
            Ok((Res::Ok { page:page }, smallest))
        },
        Some((Res::Underfull { page:child_page, delete, merged }, smallest)) => {
            // Decide which neighbor to merge with.
            debug!("delete: underfull {:?}", child_page);
            let result = Ok((try!(handle_underfull(rng, txn, page, levels, child_page, delete, merged)), smallest));
            debug!("underfull done");
            result
        },
        Some((Res::Nothing {.. },_)) | None => {
            Ok((Res::Nothing { page:page }, None))
        },

        Some((Res::Split { key_ptr,key_len,value:value_,left,right,free_page }, smallest)) => {
            // Now reinsert the element here.
            let key_ = unsafe {std::slice::from_raw_parts(key_ptr, key_len)};
            let result = unsafe {
                try!(full_local_insert(rng, txn, page, key_, value_, right.page_offset(),
                                       &mut levels, left.page_offset(), false))
            };
            try!(free(rng, txn, free_page, false));
            Ok((result, smallest))
        },
    }

}

pub fn del<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, db:&mut Db, key:&[u8], value:Option<&[u8]>)->Result<bool,Error> {

    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };

    let comp = if let Some(value) = value {
        C::KV { key: key,
                value: UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 } }
    } else {
        C::K { key:key }
    };
    unsafe {
        debug!("root: {:?}", root_page);
        match try!(delete(rng,txn, root_page, comp)) {
            (Res::Ok { page }, None) => {
                // Maybe the root is empty. Check
                let next = u16::from_le(*(page.offset(FIRST_HEAD as isize) as *const u16));
                let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                if next == NIL && next_page != 0 {
                    db.root = next_page;
                    try!(free(rng, txn, page.page_offset(), false));
                } else {
                    db.root = page.page_offset();
                }
                Ok(true)
            },
            (Res::Underfull { page, delete, merged }, None) => {
                let mut new_levels = [0;N_LEVELS];
                let page = try!(cow_pinpointing( rng, txn, page,
                                                 &delete[..],
                                                 &mut new_levels[..],
                                                 true, true, true,
                                                 merged));
                
                // If this page is empty, replace with next page.
                let next = u16::from_le(*(page.offset(FIRST_HEAD as isize) as *const u16));
                let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                if next == NIL && next_page != 0 {
                    db.root = next_page;
                    try!(free(rng, txn, page.page_offset(), false));
                } else {
                    db.root = page.page_offset();
                }
                Ok(true)
            },
            (Res::Nothing { .. }, None) => {
                Ok(false)
            },
            (x,_) => {
                db.root = try!(root_split(rng,txn,x)).page_offset();
                Ok(true)
            }
        }
    }

}

pub fn replace<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, db: &mut Db, key: &[u8], value: &[u8])->Result<(),Error> {
    try!(del(rng,txn,db,key,None));
    try!(put(rng,txn,db,key,value));
    Ok(())
}


///////////////////////////////////////////////////////////// Tests

#[test]
fn test_delete_leaf() {
    extern crate tempdir;
    extern crate rand;
    extern crate env_logger;
    use super::{Env};

    use rand::{Rng};
    let mut rng = rand::thread_rng();

    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 1000).unwrap();
    let mut txn = env.mut_txn_begin();

    let mut page = txn.alloc_page().unwrap();
    page.init();
    let mut insertions = Vec::new();
    for _ in 0..200 {
        let key_: String = rng
            .gen_ascii_chars()
            .take(20)
            .collect();
        let value_: String = rng
            .gen_ascii_chars()
            .take(20)
            .collect();
        {
            let key = key_.as_bytes();
            let value = value_.as_bytes();
            let value = if value.len() > VALUE_SIZE_THRESHOLD {
                super::put::alloc_value(&mut txn,value).unwrap()
            } else {
                UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
            };
            match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0, false) {
                Ok(Res::Ok { page:page_ }) => {
                    page = page_
                },
                Ok(Res::Underfull { page:page_, .. }) => {
                    page = page_.unwrap_mut();
                },
                Ok(Res::Nothing { page:page_ }) => {
                    page = page_.unwrap_mut()
                },
                Ok(x) => {
                    page = root_split(&mut rng, &mut txn, x).unwrap()
                },
                _ => panic!("")
            }
        }
        insertions.push((key_,value_))
    }
    insertions.sort();

    let db = Db { root_num: -1, root: page.page_offset() };
    txn.debug(&[&db], format!("/tmp/before"), false, false);
    // Delete the 10th smallest entry.
    {
        let (ref key_,ref value_) = insertions[10];
        let key = key_.as_bytes();
        let value = value_.as_bytes();
        let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
        match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }) {
            Ok((Res::Ok { page:page_, .. }, _)) => {
                page = page_
            },
            _ => panic!("")
        }
    }
    let db = Db { root_num: -1, root: page.page_offset() };
    txn.debug(&[&db], format!("/tmp/after"), false, false);
}


#[test]
fn test_delete_root() {
    extern crate tempdir;
    extern crate rand;
    extern crate env_logger;
    use super::{Env};

    use rand::{Rng};
    let mut rng = rand::thread_rng();

    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 1000).unwrap();
    let mut txn = env.mut_txn_begin();

    let mut page = txn.alloc_page().unwrap();
    page.init();
    unsafe {
        let mut insertions = Vec::new();
        for _ in 0..200 {
            //println!("i={:?}", i);
            let key_: String = rng
                .gen_ascii_chars()
                .take(20)
                .collect();
            //println!("key = {:?}", key);
            let value_: String = rng
                .gen_ascii_chars()
                .take(20)
                .collect();
            {
                let key = key_.as_bytes();
                let value = value_.as_bytes();
                let value = if value.len() > VALUE_SIZE_THRESHOLD {
                    super::put::alloc_value(&mut txn,value).unwrap()
                } else {
                    UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
                };
                match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0, false) {
                    Ok(Res::Ok { page:page_ }) => {
                        page = page_
                    },
                    Ok(Res::Underfull { page:page_, .. }) => {
                        page = page_.unwrap_mut();
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
            }
            insertions.push((key_,value_))
        }
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&[&db], format!("/tmp/before"), false, false);
        // Delete an entry in the root.
        {
            debug!("now deleting from the root page");
            let current = page.offset(0) as *mut u16;
            let next_off = u16::from_le(*(current.offset(0)));
            let next = page.offset(next_off as isize);
            let (key,value) = read_key_value(next as *const u8);
            debug!("deleting key {:?}", std::str::from_utf8(key).unwrap());
            match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }) {
                Ok((Res::Ok { page:page_, .. }, None)) => {
                    page = page_
                },
                _ => panic!("")
            }
        }
        debug!("delete done, debugging");
        
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&[&db], format!("/tmp/after"), false, false);
    }
}

#[cfg(test)]
enum Sorted {
    No, Incr, Decr
}

#[cfg(test)]
fn test_delete_all(n:usize, keysize:usize, valuesize:usize, sorted:Sorted) {
    extern crate tempdir;
    extern crate rand;
    extern crate env_logger;
    use super::{Env};

    use rand::{Rng};
    let mut rng = rand::thread_rng();

    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let env = Env::new(dir.path(), 1000).unwrap();
    let mut txn = env.mut_txn_begin();

    let mut page = txn.alloc_page().unwrap();
    page.init();
    unsafe {
        let mut insertions = Vec::new();
        for i in 0..n {
            //println!("i={:?}", i);
            let key_: String = rng
                .gen_ascii_chars()
                .take(keysize)
                .collect();
            //println!("key = {:?}", key);
            let value_: String = rng
                .gen_ascii_chars()
                .take(valuesize)
                .collect();
            {
                let key = key_.as_bytes();
                let value = value_.as_bytes();
                let value = if value.len() > VALUE_SIZE_THRESHOLD {
                    super::put::alloc_value(&mut txn,value).unwrap()
                } else {
                    UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
                };
                match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0, false).unwrap() {
                    Res::Ok { page:page_ } => {
                        page = page_
                    },
                    Res::Underfull { .. } => {
                        unreachable!()
                    },
                    Res::Nothing { page:page_ } => {
                        //println!("already present");
                        page = page_.unwrap_mut()
                    },
                    x => {
                        debug!("root split");
                        page = root_split(&mut rng, &mut txn, x).unwrap()
                    },
                }
            }
            debug!("put i = {:?}", i);
            debug!("key = {:?}", key_);

            let db = Db { root_num: -1, root: page.page_offset() };
            txn.debug(&[&db], format!("/tmp/before_{}", i), false, false);

            insertions.push((key_,value_))
        }
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&[&db], format!("/tmp/before"), false, false);

        match sorted {
            Sorted::No => {},
            Sorted::Incr => insertions.sort(),
            Sorted::Decr => {
                insertions.sort_by(|a,b| b.cmp(a))
            }
        }
        for i in 0..insertions.len() {

            let (ref key, ref value) = insertions[i];

            println!("i = {:?}", i);
            println!("key = {:?}", key);
            debug!("i = {:?}", i);
            debug!("key = {:?}", key);
            let key = key.as_bytes();
            let value = value.as_bytes();
            let value = if value.len() > VALUE_SIZE_THRESHOLD {
                super::put::alloc_value(&mut txn,value).unwrap()
            } else {
                UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
            };
            match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }).unwrap() {
                (Res::Ok { page:page_ }, None) => {
                    // If this page is empty, replace with next page.
                    let next = u16::from_le(*(page_.offset(FIRST_HEAD as isize) as *const u16));
                    let next_page = u64::from_le(*((page_.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    if next == NIL && next_page != 0 {
                        page = txn.load_cow_page(u64::from_le(*((page_.offset(FIRST_HEAD as isize) as *const u64).offset(2)))).unwrap_mut()
                    } else {
                        page = page_
                    }
                },
                (Res::Underfull { page:page_, delete, merged }, None) => {
                    println!("underfull, deleting {:?}", &delete[..]);
                    let mut new_levels = [0;N_LEVELS];
                    let page_ = cow_pinpointing(&mut rng, &mut txn, page_,
                                                &delete[..],
                                                &mut new_levels[..],
                                                true, true, true,
                                                merged).unwrap();

                    // If this page is empty, replace with next page.
                    let next = u16::from_le(*(page_.offset(FIRST_HEAD as isize) as *const u16));
                    let next_page = u64::from_le(*((page_.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    if next == NIL && next_page != 0 {
                        page = txn.load_cow_page(u64::from_le(*((page_.offset(FIRST_HEAD as isize) as *const u64).offset(2)))).unwrap_mut()
                    } else {
                        page = page_
                    }
                },
                (Res::Nothing{..},_) => unreachable!(),
                (x,_) => page = root_split(&mut rng, &mut txn, x).unwrap(),
            }
            let db = Db { root_num: -1, root: page.page_offset() };
            txn.debug(&[&db], format!("/tmp/after_{}", i), false, false);

        }
        debug!("delete done, debugging");
        
        let db = Db { root_num: -1, root: page.page_offset() };
        let mut ws = Vec::new();
        for _ in txn.iter(&db, b"", None, &mut ws) {
            panic!("Database not empty")
        }
        txn.debug(&[&db], format!("/tmp/after"), false, false);
    }
}

#[test]
fn test_delete_all_sorted_20_() {
    test_delete_all(10, 10, 20, Sorted::Incr)
}
#[test]
fn test_delete_all_decr_20_() {
    test_delete_all(20, 100, 20, Sorted::Decr)
}
#[test]
fn test_delete_all_unsorted_20_() {
    test_delete_all(20, 200, 200, Sorted::No)
}

#[test]
fn test_delete_all_sorted_200() {
    test_delete_all(200, 100, 200, Sorted::Incr)
}
#[test]
fn test_delete_all_decr_200() {
    test_delete_all(200, 100, 200, Sorted::Decr)
}
#[test]
fn test_delete_all_unsorted_200() {
    test_delete_all(200, 200, 200, Sorted::No)
}


#[test]
fn test_delete_all_large() {
    test_delete_all(200, 200, 2000, Sorted::No)
}

#[test]
fn test_delete_all_really_large() {
    test_delete_all(200, 200, 10000, Sorted::No)
}
