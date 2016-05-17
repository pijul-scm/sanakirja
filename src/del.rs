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

// Return type of the smallest (key,value).
#[derive(Debug)]
pub struct Smallest {
    pub key_ptr:*const u8,
    pub key_len:usize,
    pub value:UnsafeValue,
    pub page:u64
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
                              child_page:Cow, child_must_be_dup:bool,
                              delete:[u16;N_LEVELS], merged:u64,
                              page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("handle_underfull {:?}", page_will_be_dup);
    let mut new_levels = [0;N_LEVELS];
    unsafe {
        std::ptr::copy_nonoverlapping(levels.as_ptr(), new_levels.as_mut_ptr(), N_LEVELS)
    }

    // First try to merge with our right sibling.
    let next_offset = unsafe { u16::from_le(*(page.offset(levels[0] as isize) as *const u16)) };

    if next_offset != NIL {
        match try!(merge::merge_children_right(rng, txn, page, levels, &child_page, child_must_be_dup,
                                               &delete,
                                               merged,
                                               page_will_be_dup)) {

            Res::Nothing { page:page_ } => {
                // If we couldn't merge:
                debug!("merged failed, page={:?}, levels={:?}", page_.page_offset(), levels);
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
                    match try!(rebalance::rebalance_left(rng, txn, page_, levels, &child_page, child_must_be_dup,
                                                         forgetting, merged,
                                                         page_will_be_dup)) {
                        Res::Nothing { page:page_ } => {
                            let result = try!(rebalance::handle_failed_left_rebalancing(rng, txn, page_, levels, child_page, child_must_be_dup, delete, merged, false, page_will_be_dup));
                            // Only in this case will the page containing the smallest element be kept alive.
                            return Ok(result)
                        },
                        x => {
                            return Ok(x)
                        }
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
    // If we haven't found a solution so far, move to the previous element, and merge the child page with its left sibling.

    // Move back by one
    debug!("trying to merge to left");
    set_pred(&page, &mut new_levels);
    match try!(merge::merge_children_left(rng, txn, page, new_levels, &child_page, child_must_be_dup,
                                          &delete, merged,
                                          page_will_be_dup)) {
        Res::Nothing { page } => {
            // we couldn't merge. rebalance.
            debug!("second case of rebalancing: {:?}", child_page);
            let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
            let result = match try!(rebalance::rebalance_right(rng, txn, page, new_levels, None, &child_page,
                                                               child_must_be_dup,
                                                               forgetting, merged,
                                                               page_will_be_dup)) {
                Res::Nothing { page:page_ } => {
                    debug!("failed rebalancing");
                    // Only in this case will the page containing the smallest element be kept alive.
                    Ok(try!(rebalance::handle_failed_right_rebalancing(rng, txn, page_, new_levels, None,
                                                                       child_page, child_must_be_dup,
                                                                       delete, merged, false,
                                                                       page_will_be_dup)))
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
                                      child_must_be_dup:bool,
                                      replacement:&Smallest,
                                      delete:[u16;N_LEVELS], merged:u64,
                                      page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("handle_underfull_replace");
    // First try to merge with our right sibling.
    match try!(merge::merge_children_replace(
        rng, txn, page, levels, &child_page, child_must_be_dup,
        replacement,
        &delete, merged, page_will_be_dup)) {
        
        Res::Nothing { page:page_ } => {
            // If we couldn't merge:
            debug!("rebalancing: {:?}", levels[0]);
            let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
            match try!(rebalance::rebalance_right(rng, txn, page_, levels, Some(replacement), &child_page,
                                                  child_must_be_dup,
                                                  forgetting, merged,
                                                  page_will_be_dup)) {
                Res::Nothing { page:page_} => {
                    return rebalance::handle_failed_right_rebalancing(rng, txn, page_, levels, Some(replacement), child_page,
                                                                      child_must_be_dup,
                                                                      delete, merged, false, page_will_be_dup)
                },
                x => Ok(x)
            }
        },
        res => Ok(res)
    }
}


fn get_smallest_binding<T>(txn:&mut MutTxn<T>, mut current:u64) -> Smallest {
    loop {
        let page = txn.load_page(current);
        current = unsafe { u64::from_le(*(page.offset(FIRST_HEAD as isize + 16) as *const u64)) };
        if current == 0 {
            let (next_key,next_value) = {
                let cur_ptr = page.offset(0) as *const u16;
                let next_off = u16::from_le(unsafe { *cur_ptr });
                debug_assert!(next_off > 0 && next_off != NIL);
                let next_ptr = page.offset(next_off as isize);
                unsafe { read_key_value(next_ptr) }
            };
            return Smallest {
                key_ptr: next_key.as_ptr(),
                key_len: next_key.len(),
                value: next_value,
                page: page.page_offset()
            }
        }
    }
}


fn delete_at_internal_node<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS], page_will_be_dup:bool) -> Result<Res,Error> {
    debug!("delete_at_internal_node {:?}", page);
    // Not found below, but we can delete something here.

    // Find the matching element, and the page to its right.
    let next_off = unsafe { u16::from_le(*(page.offset(levels[0] as isize) as *const u16)) };
    let next = page.offset(next_off as isize);
    let child_page = unsafe { u64::from_le(*((next as *const u64).offset(2))) };
    let child_page = txn.load_cow_page(child_page);

    // First get the smallest binding, replace here.
    let smallest = get_smallest_binding(txn, child_page.page_offset());
    debug!("protecting {:?}", smallest.page);
    let mut protected_index = 0;
    if txn.protected_pages[0] != 0 {
        protected_index = 1
    }
    txn.protected_pages[protected_index] = smallest.page;
    txn.free_protected[protected_index] = false;


    {
        let key = unsafe { std::slice::from_raw_parts(smallest.key_ptr, smallest.key_len) };
        debug!("smallest: {:?} {:?}", std::str::from_utf8(key), smallest.page);
    }
    let result = match try!(delete(rng,txn, child_page, C::Smallest, page_will_be_dup)) {
        Res::Ok { page: child_page } => {
            debug!("internal: ok");
            // Set the child page here, regardless of whether a merge is coming after this.
            debug!("not underfull");

            let smallest_key = unsafe { std::slice::from_raw_parts(smallest.key_ptr, smallest.key_len) };
            let size = record_size(smallest.key_len, smallest.value.len() as usize);

            let (key,value) = unsafe { read_key_value(next) };
            let deleted_size = record_size(key.len(), value.len() as usize);
            let result = if (page.occupied() + size) - deleted_size <= PAGE_SIZE as u16 {
                let mut new_levels = [0;N_LEVELS];
                let mut page =
                    if page_will_be_dup {
                        debug!("copying");
                        try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, false, 0, true))
                    } else {
                        let off = page.can_alloc(size);
                        debug!("off = {:?}", off);
                        if off > 0 && off + size <= PAGE_SIZE as u16 {
                            debug!("pinpointing, levels[0]={:?}", levels[0]);
                            try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, true, 0))
                        } else {
                            debug!("compacting");
                            try!(cow_pinpointing(rng, txn, page.as_nonmut(), &levels, &mut new_levels, true, true, 0))
                        }
                    };
                let off = page.can_alloc(size);
                debug!("off = {:?}, size={:?}", off, size);
                debug_assert!(off + size <= PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, smallest_key, smallest.value, child_page.page_offset(), off, size, &mut new_levels);
                Res::Ok { page:page }

            } else {
                // split page.
                // Decrement value, except if the page is duplicated
                if !page_will_be_dup {
                    if let UnsafeValue::O { offset, len } = value {
                        try!(free_value(rng, txn, offset, len))
                    }
                }
                unsafe {
                    try!(split_page(rng, txn, &page,
                                    smallest_key, smallest.value, child_page.page_offset(),
                                    page_will_be_dup, next_off,
                                    NIL, 0))
                }
            };
            Ok(result)
        },
        Res::Underfull { page: child_page, delete, merged, must_be_dup, .. } => {

            if !page_will_be_dup {
                let (_,value) = unsafe { read_key_value(next) };
                if let UnsafeValue::O { offset, len } = value {
                    try!(free_value(rng, txn, offset, len))
                }
            }

            debug!("internal: underfull");
            handle_underfull_replace(rng, txn, page, levels, child_page,
                                     must_be_dup,
                                     &smallest, delete, merged,
                                     page_will_be_dup)
        },
        Res::Split { key_len,key_ptr,value, left, right, free_page } => {

            debug!("internal: split");
            let middle_key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
            let middle_size = record_size(key_len, value.len() as usize);

            let smallest_key = unsafe { std::slice::from_raw_parts(smallest.key_ptr, smallest.key_len) };
            let smallest_size = record_size(smallest.key_len, smallest.value.len() as usize);

            // We need to insert middle_key -> right and smallest_key -> left to the page.
            let deleted_size = unsafe {
                let (key,value) = read_key_value(next);

                if !page_will_be_dup {
                    if let UnsafeValue::O { offset, len } = value {
                        try!(free_value(rng, txn, offset, len))
                    }
                }

                record_size(key.len(), value.len() as usize)
            };

            let result = if (page.occupied() + middle_size + smallest_size) - deleted_size <= PAGE_SIZE as u16 {

                let mut new_levels = [0;N_LEVELS];
                // Delete the current element.
                let mut page = if page_will_be_dup {
                    try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, false, 0, true))
                } else {
                    if page.first_free() + middle_size + smallest_size <= PAGE_SIZE as u16 {
                        try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, false, 0))
                    } else {
                        try!(cow_pinpointing(rng, txn, page.as_nonmut(), &levels, &mut new_levels, true, false, 0))
                    }
                };
                // Reinsert the left page with the smallest key.
                let middle_off = page.can_alloc(middle_size);
                debug_assert!(middle_off + middle_size <= PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, middle_key, value, right.page_offset(), middle_off, middle_size, &mut new_levels);

                let smallest_off = page.can_alloc(smallest_size);
                debug_assert!(smallest_off + smallest_size <= PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, smallest_key, smallest.value, left.page_offset(), smallest_off, smallest_size, &mut new_levels);

                Ok(Res::Ok { page:page })
            } else {
                // split.
                unsafe {
                    split_page(rng, txn, &page,
                               middle_key, value, right.page_offset(),
                               page_will_be_dup, NIL,
                               levels[0], left.page_offset())
                }
            };
            if !page_will_be_dup && free_page > 0 {
                try!(free(rng, txn, free_page));
            } else {
                // incrementing value: already done in split_page
                /*
                if let UnsafeValue::O { offset, .. } = value {
                    try!(incr_rc(rng, txn, offset))
                }
                 */
            }
            result
        },
        Res::Nothing { .. } => {
            if cfg!(debug_assertions) {
                panic!("Child page {:?} was empty when trying to remove its smallest element.", page)
            } else {
                unreachable!()
            }
        }
    };
    debug!("protected: {:?}", txn.protected_pages);
    if txn.free_protected[protected_index] {
        debug!("freeing previously protected {:?}", smallest.page);
        unsafe { super::transaction::free(&mut txn.txn, smallest.page) }
    }
    txn.protected_pages[protected_index] = 0;
    txn.free_protected[protected_index] = false;
    result
}


fn delete<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, comp:C,
                    parent_will_be_dup:bool) -> Result<Res, Error> {

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
    let page_rc = get_rc(txn, page.page_offset());
    let this_will_be_dup = parent_will_be_dup || (page_rc > 1);
    debug!("needs_dup={:?} {:?}", parent_will_be_dup, page_rc);

    // If the reference count of the current page is n > 1, we need to
    // decrement it, as it will no longer be referenced from its
    // current reference.

    let del = if child_page > 0 {
        let next_page = txn.load_cow_page(child_page);
        Some(try!(delete(rng, txn, next_page, comp, this_will_be_dup)))
    } else {
        None
    };
    match del {
        None if eq => {
            debug!("deleting here, rc={:?}", page_rc);
            let (next_key,next_value) = {
                let cur_ptr = page.offset(levels[0] as isize) as *const u16;
                let next_off = u16::from_le(unsafe { *cur_ptr });
                debug_assert!(next_off > 0 && next_off != NIL);
                let next_ptr = page.offset(next_off as isize);
                unsafe { read_key_value(next_ptr) }
            };
            let deleted_size = record_size(next_key.len(), next_value.len() as usize);

            let will_be_underfull = page.occupied() - deleted_size < (PAGE_SIZE as u16)/2;

            debug!("will_be_underfull = {:?} {:?}", will_be_underfull, levels);
            if will_be_underfull {
                if let UnsafeValue::O { offset, len } = next_value {
                    if let C::Smallest = comp {
                        if this_will_be_dup {
                            debug!("incr_rc");
                            try!(incr_rc(rng, txn, offset))
                        }
                    } else {
                        if !this_will_be_dup {
                            debug!("free_value");
                            try!(free_value(rng, txn, offset, len))
                        }
                    }
                }
                Ok(Res::Underfull { page:page, delete: levels, merged:0, must_be_dup: page_rc > 1 })
            } else {
                let mut new_levels = [0;N_LEVELS];

                if !parent_will_be_dup && page_rc > 1 {
                    // The parent contained a pointer to this page,
                    // which will be dropped since the parent is not duplicated.
                    try!(decr_rc(rng, txn, page.page_offset()))
                }

                let page =
                    if this_will_be_dup {
                        // After this page is copied, if we're in case
                        // C::Smallest, there will be one more
                        // reference to the value.
                        match (comp,next_value) {
                            (C::Smallest, UnsafeValue::O { offset, .. }) => {
                                try!(incr_rc(rng, txn, offset));
                            },
                            _ => { }
                        }
                        // Never free the value here.
                        try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, false, 0, true))
                    } else {
                        let free_value = match comp {
                            C::Smallest => false,
                            _ => true
                        };
                        try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, free_value, 0))
                    };
                debug!("page={:?}", page);
                Ok(Res::Ok { page:page })
            }
        },
        Some(Res::Nothing { .. }) if eq => {
            // Find smallest, etc.
            let page_offset = page.page_offset();
            let result = try!(delete_at_internal_node(rng, txn, page, levels, this_will_be_dup));
            match result {
                Res::Underfull { .. } => {
                    // This case will be handled by the parent.
                },
                _ if page_rc > 1 && !parent_will_be_dup => // decrease the RC of the first page on the path referenced at least twice.
                    try!(decr_rc(rng, txn, page_offset)),
                _ => {}
            }
            Ok(result)

        },
        Some(Res::Underfull { page:child_page, delete, merged, must_be_dup }) => {

            debug!("delete: underfull {:?}", child_page);
            let page_offset = page.page_offset();
            let result = try!(handle_underfull(rng, txn, page, levels, child_page,
                                               must_be_dup,
                                               delete, merged,
                                               this_will_be_dup));
            match result {
                Res::Underfull { .. } => {
                    // This case will be handled by the parent.
                },
                _ if page_rc > 1 && !parent_will_be_dup => // decrease the RC of the first page on the path referenced at least twice.
                    try!(decr_rc(rng, txn, page_offset)),
                _ => {}
            }
            Ok(result)
        },
        Some(Res::Ok { page:child_page }) => {
            debug!("ok, back to page {:?} with child {:?}", page.page_offset(), child_page.page_offset());
            if page_rc > 1 && !parent_will_be_dup {
                // decrease the RC of the first page on the path referenced at least twice.
                try!(decr_rc(rng, txn, page.page_offset()))
            }
            // Update the pointer here
            let mut new_levels = [0;N_LEVELS];
            let page =
                if this_will_be_dup {
                    try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels,
                                   false, false, child_page.page_offset(), true))
                } else {
                    try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false, false, child_page.page_offset()))
                };
            Ok(Res::Ok { page:page })
        },
        Some(Res::Nothing {.. }) | None => {
            Ok(Res::Nothing { page:page })
        },

        Some(Res::Split { key_ptr,key_len,value:value_,left,right,free_page }) => {
            // Now reinsert the element here.
            if page_rc > 1 && !parent_will_be_dup {
                // decrease the RC of the first page on the path referenced at least twice.
                try!(decr_rc(rng, txn, page.page_offset()))
            }
            let key_ = unsafe {std::slice::from_raw_parts(key_ptr, key_len)};
            let result = unsafe {
                try!(full_local_insert(rng, txn, page, key_, value_, right.page_offset(), &mut levels, left.page_offset(),
                                       parent_will_be_dup,
                                       this_will_be_dup))
            };
            if !this_will_be_dup && free_page > 0 {
                try!(free(rng, txn, free_page));
            } else {
                // incrementing value: already done in split_page
                /*if let UnsafeValue::O { offset, .. } = value_ {
                    try!(incr_rc(rng, txn, offset))
                }*/
            }
            Ok(result)
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
        match try!(delete(rng,txn, root_page, comp, false)) {
            Res::Ok { page } => {
                // Maybe the root is empty. Check
                let next = u16::from_le(*(page.offset(FIRST_HEAD as isize) as *const u16));
                let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                if next == NIL && next_page != 0 {
                    db.root = next_page;
                    try!(free(rng, txn, page.page_offset()));
                } else {
                    db.root = page.page_offset();
                }
                Ok(true)
            },
            Res::Underfull { page, delete, merged, must_be_dup } => {
                let mut new_levels = [0;N_LEVELS];

                debug!("del: must_be_dup = {:?}", must_be_dup);
                let page =
                    if must_be_dup {
                        try!(decr_rc(rng, txn, page.page_offset()));
                        try!(copy_page( rng, txn, &page.as_page(),
                                        &delete,
                                        &mut new_levels,
                                        true, false, merged, true))
                    } else {
                        try!(cow_pinpointing( rng, txn, page,
                                              &delete[..],
                                              &mut new_levels[..],
                                              true, false,
                                              merged))
                    };
                
                // If this page is empty, replace with next page.
                let next = u16::from_le(*(page.offset(FIRST_HEAD as isize) as *const u16));
                let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                if next == NIL && next_page != 0 {
                    db.root = next_page;
                    try!(free(rng, txn, page.page_offset()));
                } else {
                    db.root = page.page_offset();
                }
                Ok(true)
            },
            Res::Nothing { .. } => {
                Ok(false)
            },
            x => {
                debug!("root split");
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


fn drop_page<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, page:u64)->Result<(),Error> {
    let mut rc = if let Some(rc) = txn.rc() { rc } else { try!(txn.create_db()) };
    let count = txn.get_u64(&rc, page).unwrap_or(1);
    if count > 1 {
        if count == 2 {
            try!(txn.del_u64(rng, &mut rc, page));
        } else {
            try!(txn.replace_u64(rng, &mut rc, page, count-1));
        }
    } else {
        let page = txn.load_page(page);
        for (_ , _, value, r) in PageIterator::new(&page,0) {
            if let UnsafeValue::O { offset, len } = value {
                try!(free_value(rng, txn, offset, len))
            }
            try!(drop_page(rng, txn, r))
        }
        unsafe {
            super::transaction::free(&mut txn.txn, page.page_offset())
        }
    }
    Ok(())
}


pub fn drop<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, db: Db)->Result<(),Error> {
    drop_page(rng, txn, db.root)
}

pub fn clear<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, db: &mut Db)->Result<(),Error> {
    if get_rc(txn, db.root) > 1 {
        decr_rc(rng, txn, db.root)
    } else {
        let page = txn.load_cow_page(db.root);
        for (_ , _, value, r) in PageIterator::new(&page,0) {
            if let UnsafeValue::O { offset, len } = value {
                try!(free_value(rng, txn, offset, len))
            }
            try!(drop_page(rng, txn, r))
        }
        match page.cow {
            super::transaction::Cow::Page(p0) => {
                unsafe { super::transaction::free(&mut txn.txn, p0.offset) }
                db.root = try!(txn.alloc_page()).page_offset();
            }
            super::transaction::Cow::MutPage(p0) => {
                (MutPage { page:p0 }).init()
            }
        }
        Ok(())
    }
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
    let tmp = tempdir::TempDir::new("pijul").unwrap();
    {
        let tmp_path = tmp.path();
        debug!("tmp_path: {:?}", tmp_path);
        let env = Env::new(dir.path(), 1000).unwrap();
        let mut txn = env.mut_txn_begin().unwrap();

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
        txn.debug(&[&db], tmp_path.join("before"), false, false);
        // Delete the 10th smallest entry.
        {
            let (ref key_,ref value_) = insertions[10];
            let key = key_.as_bytes();
            let value = value_.as_bytes();
            let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
            match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }, false) {
                Ok(Res::Ok { page:page_, .. }) => {
                    page = page_
                },
                _ => panic!("")
            }
        }
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&[&db], tmp_path.join("after"), false, false);
        println!("tmp: {:?}", tmp_path);
    }
    std::mem::forget(tmp);
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
    let mut txn = env.mut_txn_begin().unwrap();

    let mut page = txn.alloc_page().unwrap();
    page.init();
    let tmp = tempdir::TempDir::new("pijul").unwrap();
    unsafe {
        let tmp_path = tmp.path();
        debug!("tmp_path: {:?}", tmp_path);
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
        txn.debug(&[&db], tmp_path.join("before"), false, false);
        // Delete an entry in the root.
        {
            debug!("now deleting from the root page");
            let current = page.offset(0) as *mut u16;
            let next_off = u16::from_le(*(current.offset(0)));
            let next = page.offset(next_off as isize);
            let (key,value) = read_key_value(next as *const u8);
            debug!("deleting key {:?}", std::str::from_utf8(key).unwrap());
            match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }, false) {
                Ok(Res::Ok { page:page_, .. }) => {
                    page = page_
                },
                _ => panic!("")
            }
        }
        debug!("delete done, debugging");
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&[&db], tmp_path.join("after"), false, false);
    }
    std::mem::forget(tmp);
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
    let mut txn = env.mut_txn_begin().unwrap();

    let mut page = txn.alloc_page().unwrap();
    page.init();
    let tmp = tempdir::TempDir::new("pijul").unwrap();
    let tmp_path = tmp.path().to_path_buf();
    std::mem::forget(tmp);
    unsafe {
        debug!("tmp_path: {:?}", tmp_path);
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
            let value = {
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
                value
            };
            debug!("put i = {:?}", i);
            debug!("key = {:?}", key_);

            let db = Db { root_num: -1, root: page.page_offset() };
            txn.debug(&[&db], (&tmp_path).join(format!("before_{}", i)), false, false);

            insertions.push((key_,value_, value))
        }
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&[&db], (&tmp_path).join("before"), false, false);

        match sorted {
            Sorted::No => {},
            Sorted::Incr => {
                insertions.sort_by(|&(ref a,_,_),&(ref b,_,_)| a.cmp(b))
            },
            Sorted::Decr => {
                insertions.sort_by(|&(ref a,_,_),&(ref b,_,_)| b.cmp(a))
            }
        }
        for i in 0..insertions.len() {

            let (ref key, ref value, ref val) = insertions[i];

            println!("i = {:?}", i);
            println!("key = {:?}", key);
            debug!("i = {:?}", i);
            debug!("key = {:?}", key);
            debug!("allocated = {:?}", val);
            let key = key.as_bytes();
            let value = value.as_bytes();
            let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
            match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }, false).unwrap() {
                Res::Ok { page:page_ } => {
                    // If this page is empty, replace with next page.
                    let next = u16::from_le(*(page_.offset(FIRST_HEAD as isize) as *const u16));
                    let next_page = u64::from_le(*((page_.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    if next == NIL && next_page != 0 {
                        page = txn.load_cow_page(u64::from_le(*((page_.offset(FIRST_HEAD as isize) as *const u64).offset(2)))).unwrap_mut()
                    } else {
                        page = page_
                    }
                },
                Res::Underfull { page:page_, delete, merged, .. } => {
                    println!("underfull, deleting {:?}", &delete[..]);
                    let mut new_levels = [0;N_LEVELS];
                    let page_ = cow_pinpointing(&mut rng, &mut txn, page_,
                                                &delete[..],
                                                &mut new_levels[..],
                                                true, false,
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
                Res::Nothing{..} => unreachable!(),
                x => page = root_split(&mut rng, &mut txn, x).unwrap(),
            }
            let db = Db { root_num: -1, root: page.page_offset() };
            txn.debug(&[&db], (&tmp_path).join(format!("after_{}", i)), false, false);
        }
        debug!("delete done, debugging");
        
        let db = Db { root_num: -1, root: page.page_offset() };
        for _ in txn.iter(&db, b"", None) {
            panic!("Database not empty")
        }
        //txn.debug(&[&db], format!("/tmp/after"), false, false);
    }
}

#[test]
fn test_delete_all_sorted_20_() {
    test_delete_all(20, 10, 20, Sorted::Incr)
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
fn test_delete_all_unsorted_1000() {
    test_delete_all(800, 200, 200, Sorted::No)
}


#[test]
fn test_delete_all_large() {
    test_delete_all(200, 200, 2000, Sorted::No)
}

#[test]
fn test_delete_all_really_large() {
    test_delete_all(200, 200, 10000, Sorted::No)
}
