use super::txn::*;
use super::transaction::{PAGE_SIZE,Error};
use std;
use rand::{Rng};

extern crate log;
use super::put::*;

use super::del::Smallest;

/// Add all bindings from `source` to `target`, assuming `target` has
/// enough free space and doesn't need compaction.
//
/// Forget offset `forgetting` during the copy, replacing its left
/// child with `replace_page`.
fn merge_page<R:Rng,T>(
    rng:&mut R,
    txn:&mut MutTxn<T>,
    source:&Cow, mut target:&mut MutPage,
    levels:&mut [u16],
    forgetting:u16, replace_page:u64, do_free_value:bool, increment_children:bool) -> Result<(),Error> {
    unsafe {
        // A pointer to the last inserted value, so we can replace the
        // deleted's left child with `replace_page`
        let mut current_ptr = target.offset(levels[0] as isize);
        // Let's go.
        for (current, key,value,r) in PI::new(source, 0) {
            debug!("merge_page: {:?} {:?} {:?} {:?}", current, std::str::from_utf8(key), r, increment_children);
            if current != forgetting {
                let size = record_size(key.len(), value.len() as usize);
                let off = target.can_alloc(size);
                debug_assert!(off > 0);
                debug_assert!(off + size <= PAGE_SIZE as u16);
                current_ptr = target.offset(off as isize);
                debug!("merge_page: off={:?}", off);
                if increment_children && r > 0 {
                    try!(incr_rc(rng, txn, r))
                }
                local_insert_at(rng, target, key, value, r, off, size, levels);
            } else {
                debug!("forget, replace with {:?}", replace_page);
                debug!("forget, freeing? {:?} {:?}", do_free_value, value);
                if do_free_value {
                    if let UnsafeValue::O { offset, len } = value {
                        try!(free_value(rng, txn, offset, len))
                    }
                }
                if replace_page > 0 {
                    *((current_ptr as *mut u64).offset(2)) = replace_page.to_le()
                }
            }
        }
    }
    Ok(())
}

/// Merge a left child into a right child, adding the separator
/// element (given as (key,value)), forgetting one value, and
/// replacing the left child of that value with `replace_page`.
fn merge_right<R:Rng,T>(
    rng:&mut R,
    txn:&mut MutTxn<T>,
    left:&Cow, right:&mut MutPage, forgetting:u16, replace_page:u64,
    key:&[u8], value:UnsafeValue, do_free_value:bool, increment_children:bool) -> Result<(), Error> {
    unsafe {
        debug!("merge right {:?} {:?} {:?}", left.page_offset(), right.page_offset(), std::str::from_utf8(key));
        // Merge the left page into the right page.
        // TODO: maybe we need to compact `right`.
        let mut levels = [0;N_LEVELS];
        let right_left_child = u64::from_le(*((right.offset(0) as *const u64).offset(2)));
        let left_left_child = *((left.offset(0) as *const u64).offset(2));
        *((right.offset(0) as *mut u64).offset(2)) = left_left_child.to_le();

        if increment_children && left_left_child > 0 {
            let first_left = u16::from_le(*(left.offset(FIRST_HEAD as isize) as *const u16));
            if first_left != forgetting {
                try!(incr_rc(rng, txn, left_left_child))
            }
        }

        try!(merge_page(rng, txn, left, right, &mut levels, forgetting, replace_page, do_free_value, increment_children));

        let size = record_size(key.len(), value.len() as usize);
        let off = right.can_alloc(size);
        debug_assert!(off + size <= PAGE_SIZE as u16);
        if increment_children && right_left_child > 0 {
            try!(incr_rc(rng, txn, right_left_child))
        }
        local_insert_at(rng, right, key, value, right_left_child, off, size, &mut levels);
    }
    Ok(())
}

/// Merge a right child into a left child, adding the separator
/// element (given as (key,value)), forgetting one value, and
/// replacing the left child of that value with `replace_page`.
fn merge_left<R:Rng,T>(
    rng:&mut R,
    txn:&mut MutTxn<T>,
    right:&Cow, left:&mut MutPage, forgetting:u16, replace_page:u64,
    key:&[u8], value:UnsafeValue, do_free_value:bool,
    increment_children:bool) -> Result<(), Error> {
    unsafe {
        debug!("merge left {:?} {:?} {:?}", right.page_offset(), left.page_offset(), std::str::from_utf8(key));
        let mut levels = [0;N_LEVELS];
        // First mission: set the levels to the last entry.
        let mut l = N_LEVELS-1;
        loop {
            loop {
                let next = u16::from_le(*((left.offset(levels[l] as isize) as *const u16).offset(l as isize)));
                if next != NIL {
                    levels[l] = next
                } else {
                    break
                }
            }
            if l == 0 {
                break
            } else {
                l-=1;
                levels[l] = levels[l+1]
            }
        }
        // Then, insert the separator, with child page the leftmost child of `right`.
        debug!("levels={:?}", levels);
        {
            let child = u64::from_le(*((right.offset(0) as *const u64).offset(2)));
            let size = record_size(key.len(), value.len() as usize);
            let off = left.can_alloc(size);
            // TODO: compact if necessary.
            debug_assert!(off + size <= PAGE_SIZE as u16);
            if increment_children && child > 0 {
                try!(incr_rc(rng, txn, child))
            }
            local_insert_at(rng, left, key, value, child, off, size, &mut levels);
        }
        // Finally, add all elements from `right` to `left`.
        // TODO: compact if necessary.
        let compact={};
        try!(merge_page(rng, txn, right, left, &mut levels, forgetting, replace_page, do_free_value, increment_children));
    }
    Ok(())
}


/// Assuming `child_page` is the right child of the binding given by
/// `levels`, merge it into its right sibling.
pub fn merge_children_right<R:Rng, T>(
    rng:&mut R, txn:&mut MutTxn<T>, page:Cow,
    levels:[u16;N_LEVELS],
    child_page:&Cow, child_will_be_dup:bool,
    delete:&[u16], merged:u64, do_free_value:bool,
    page_will_be_dup:bool) -> Result<Res, Error> {

    let next_offset = unsafe { u16::from_le(*(page.offset(levels[0] as isize) as *const u16)) };
    let next_ptr = page.offset(next_offset as isize);
    let right_sibling = txn.load_cow_page(unsafe { u64::from_le(*(next_ptr as *const u64).offset(2)) });
    debug_assert!(child_page.page_offset() != right_sibling.page_offset());
    let right_sibling_size = right_sibling.occupied();

    // Separator
    let (next_key, next_value) = unsafe { read_key_value(next_ptr) };
    let next_record_size = record_size(next_key.len(), next_value.len() as usize);

    // Size of the element deleted in `child_page`.
    let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
    let deleted_size = {
        let ptr = child_page.offset(forgetting as isize);
        let (key,value) = unsafe { read_key_value(ptr) };
        debug!("delete key: {:?}", std::str::from_utf8(key));
        record_size(key.len(), value.len() as usize)
    };
    debug!("child_page_occupied {:?} {:?}", child_page.occupied(), deleted_size);

    if right_sibling_size + child_page.occupied() - deleted_size - 24 + next_record_size <= PAGE_SIZE as u16 {
        // Merge child_page into its right sibling.

        // Check the need for compaction of the right sibling.
        let needs_compaction = {
            let extra_size =  child_page.occupied() - deleted_size - 24 + next_record_size;
            let off = right_sibling.can_alloc(extra_size);
            off+extra_size > PAGE_SIZE as u16
        };
        let merged_right_sibling = {
            let levels = [0;N_LEVELS];
            let mut new_levels = [0;N_LEVELS];
            let right_sibling_rc = get_rc(txn, right_sibling.page_offset());
            if right_sibling_rc > 1 {
                // We're not going to reference it anymore, since we need to copy it.
                try!(decr_rc(rng, txn, right_sibling.page_offset()))
            }
            let mut right_sibling =
                if page_will_be_dup || right_sibling_rc > 1 || child_will_be_dup {
                    // If another page is pointing to the right sibling, or will be (needs_dup), copy.
                    try!(copy_page(rng, txn, &right_sibling.as_page(), &levels, &mut new_levels, false, false, 0, true))
                } else {
                    // Else, just CoW.
                    try!(cow_pinpointing(rng, txn,
                                         if needs_compaction { right_sibling.as_nonmut() } else { right_sibling },
                                         &levels,
                                         &mut new_levels, false, false, true, 0))
                };
            try!(merge_right(rng, txn, &child_page, &mut right_sibling, forgetting, merged, next_key,
                             next_value, do_free_value, page_will_be_dup || right_sibling_rc > 1 || child_will_be_dup));
            right_sibling
        };

        debug!("page_will_be_dup: {:?} {:?}", child_page.page_offset(), page_will_be_dup);
        if !page_will_be_dup {
            // If the page is not duplicated, we lose one reference to
            // the child. The right sibling is unchanged, though (or
            // was already duplicated).
            try!(free(rng, txn, child_page.page_offset(), false))
        }
        // Now, delete (next_key, next_value) from the current page.
        if page.occupied() - next_record_size < (PAGE_SIZE as u16)/2 {

            // let page_rc = get_rc(txn, page.page_offset());
            Ok(Res::Underfull { page:page, delete:levels, merged:merged_right_sibling.page_offset(),
                                free_value: false,
                                must_be_dup: page_will_be_dup })

        } else {
            let mut new_levels = [0;N_LEVELS];
            let page =
                if page_will_be_dup {
                    // If there are, or will be, several pointers to the current page, copy it.
                    try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, false,
                                   merged_right_sibling.page_offset(), true))
                } else {
                    try!(cow_pinpointing(rng, txn, page, &levels,
                                         &mut new_levels, true, false, true,
                                         merged_right_sibling.page_offset()))
                };
            Ok(Res::Ok { page:page })
        }
    } else {
        Ok(Res::Nothing { page:page })
    }
}


/// Assuming `child_page` is the right child of the *next* binding, merge it into its left sibling.
pub fn merge_children_left<R:Rng, T>(
    rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
    child_page:&Cow, child_will_be_dup:bool,
    delete:&[u16], merged:u64, do_free_value:bool,
    page_will_be_dup:bool) -> Result<Res, Error> {

    debug!("merge_children_left {:?}", page_will_be_dup);
    // Load the left sibling and compute its size.
    let left_sibling = {
        let current_ptr = page.offset(levels[0] as isize);
        txn.load_cow_page(unsafe { u64::from_le(*(current_ptr as *const u64).offset(2)) })
    };
    debug_assert!(child_page.page_offset() != left_sibling.page_offset());
    debug!("left_sibling = {:?}", left_sibling);
    let left_sibling_size = left_sibling.occupied();

    // Find the separator and compute its size
    let next_offset = u16::from_le(unsafe { *(page.offset(levels[0] as isize) as *const u16) });
    let next_ptr = page.offset(next_offset as isize);
    let (next_key, next_value) = unsafe { read_key_value(next_ptr) };
    let next_record_size = record_size(next_key.len(), next_value.len() as usize);

    // Compute the size of the element deleted in `child_page`.
    let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
    let deleted_size = {
        let ptr = child_page.offset(forgetting as isize);
        let (key,value) = unsafe { read_key_value(ptr) };
        debug!("delete key: {:?}", std::str::from_utf8(key));
        record_size(key.len(), value.len() as usize)
    };
    debug!("child_page_occupied {:?} {:?}", child_page.occupied(), deleted_size);
    // If there's enough space in the left sibling, merge. Else, return Res::Nothing { .. }.
    if left_sibling_size + child_page.occupied() - deleted_size - 24 + next_record_size <= PAGE_SIZE as u16 {

        // Check the need for compaction of the right sibling.
        let needs_compaction = {
            let extra_size =  child_page.occupied() - deleted_size - 24 + next_record_size;
            let off = left_sibling.can_alloc(extra_size);
            off+extra_size > PAGE_SIZE as u16
        };

        let left_sibling_rc = get_rc(txn, left_sibling.page_offset());
        if left_sibling_rc > 1 {
            // We're not going to reference it anymore, since we need to copy it.
            try!(decr_rc(rng, txn, left_sibling.page_offset()))
        }
        let merged_left_sibling = {
            let levels = [0;N_LEVELS];
            let mut new_levels = [0;N_LEVELS];
            let mut left_sibling =
                if page_will_be_dup || left_sibling_rc > 1 {
                    try!(copy_page(rng, txn, &left_sibling.as_page(), &levels, &mut new_levels, false, false, 0, true))
                } else {
                    try!(cow_pinpointing(rng, txn,
                                         if needs_compaction { left_sibling.as_nonmut() } else { left_sibling },
                                         &levels,
                                         &mut new_levels, false, false, true, 0))
                };
            try!(merge_left(rng, txn, &child_page, &mut left_sibling, forgetting, merged, next_key, next_value,
                            do_free_value,
                            page_will_be_dup || left_sibling_rc > 1 || child_will_be_dup));
            left_sibling
        };
        debug!("page_will_be_dup: {:?} {:?}", child_page.page_offset(), page_will_be_dup);
        if !page_will_be_dup {
            // If the page is not duplicated, we lose one reference to
            // the child. The right sibling is unchanged, though (or
            // was already duplicated).
            try!(free(rng, txn, child_page.page_offset(), false))
        }

        // Now, delete (next_key, next_value) from the current page.
        if page.occupied() - next_record_size < (PAGE_SIZE as u16)/2 {
            //let page_rc = get_rc(txn, page.page_offset());
            Ok(Res::Underfull { page:page, delete:levels, merged:merged_left_sibling.page_offset(),
                                free_value: false,
                                must_be_dup: page_will_be_dup })

        } else {
            let mut new_levels = [0;N_LEVELS];
            let page =
                if page_will_be_dup {
                    // If there are, or will be, several pointers to the current page, copy.
                    try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, false,
                                   merged_left_sibling.page_offset(), true))
                } else {
                    try!(cow_pinpointing(rng, txn, page, &levels,
                                         &mut new_levels, true, false, true,
                                         merged_left_sibling.page_offset()))
                };
            Ok(Res::Ok { page:page })
        }
    } else {
        Ok(Res::Nothing { page:page })
    }
}








// Assuming we've just deleted an internal node (and thus `levels` is
// set to the element just before the deleted node), merge
// `child_page` to its left sibling if possible, and return `Res::Nothing{..}` else.
pub fn merge_children_replace<R:Rng, T>(
    rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
    child_page:&Cow, child_will_be_dup:bool,
    replacement:&Smallest,
    delete:&[u16], merged:u64,
    page_will_be_dup:bool) -> Result<Res, Error> {

    debug!("merge_children_replace");
    // Compute the sizes of (1) the left sibling, (2) the deleted
    // element on `child_page`, (3) the size of the separator and (4)
    // the size of `child_page`.

    let left_ptr = page.offset(levels[0] as isize);
    let left_sibling = txn.load_cow_page(u64::from_le(unsafe { *(left_ptr as *const u64).offset(2) }));
    // (1)
    let left_sibling_size = left_sibling.occupied();

    // (2)
    let forgetting = u16::from_le(unsafe { *(child_page.offset(delete[0] as isize) as *const u16) });
    let deleted_size = {
        let ptr = child_page.offset(forgetting as isize);
        let (key,value) = unsafe { read_key_value(ptr) };
        debug!("delete key: {:?}", std::str::from_utf8(key));
        record_size(key.len(), value.len() as usize)
    };
    // (3)
    let (next_key, next_value) = {
        let key = unsafe { std::slice::from_raw_parts(replacement.key_ptr, replacement.key_len) };
        (key, replacement.value)
    };
    debug!("replacement = {:?}",std::str::from_utf8(next_key));
    let next_record_size = record_size(next_key.len(), next_value.len() as usize);
    // (4)
    let child_page_size = child_page.occupied();

    debug!("child_page_occupied {:?} {:?}", child_page.occupied(), deleted_size);
    // If we can merge, do it. Else, return Res::Nothing { .. }.
    if left_sibling_size + child_page_size - 24 + next_record_size - deleted_size <= PAGE_SIZE as u16 {

        // Check the need for compaction of the right sibling.
        let needs_compaction = {
            let extra_size =  child_page.occupied() - deleted_size - 24 + next_record_size;
            let off = left_sibling.can_alloc(extra_size);
            off+extra_size > PAGE_SIZE as u16
        };
        let left_sibling_rc = get_rc(txn, left_sibling.page_offset());
        if left_sibling_rc > 1 {
            // We're not going to reference it anymore, since we need to copy it.
            try!(decr_rc(rng, txn, left_sibling.page_offset()))
        }
        let merged_left_sibling = {
            let levels = [0;N_LEVELS];
            let mut new_levels = [0;N_LEVELS];
            let mut left_sibling =
                if page_will_be_dup || left_sibling_rc > 1 {
                    try!(copy_page(rng, txn,
                                   &left_sibling.as_page(),
                                   &levels, &mut new_levels, false, false, 0, true))
                } else {
                    try!(cow_pinpointing(rng, txn,
                                         if needs_compaction { left_sibling.as_nonmut() } else { left_sibling },
                                         &levels, &mut new_levels, false, false, true, 0))
                };
            try!(merge_left(rng, txn, &child_page, &mut left_sibling, forgetting, merged, next_key, next_value,
                            false,
                            page_will_be_dup || left_sibling_rc > 1));
            left_sibling
        };
        // Now, delete (next_key, next_value) from the current page.
        let result = if page.occupied() - next_record_size < (PAGE_SIZE as u16)/2 {
            // If this makes the current page underfull.
            // let page_rc = get_rc(txn, page.page_offset());
            debug!("underfull");
            Ok(Res::Underfull { page:page, delete:levels, merged:merged_left_sibling.page_offset(),
                                free_value: true,
                                must_be_dup: page_will_be_dup })
        } else {
            // Else, just delete.
            debug!("not underfull");
            let mut new_levels = [0;N_LEVELS];
            let page =
                if page_will_be_dup {
                    try!(copy_page(rng, txn,
                                   &page.as_page(),
                                   &levels, &mut new_levels, true, true,
                                   merged_left_sibling.page_offset(), true))
                } else {
                    try!(cow_pinpointing(rng, txn, page, &levels,
                                         &mut new_levels, true, true, true,
                                         merged_left_sibling.page_offset()))
                };
            Ok(Res::Ok { page:page })
        };
        if !page_will_be_dup {
            try!(free(rng, txn, child_page.page_offset(), false));
        }
        result
    } else {
        Ok(Res::Nothing { page:page })
    }
}


