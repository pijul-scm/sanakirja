use super::txn::*;
use super::transaction::{PAGE_SIZE,Error};
use std;
use rand::{Rng};

extern crate log;
use super::put::*;

use super::del::Smallest;

/// child_page is the next element's right child.
pub fn handle_failed_right_rebalancing<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
                                                 replacement:Option<&Smallest>,
                                                 child_page:Cow,
                                                 child_must_be_dup:bool,
                                                 delete:[u16;N_LEVELS], replace_page:u64,
                                                 do_free_value:bool, page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("handle failed right rebalancing {:?} {:?}", page.page_offset(), child_page.page_offset());
    // Actually delete and replace in the child.
    let child_page_offset = child_page.page_offset();
    let new_child_page = {
        let mut new_delete = [0;N_LEVELS];
        if page_will_be_dup || child_must_be_dup {
            try!(copy_page(rng, txn, &child_page.as_page(), &delete, &mut new_delete, true, do_free_value, 0, true))
        } else {
            try!(cow_pinpointing(rng, txn, child_page,
                                 &delete,
                                 &mut new_delete,
                                 true, do_free_value, true,
                                 replace_page))
        }
    };
    debug!("new_child_page: {:?}", new_child_page.page_offset());
    if child_must_be_dup && !page_will_be_dup {
        try!(decr_rc(rng, txn, child_page_offset))
    }
    if let Some(repl) = replacement {
        let mut new_levels = [0;N_LEVELS];
        // Delete the next element on this page.
        let mut page =
            if page_will_be_dup {
                try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, true, 0, true))
            } else {
                try!(cow_pinpointing(rng, txn, page,
                                     &levels,
                                     &mut new_levels,
                                     true, true, true,
                                     0))
            };
        // Reinsert the replacement.
        let key = unsafe { std::slice::from_raw_parts(repl.key_ptr, repl.key_len) };
        let size = record_size(key.len(), repl.value.len() as usize);
        let off = page.can_alloc(size);
        unsafe {
            local_insert_at(rng, &mut page, key, repl.value, new_child_page.page_offset(), off, size, &mut new_levels)
        }
        /*if repl.needs_freeing {
            try!(free(rng, txn, repl.free_page, false))
        }*/
        Ok(Res::Ok { page:page })
    } else {
        let mut new_levels = [0;N_LEVELS];
        let page = if page_will_be_dup {
            try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, false, false, 0, true))
        } else {
            try!(cow_pinpointing(rng, txn, page,
                                 &levels,
                                 &mut new_levels,
                                 false, false, true,
                                 0))
        };
        let next = u16::from_le(unsafe { *(page.offset(new_levels[0] as isize) as *const u16) });
        unsafe { *((page.offset(next as isize) as *mut u64).offset(2)) = new_child_page.page_offset().to_le() }
        Ok(Res::Ok { page:page })
    }
}

/// child_page is the current element's right child.
pub fn handle_failed_left_rebalancing<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
                                                child_page:Cow,
                                                child_must_be_dup:bool,
                                                delete:[u16;N_LEVELS], replace_page:u64, do_free_value:bool,
                                                page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("handle failed left rebalancing {:?} {:?} {:?}", page.page_offset(), child_page.page_offset(), page_will_be_dup);
    // Actually delete and replace in the child.
    let child_page_offset = child_page.page_offset();
    let new_child_page = {
        let mut new_delete = [0;N_LEVELS];
        if page_will_be_dup || child_must_be_dup {
            try!(copy_page(rng, txn, &child_page.as_page(), &delete, &mut new_delete,
                           true, do_free_value, replace_page, true))
        } else {
            try!(cow_pinpointing(rng, txn, child_page,
                                 &delete,
                                 &mut new_delete,
                                 true, do_free_value, true,
                                 replace_page))
        }
    };
    debug!("new_child_page: {:?}", new_child_page.page_offset());
    let mut new_levels = [0;N_LEVELS];
    let page =
        if page_will_be_dup {
            try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, false, false,
                           new_child_page.page_offset(), true))
        } else {
            try!(cow_pinpointing(rng, txn, page,
                                 &levels[..],
                                 &mut new_levels[..],
                                 false, false, true,
                                 new_child_page.page_offset()))
        };
    if child_must_be_dup && !page_will_be_dup {
        // If the child was copied, but its parent was not duplicated, one reference is lost.
        // In all other cases, all references are preserved.
        try!(decr_rc(rng, txn, child_page_offset))
    }
    // We don't need to touch any reference counting here (they are
    // already handled in the calls to `copy_page` above).
    Ok(Res::Ok { page:page })
}


/// Take elements from the current element's right child, and move
/// them to the next element's right child, updating, and possibly
/// replacing the separator with the provided replacement.
///
/// Assumes the child page is the next element's right child.
pub fn rebalance_right<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, mut levels:[u16;N_LEVELS],
                                 replacement:Option<&Smallest>,
                                 child_page:&Cow, child_must_dup:bool,
                                 forgetting:u16, replace_page:u64, do_free_value:bool,
                                 page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("rebalance_right {:?}, levels {:?}", page.page_offset(), &levels[..]);

    // First operation: take all elements from one of the sides of the
    // merge, insert them into the other side. This might cause a split.

    // We want to delete the next element, i.e. the one after levels[0].
    let next = u16::from_le(unsafe { *(page.offset(levels[0] as isize) as *const u16) });
    debug_assert!(next!=NIL);

    // From now on, we'll call the "current" and "next" elements the
    // elements at levels[0] and the successor of levels[0],
    // regardless of whether they've been changed by the previous
    // calls.

    // Find the right child of the next element.
    let left_child = {
        let left_child = u64::from_le(unsafe { *((page.offset(levels[0] as isize) as *const u64).offset(2)) });
        txn.load_cow_page(left_child)
    };
    // Find the right child of the current element.

    // Compute the page sizes to decide what to do (merge vs. rebalance).
    let right_size = child_page.occupied();
    let left_size = left_child.occupied();
    let middle_size = {
        if let Some(repl) = replacement {
            record_size(repl.key_len, repl.value.len() as usize)
        } else {
            let (key,value) = unsafe { read_key_value(page.offset(next as isize)) };
            record_size(key.len(), value.len() as usize)
        }
    };
    let deleted_size = {
        let ptr = child_page.offset(forgetting as isize);
        let (key,value) = unsafe { read_key_value(ptr) };
        debug!("delete key: {:?}", std::str::from_utf8(key));
        record_size(key.len(), value.len() as usize)
    };
    if left_size <= right_size - deleted_size {
        return Ok(Res::Nothing { page:page })
    }

    //////////////////////////////////////////////

    let size = right_size + left_size + middle_size - deleted_size;
    debug!("sizes: {:?} {:?} {:?} sum = {:?}", right_size, left_size, middle_size, size);

    let mut new_left = try!(txn.alloc_page());
    new_left.init();
    let mut new_right = try!(txn.alloc_page());
    new_right.init();
    let mut middle = None;
    debug!("allocated {:?} and {:?}", new_left.page_offset(), new_right.page_offset());

    let left_rc = get_rc(txn, left_child.page_offset());

    unsafe {
        let left_left_child = u64::from_le(*((left_child.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
        *((new_left.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = left_left_child.to_le();
        if (page_will_be_dup || left_rc > 1) && left_left_child > 0 {
            // If both `left` and `new_left` stay alive after this
            // call, there is one more reference to left_left
            try!(incr_rc(rng, txn, left_left_child))
        }
    }


    let mut left_bytes = 24;
    let mut left_levels = [0;N_LEVELS];
    let mut right_levels = [0;N_LEVELS];

    for (_, key, value, r) in PI::new(&left_child,0) {

        let next_size = record_size(key.len(),value.len() as usize);
        if middle.is_none() {
            debug!("left_bytes = {:?} {:?} {:?}", left_bytes, size, next_size);
            // Should we insert next_size into the left page, or as the middle element?
            if left_bytes+next_size // Size of the left page if we insert it into the left page.
                <=
                (size - next_size) / 2 // Size if we use this element as the middle one.
            {
                // insert in left page.
                let off = new_left.can_alloc(next_size);
                debug_assert!(off > 0);
                debug_assert!(off + next_size <= PAGE_SIZE as u16);
                debug!("key -> left: {:?} {:?}", std::str::from_utf8(key), r);
                if (page_will_be_dup || left_rc > 1) && r > 0 {
                    try!(incr_rc(rng, txn, r))
                }
                unsafe { local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels) }
                left_bytes += next_size;
            } else {
                if (page_will_be_dup || left_rc > 1) && r > 0 {
                    try!(incr_rc(rng, txn, r))
                }
                middle = Some((key.as_ptr(),key.len(),value,r))
            }
        } else {
            // insert in right page.
            let off = new_right.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            debug!("key -> right: {:?} {:?}", std::str::from_utf8(key), r);
            if (page_will_be_dup || left_rc > 1) && r > 0 {
                try!(incr_rc(rng, txn, r))
            }
            unsafe { local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels) }
        }
    }

    debug_assert!(middle.is_some());
    {
        let right_left_child = u64::from_le(unsafe { *((child_page.offset(0) as *const u64).offset(2)) });
        debug!("right_left_child = {:?}", right_left_child);
        let (key,value) = unsafe { read_key_value(page.offset(next as isize)) };
        let (key,value) =
            if let Some(repl) = replacement {
                debug!("replacement");
                if let UnsafeValue::O { offset, len } = value {
                    try!(free_value(rng, txn, offset, len))
                }
                unsafe { (std::slice::from_raw_parts(repl.key_ptr, repl.key_len), repl.value) }
            } else {
                debug!("original");
                (key, value)
            };
        let next_size = record_size(key.len(),value.len() as usize);
        let off = new_right.can_alloc(next_size);
        debug_assert!(off > 0);
        debug_assert!(off + next_size <= PAGE_SIZE as u16);
        debug!("key -> right (middle): {:?} {:?} {:?} {:?}", std::str::from_utf8(key), right_left_child, page_will_be_dup, child_must_dup);
        if (page_will_be_dup || child_must_dup) && right_left_child > 0 {
            // If the child is still alive after this call, increment
            // the grandchild's RC
            try!(incr_rc(rng, txn, right_left_child))
        }
        unsafe { local_insert_at(rng, &mut new_right, key, value, right_left_child, off, next_size, &mut right_levels) }
    }

    let mut last_updated_ptr = new_right.offset(right_levels[0] as isize);
    let forgotten_page = unsafe {
        u64::from_le(*((child_page.offset(forgetting as isize) as *const u64).offset(2)))
    };
    debug!("forgetting:{:?}, forgotten_page:{:?}", forgetting, forgotten_page);
    for (cur, key, value, r) in PI::new(child_page,0) {
        debug!("cur:{:?}, r:{:?}", cur, r);
        if cur != forgetting {
            let next_size = record_size(key.len(),value.len() as usize);
            // insert in right page.
            let off = new_right.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            last_updated_ptr = new_right.offset(off as isize);
            debug!("key -> right: {:?} {:?}", std::str::from_utf8(key), r);

            if (page_will_be_dup || child_must_dup) && r > 0 && r != forgotten_page {
                try!(incr_rc(rng, txn, r))
            }
            unsafe {local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels) }
        } else {
            if do_free_value {
                if let UnsafeValue::O { offset, len } = value {
                    try!(free_value(rng, txn, offset, len))
                }
            }
            debug!("replacing ptr, replace_page={:?}", replace_page);
            unsafe { *((last_updated_ptr as *mut u64).offset(2)) = replace_page.to_le(); }
        }
    }

    
    let result = {
        // Delete the current entry, insert the new one instead.
        if let Some((key_ptr,key_len,value,r)) = middle {

            unsafe { *((new_right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = r.to_le(); }
            let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
            debug!("middle = {:?}", std::str::from_utf8(key));
            // The following call might split.
            unsafe {
                check_alloc_local_insert(rng, txn, page,
                                         key, value, new_left.page_offset(), new_right.page_offset(), &mut levels,
                                         page_will_be_dup)
            }
        } else {
            unreachable!()
        }
    };
    debug!("result = {:?}", result);
    //
    debug!("freeing left: {:?}", left_child.page_offset());

    if !page_will_be_dup {
        // Decrease the reference counter of the left child.
        try!(free(rng, txn, left_child.page_offset(), false));
        // Decrease the reference counter of the child.
        debug!("freeing child: {:?}", child_page.page_offset());
        try!(free(rng, txn, child_page.page_offset(), false));
    }
    /*
    if replacement_free > 0 {
        debug!("freeing replacement: {:?}", replacement_free);
        try!(free(rng, txn, replacement_free, false));
    }
     */
    result
}







/// Take elements from the right child of the element immediately
/// after the current one (where "current" is the one indicated by
/// `levels`), and move them to the right child of the current
/// element, updating the separator.
///
/// Assumes `child_page` is the current element's right child.
pub fn rebalance_left<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, mut levels:[u16;N_LEVELS],
                                child_page:&Cow, child_must_dup:bool,
                                forgetting:u16, replace_page:u64, do_free_value:bool,
                                page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("rebalance_left");

    // First operation: take all elements from one of the sides of the
    // merge, insert them into the other side. This might cause a split.

    // We want to delete the next element, i.e. the one after levels[0].
    let next = u16::from_le(unsafe { *(page.offset(levels[0] as isize) as *const u16) });
    debug_assert!(next!=NIL);

    // From now on, we'll call the "current" and "next" elements the
    // elements at levels[0] and the successor of levels[0],
    // regardless of whether they've been changed by the previous
    // calls.

    // Find the right child of the next element.
    let right_child = {
        let right_child = u64::from_le(unsafe { *((page.offset(next as isize) as *const u64).offset(2)) });
        txn.load_cow_page(right_child)
    };

    // Compute the page sizes to decide what to do (merge vs. rebalance).
    let left_size = child_page.occupied();
    let right_size = right_child.occupied();
    let middle_size = {
        let (key,value) = unsafe { read_key_value(page.offset(next as isize)) };
        record_size(key.len(), value.len() as usize)
    };
    let deleted_size = {
        let ptr = child_page.offset(forgetting as isize);
        let (key,value) = unsafe { read_key_value(ptr) };
        debug!("delete key: {:?}", std::str::from_utf8(key));
        record_size(key.len(), value.len() as usize)
    };
    if left_size - deleted_size <= right_size {
        return Ok(Res::Nothing { page:page })
    }
    let size = right_size + left_size + middle_size - deleted_size;
    debug!("sizes: {:?} {:?} {:?} sum = {:?}", right_size, left_size, middle_size, size);

    let mut new_left = try!(txn.alloc_page());
    new_left.init();
    let mut new_right = try!(txn.alloc_page());
    new_right.init();
    let mut middle = None;
    debug!("allocated {:?} and {:?}", new_left.page_offset(), new_right.page_offset());
    unsafe {
        let left_left_child = u64::from_le(*((child_page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
        *((new_left.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = left_left_child.to_le();
        if (page_will_be_dup || child_must_dup) && left_left_child > 0 {
            debug!("incr left_left {:?}", left_left_child);
            try!(incr_rc(rng, txn, left_left_child))
        }
    }

    let mut left_bytes = 24;
    let mut left_levels = [0;N_LEVELS];
    let mut right_levels = [0;N_LEVELS];

    let forgotten_page = unsafe {
        u64::from_le(*((child_page.offset(forgetting as isize) as *const u64).offset(2)))
    };
    let mut last_updated_ptr = new_left.offset(0);
    for (cur, key, value, r) in PI::new(child_page,0) {
        if cur != forgetting {
            let next_size = record_size(key.len(),value.len() as usize);
            // insert in right page.
            let off = new_left.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            last_updated_ptr = new_left.offset(off as isize);

            debug!("key -> left: {:?} {:?}", std::str::from_utf8(key), r);
            if (page_will_be_dup || child_must_dup) && r > 0 && r != forgotten_page {
                try!(incr_rc(rng, txn, r))
            }
            unsafe { local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels) };
            left_bytes += next_size;
        } else {
            if do_free_value {
                if let UnsafeValue::O { offset, len } = value {
                    try!(free_value(rng, txn, offset, len))
                }
            }
            unsafe { *((last_updated_ptr as *mut u64).offset(2)) = replace_page.to_le() }
        }
    }
    {
        let right_left_child = u64::from_le(unsafe { *((child_page.offset(0) as *const u64).offset(2)) });
        let (key,value) = unsafe { read_key_value(page.offset(next as isize)) };
        let next_size = record_size(key.len(),value.len() as usize);
        let off = new_left.can_alloc(next_size);
        debug_assert!(off > 0);
        debug_assert!(off + next_size <= PAGE_SIZE as u16);
        debug!("key -> left: {:?} {:?}", std::str::from_utf8(key), right_left_child);
        if (page_will_be_dup || child_must_dup) && right_left_child > 0 {
            debug!("incr right_left {:?}", right_left_child);
            try!(incr_rc(rng, txn, right_left_child))
        }
        unsafe { local_insert_at(rng, &mut new_left, key, value, right_left_child, off, next_size, &mut left_levels) };
        left_bytes += next_size;
    }
    let right_rc = get_rc(txn, right_child.page_offset());
    for (_, key, value, r) in PI::new(&right_child,0) {

        let next_size = record_size(key.len(),value.len() as usize);
        if middle.is_none() {
            debug!("left_bytes = {:?} {:?} {:?}", left_bytes, size, next_size);
            // Should we insert next_size into the left page, or as the middle element?
            if left_bytes+next_size // Size of the left page if we insert it into the left page.
                <=
                (size - next_size) / 2 // Size if we use this element as the middle one.
            {
                // insert in left page.
                let off = new_left.can_alloc(next_size);
                debug_assert!(off > 0);
                debug_assert!(off + next_size <= PAGE_SIZE as u16);
                debug!("key -> right: {:?} {:?}", std::str::from_utf8(key), r);
                if (page_will_be_dup || right_rc > 1) && r > 0 {
                    try!(incr_rc(rng, txn, r))
                }
                unsafe { local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels) };
                left_bytes += next_size;
            } else {
                if (page_will_be_dup || right_rc > 1) && r > 0 {
                    try!(incr_rc(rng, txn, r))
                }
                middle = Some((key.as_ptr(),key.len(),value,r))
            }
        } else {
            // insert in right page.
            let off = new_right.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            if (page_will_be_dup || right_rc > 1) && r > 0 {
                try!(incr_rc(rng, txn, r))
            }
            unsafe { local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels) };
        }
    }

    let result = {
        // Delete the current entry, insert the new one instead.
        if let Some((key_ptr,key_len,value,r)) = middle {

            unsafe { *((new_right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = r.to_le(); }
            let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
            debug!("middle = {:?}", std::str::from_utf8(key));
            // The following call might split.
            unsafe {
                check_alloc_local_insert(rng, txn, page,
                                         key, value, new_left.page_offset(), new_right.page_offset(), &mut levels,
                                         page_will_be_dup)
            }
        } else {
            unreachable!()
        }
    };
    debug!("result = {:?}", result);
    //
    debug!("freeing left: {:?}", right_child.page_offset());
    if !page_will_be_dup {
        try!(free(rng, txn, right_child.page_offset(), false));
        try!(free(rng, txn, child_page.page_offset(), false));
    }
    result
}




/// If the levels have already been found, compact or split the page
/// if necessary, and inserts the input (key, value) into the result,
/// at the input levels.
unsafe fn check_alloc_local_insert<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, key:&[u8], value:UnsafeValue, left_page: u64, right_page:u64, levels:&mut [u16], page_will_be_dup:bool) -> Result<Res, Error> {
    debug!("check_alloc_local_insert, levels {:?}, left={:?}, right={:?}", levels, left_page, right_page);
    let size = record_size(key.len(), value.len() as usize);
    let mut new_levels = [NIL;N_LEVELS];
    let off = page.can_alloc(size);
    if off > 0 {

        debug!("check_alloc_local_insert: non-split");
        let mut page =
            if page_will_be_dup {
                try!(copy_page(rng, txn, &page.as_page(), &levels, &mut new_levels, true, false, left_page, true))
            } else {
                debug_assert!(get_rc(txn, page.page_offset()) <= 1);
                if off + size < PAGE_SIZE as u16 {
                    // No need to copy nor compact the page, the value can be written right away.
                    debug!("check_alloc, no compaction, levels={:?}", levels);
                    try!(cow_pinpointing(rng, txn, page, levels, &mut new_levels, true, false, true, left_page))
                } else {
                    // Here, we need to compact the page, which is equivalent to considering it non mutable and CoW it.
                    debug!("check_alloc, compaction, levels={:?}", levels);
                    let page = try!(cow_pinpointing(rng, txn, page.as_nonmut(), levels, &mut new_levels, true, false, true, left_page));
                    let off = page.can_alloc(size);
                    page
                }
            };
        let off = page.can_alloc(size);
        debug_assert!(off+size < PAGE_SIZE as u16);
        debug!("new_levels:{:?}", new_levels);
        local_insert_at(rng, &mut page, key, value, right_page, off, size, &mut new_levels);
        std::ptr::copy_nonoverlapping(new_levels.as_ptr(), levels.as_mut_ptr(), N_LEVELS);
        Ok(Res::Ok { page:page })
    } else {
        debug!("check_alloc_local_insert: split");
        let next = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));
        Ok(try!(split_page(rng, txn, &page, key, value, right_page, page_will_be_dup, next, levels[0], left_page)))
    }
}
