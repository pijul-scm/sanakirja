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
                                                 child_page:Cow, delete:[u16;N_LEVELS], replace_page:u64) -> Result<Res, Error> {
    // Actually delete and replace in the child.
    let new_child_page = {
        let mut new_delete = [0;N_LEVELS];
        try!(cow_pinpointing(rng, txn, child_page,
                             &delete[..],
                             &mut new_delete[..],
                             true, true, true,
                             replace_page))
    };
    if let Some(repl) = replacement {
        let mut new_levels = [0;N_LEVELS];
        // Delete the next element on this page.
        let mut page = try!(cow_pinpointing(rng, txn, page,
                                            &levels[..],
                                            &mut new_levels[..],
                                            true, true, true,
                                            0));
        // Reinsert the replacement.
        let key = unsafe { std::slice::from_raw_parts(repl.key_ptr, repl.key_len) };
        let size = record_size(key.len(), repl.value.len() as usize);
        let off = page.can_alloc(size);
        unsafe {
            local_insert_at(rng, &mut page, key, repl.value, new_child_page.page_offset(), off, size, &mut new_levels)
        }
        Ok(Res::Ok { page:page })
    } else {
        let mut new_levels = [0;N_LEVELS];
        let page = try!(cow_pinpointing(rng, txn, page,
                                        &levels[..],
                                        &mut new_levels[..],
                                        false, false, true,
                                        0));
        let next = u16::from_le(unsafe { *(page.offset(new_levels[0] as isize) as *const u16) });
        unsafe { *((page.offset(next as isize) as *mut u64).offset(2)) = new_child_page.page_offset().to_le() }
        Ok(Res::Ok { page:page })
    }
}

/// child_page is the current element's right child.
pub fn handle_failed_left_rebalancing<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
                                                child_page:Cow, delete:[u16;N_LEVELS], replace_page:u64) -> Result<Res, Error> {
    // Actually delete and replace in the child.
    let new_child_page = {
        let mut new_delete = [0;N_LEVELS];
        try!(cow_pinpointing(rng, txn, child_page,
                             &delete[..],
                             &mut new_delete[..],
                             true, true, true,
                             replace_page))
    };
    let mut new_levels = [0;N_LEVELS];
    let page = try!(cow_pinpointing(rng, txn, page,
                                    &levels[..],
                                    &mut new_levels[..],
                                    false, false, true,
                                    0));
    unsafe { *((page.offset(new_levels[0] as isize) as *mut u64).offset(2)) = new_child_page.page_offset().to_le() }
    Ok(Res::Ok { page:page })
}


/// Take elements from the current element's right child, and move
/// them to the next element's right child, updating, and possibly
/// replacing the separator with the provided replacement.
///
/// Assumes the child page is the next element's right child.
pub fn rebalance_right<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, mut levels:[u16;N_LEVELS],
                                 replacement:Option<&Smallest>,
                                 child_page:&Cow, forgetting:u16, replace_page:u64) -> Result<Res, Error> {
    debug!("rebalance_right");

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
    let size = right_size + left_size + middle_size - deleted_size;
    debug!("sizes: {:?} {:?} {:?} sum = {:?}", right_size, left_size, middle_size, size);

    let mut new_left = try!(txn.alloc_page());
    new_left.init();
    let mut new_right = try!(txn.alloc_page());
    new_right.init();
    let mut middle = None;
    debug!("allocated {:?} and {:?}", new_left.page_offset(), new_right.page_offset());
    // What happens here is, we can prove that the middle element
    // (the one which borrows `page` in the iterator) will be
    // copied to one of the pages, but this is because of
    // sizes. "dependent types lifetimes" would be great here,
    // but raw pointers also do the trick.
    unsafe {
        *((new_left.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) =
            *((left_child.offset(FIRST_HEAD as isize) as *const u64).offset(2));
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
                debug!("key -> left: {:?}", std::str::from_utf8(key));
                unsafe { local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels) }
                left_bytes += next_size;
            } else {
                middle = Some((key.as_ptr(),key.len(),value,r))
            }
        } else {
            // insert in right page.
            let off = new_right.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            debug!("key -> right: {:?}", std::str::from_utf8(key));
            unsafe { local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels) }
        }
    }

    debug_assert!(middle.is_some());
    {
        let right_left_child = u64::from_le(unsafe { *((child_page.offset(0) as *const u64).offset(2)) });
        let (key,value) =
            if let Some(repl) = replacement {
                debug!("replacement");
                unsafe { (std::slice::from_raw_parts(repl.key_ptr, repl.key_len), repl.value) }
            } else {
                debug!("original");
                unsafe { read_key_value(page.offset(next as isize)) }
            };
        let next_size = record_size(key.len(),value.len() as usize);
        let off = new_right.can_alloc(next_size);
        debug_assert!(off > 0);
        debug_assert!(off + next_size <= PAGE_SIZE as u16);
        debug!("key -> right (middle): {:?}", std::str::from_utf8(key));
        unsafe { local_insert_at(rng, &mut new_right, key, value, right_left_child, off, next_size, &mut right_levels) }
    }

    let mut last_updated_ptr = new_right.offset(right_levels[0] as isize);
    for (cur, key, value, r) in PI::new(child_page,0) {
        if cur != forgetting {
            let next_size = record_size(key.len(),value.len() as usize);
            // insert in right page.
            let off = new_right.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            last_updated_ptr = new_right.offset(off as isize);
            debug!("key -> right: {:?}", std::str::from_utf8(key));
            unsafe {local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels) }
        } else {
            unsafe { *((last_updated_ptr as *mut u64).offset(2)) = replace_page.to_le(); }
        }
    }

    
    let result = {
        let mut new_levels = [0;N_LEVELS];
        // Delete the current entry, insert the new one instead.
        let page = try!(cow_pinpointing(rng, txn, page, &mut levels, &mut new_levels, true, false, true, new_left.page_offset()));

        if let Some((key_ptr,key_len,value,r)) = middle {

            unsafe { *((new_right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = r.to_le(); }
            let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
            debug!("middle = {:?}", std::str::from_utf8(key));
            // The following call might split.
            unsafe {
                check_alloc_local_insert(rng, txn, Cow::from_mut_page(page),
                                         key, value, new_right.page_offset(), &mut new_levels)
            }
        } else {
            unreachable!()
        }
    };
    debug!("result = {:?}", result);
    //
    debug!("freeing left: {:?}", left_child.page_offset());
    try!(free(rng, txn, left_child.page_offset(), false));
    result
}







/// Take elements from the right child of the element immediately
/// after the current one (where "current" is the one indicated by
/// `levels`), and move them to the right child of the current
/// element, updating the separator.
///
/// Assumes `child_page` is the current element's right child.
pub fn rebalance_left<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, mut levels:[u16;N_LEVELS],
                                child_page:&Cow, forgetting:u16, replace_page:u64) -> Result<Res, Error> {
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
    // What happens here is, we can prove that the middle element
    // (the one which borrows `page` in the iterator) will be
    // copied to one of the pages, but this is because of
    // sizes. "dependent types lifetimes" would be great here,
    // but raw pointers also do the trick.
    unsafe {
        *((new_left.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) =
            *((child_page.offset(0) as *const u64).offset(2))
    }
    let mut left_bytes = 24;
    let mut left_levels = [0;N_LEVELS];
    let mut right_levels = [0;N_LEVELS];

    let mut last_updated_ptr = new_left.offset(0);
    for (cur, key, value, r) in PI::new(child_page,0) {
        if cur != forgetting {
            let next_size = record_size(key.len(),value.len() as usize);
            // insert in right page.
            let off = new_left.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            last_updated_ptr = new_left.offset(off as isize);
            unsafe { local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels) };
            left_bytes += next_size;
        } else {
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
        unsafe { local_insert_at(rng, &mut new_left, key, value, right_left_child, off, next_size, &mut left_levels) };
        left_bytes += next_size;
    }

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
                unsafe { local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels) };
                left_bytes += next_size;
            } else {
                middle = Some((key.as_ptr(),key.len(),value,r))
            }
        } else {
            // insert in right page.
            let off = new_right.can_alloc(next_size);
            debug_assert!(off > 0);
            debug_assert!(off + next_size <= PAGE_SIZE as u16);
            unsafe { local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels) };
        }
    }

    let result = {
        let mut new_levels = [0;N_LEVELS];
        // Delete the current entry, insert the new one instead.
        let page = try!(cow_pinpointing(rng, txn, page, &mut levels, &mut new_levels, true, false, true, new_left.page_offset()));

        if let Some((key_ptr,key_len,value,r)) = middle {

            unsafe { *((new_right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = r.to_le(); }
            let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
            debug!("middle = {:?}", std::str::from_utf8(key));
            // The following call might split.
            unsafe {
                check_alloc_local_insert(rng, txn, Cow::from_mut_page(page),
                                         key, value, new_right.page_offset(), &mut new_levels)
            }
        } else {
            unreachable!()
        }
    };
    debug!("result = {:?}", result);
    //
    debug!("freeing left: {:?}", right_child.page_offset());
    try!(free(rng, txn, right_child.page_offset(), false));
    result
}




/// If the levels have already been found, compact or split the page
/// if necessary, and inserts the input (key, value) into the result,
/// at the input levels.
unsafe fn check_alloc_local_insert<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, key:&[u8], value:UnsafeValue, right_page:u64, levels:&mut [u16]) -> Result<Res, Error> {

    let size = record_size(key.len(), value.len() as usize);
    let mut new_levels = [0;N_LEVELS];
    let off = page.can_alloc(size);
    if off > 0 {
        let (mut page,off) =
            if off + size < PAGE_SIZE as u16 && get_rc(txn, page.page_offset()) <= 1 {
                // No need to copy nor compact the page, the value can be written right away.
                (try!(cow_pinpointing(rng, txn, page, levels, &mut new_levels, false, false, true, 0)), off)
            } else {
                // Here, we need to compact the page, which is equivalent to considering it non mutable and CoW it.
                let page = try!(cow_pinpointing(rng, txn, page.as_nonmut(), levels, &mut new_levels, false, false, true, 0));
                let off = page.can_alloc(size);
                (page,off)
            };
        local_insert_at(rng, &mut page, key, value, right_page, off, size, &mut new_levels);
        std::ptr::copy_nonoverlapping(new_levels.as_ptr(), levels.as_mut_ptr(), N_LEVELS);
        Ok(Res::Ok { page:page })
    } else {
        Ok(try!(split_page(rng, txn, &page, key, value, right_page, NIL, 0)))
    }
}
