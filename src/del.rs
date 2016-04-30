use super::txn::*;
use super::transaction::{PAGE_SIZE,Error};
use std;
use std::cmp::Ordering;
use rand::{Rng};
use super::Transaction;

extern crate log;
use super::put::*;

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
struct Smallest {
    // smallest key
    key_ptr:*const u8,
    key_len:usize,
    // smallest of its values
    value:UnsafeValue,
    free_page: u64,
}


// There's a problem with the current merge and split.
// Requirements for a split that works in this module:
//
// - We merge a left child into a right child (adding the middle key). Two cases:
//
//   - Either it splits back, and we copy the middle key into the parent.
//     1. Allocate two new pages.
//     2. Search the pages in order, jumping to the right-hand side
//        when half the total size is reached.
//
//   - Or it doesn't split, and we're done.
//
// - Sometimes, we delete the smallest key from a leaf, and return it.
//   We need the page containing that key to stay alive until the entry is copied.




/// Move back to the predecessor of levels[0]. If levels[0] appears in
/// other lists, move back on them too.
unsafe fn set_pred(page:&Cow, levels:&mut [u16]) {
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
            let next = u16::from_le(*((page.offset(levels[l] as isize) as *const u16).offset(l as isize)));
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

/*
unsafe fn merge<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:&mut [u16], replace: Option<&Smallest>, keep_left:bool) -> Result<Res,Error> {

    // First operation: take all elements from one of the sides of the
    // merge, insert them into the other side. This might cause a split.

    // We want to delete the next element, i.e. the one after
    // levels[0]. Alternatively, if there's no such element, we'll
    // delete the current one.
    let next = {
        let next = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));
        if next == NIL {
            // If the current element has no successor (i.e. we deleted an
            // entry in the last child of a page), 
            set_pred(&page, levels);
            u16::from_le(*(page.offset(levels[0] as isize) as *const u16))
        } else {
            next
        }
    };
    // From now on, we'll call the "current" and "next" elements the
    // elements at levels[0] and the successor of levels[0],
    // regardless of whether they've been changed by the previous
    // calls.

    // Find the right child of the next element.
    let right_child = u64::from_le(*((page.offset(next as isize) as *const u64).offset(2)));
    let mut right_child = txn.load_cow_page(right_child);

    // Find the right child of the current element.
    let left_child = u64::from_le(*((page.offset(levels[0] as isize) as *const u64).offset(2)));
    let left_child = txn.load_cow_page(left_child);

    // Compute the page sizes to decide what to do (merge vs. rebalance).
    let right_size = right_child.occupied();
    let left_size = left_child.occupied();
    let middle_size = {
        if let Some(ref repl) = replace {
            record_size(repl.key_len, repl.value.len() as usize)
        } else {
            let next = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));
            let (key,value) = read_key_value(page.offset(next as isize));
            record_size(key.len(), value.len() as usize)
        }
    };
    let size = right_size + left_size + middle_size;
    debug!("sizes: {:?} {:?} {:?} sum = {:?}", right_size, left_size, middle_size, size);


    // Here, there are two cases: either there's enough space on the
    // right child to simply merge the pages, or we need to rebalance.

    if size - 24 <= PAGE_SIZE as u16 { // `- 24` because the initial header is counted twice in `size`.
    } else {
        debug!("rebalancing {:?} and {:?}", left_child.page_offset(), right_child.page_offset());
        // Rebalance. Allocate two pages, fill the first one to ceil(size/2), which is smaller than PAGE_SIZE.
        // Delete the middle element and insert it in the appropriate page.
        //
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
        {
            let left_left_child = u64::from_le(*((left_child.offset(0) as *const u64).offset(2)));
            *((new_left.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = left_left_child.to_le();
        }
        {
            let right_left_child = u64::from_le(*((right_child.offset(0) as *const u64).offset(2)));
            let it = PI::new(&left_child, 0)
                .chain((PI { page: &page, current:next, level:0 }).take(1))
                .chain(PI::new(&right_child, 0));

            let mut left_bytes = 24;
            let mut left_levels = [FIRST_HEAD;N_LEVELS];
            let mut right_levels = [FIRST_HEAD;N_LEVELS];
            for (_, key, value, r) in it {
                debug!("rebalance, inserting {:?} {:?} {:?}", std::str::from_utf8_unchecked(key), r, right_child.page_offset());
                let (key,value, r) = match replace {
                    Some(ref repl) if r == right_child.page_offset() => {


                        if let UnsafeValue::O { offset,len } = value {
                            debug!("rebalancing: I'd like to free {:?}", offset);
                            try!(free_value(rng, txn, offset, len))
                        }


                        (std::slice::from_raw_parts(repl.key_ptr, repl.key_len), repl.value, right_left_child)
                    },
                    _ if r == right_child.page_offset() => (key, value, right_left_child),
                    _ => (key,value,r)
                };
                debug!("r={:?}", r);
                let next_size = record_size(key.len(),value.len() as usize);
                if middle.is_none() {
                    if left_bytes + next_size <= size / 2 {
                        // insert in left page.
                        let off = new_left.can_alloc(next_size);
                        local_insert_at(rng, &mut new_left, key, value, r, off, next_size, &mut left_levels);
                        left_bytes += next_size;
                    } else {
                        middle = Some((key.as_ptr(),key.len(),value,r))
                    }
                } else {
                    // insert in right page.
                    let off = new_right.can_alloc(next_size);
                    local_insert_at(rng, &mut new_right, key, value, r, off, next_size, &mut right_levels);
                }
            }
        }
        let result = {
            let mut new_levels = [0;N_LEVELS];
            // Delete the current entry, insert the new one instead.
            let page = try!(cow_pinpointing(rng, txn, page, levels, &mut new_levels, true, false, true, new_left.page_offset()));

            if let Some((key_ptr,key_len,value,r)) = middle {

                *((new_right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = r.to_le();
                let key = std::slice::from_raw_parts(key_ptr, key_len);
                debug!("middle = {:?}", std::str::from_utf8_unchecked(key));
                // The following call might split.
                check_alloc_local_insert(rng, txn, Cow::from_mut_page(page),
                                         key, value, new_right.page_offset(), &mut new_levels)
            } else {
                unreachable!()
            }
        };
        debug!("result = {:?}", result);

        // We can safely free the right child.
        if cfg!(test) {
            match result {
                Ok(Res::Ok { ref page, .. }) => {
                    for (_,_,_,r) in PI::new(page, 0) {
                        assert!(r != right_child.page_offset());
                        assert!(r != left_child.page_offset());
                    }
                },
                Ok(Res::Split { ref left, ref right, .. }) => {
                    for (_,_,_,r) in PI::new(left, 0) {
                        assert!(r != right_child.page_offset());
                        assert!(r != left_child.page_offset());
                    }
                    for (_,_,_,r) in PI::new(right, 0) {
                        assert!(r != right_child.page_offset());
                        assert!(r != left_child.page_offset());
                    }
                },
                _ => {}
            }
        }
        //
        try!(free(rng, txn, right_child.page_offset(), false));
        if !keep_left {
            debug!("freeing left: {:?}", left_child.page_offset());
            try!(free(rng, txn, left_child.page_offset(), false));
        }
        result
    }
}
*/



fn merge_page<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>,
                        source:&Cow, mut target:&mut MutPage,
                        levels:&mut [u16],
                        forgetting:u16, replace_page:u64) {
    unsafe {
        // A pointer to the last inserted value, so we can replace the
        // deleted's left child with `replace_page`
        let mut current_ptr = target.offset(0);
        // Let's go.
        for (current, key,value,r) in PI::new(source, 0) {
            if current != forgetting {
                let size = record_size(key.len(), value.len() as usize);
                let off = target.can_alloc(size);
                debug_assert!(off + size <= PAGE_SIZE as u16);
                current_ptr = target.offset(off as isize);
                local_insert_at(rng, target, key, value, r, off, size, levels);
            } else if replace_page > 0 {
                *((current_ptr as *mut u64).offset(2)) = replace_page.to_le()
            }
        }
    }
}

// Merge a left child into a right child.
fn merge_right<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>,
                         left:&Cow, right:&mut MutPage, forgetting:u16, replace_page:u64,
                         key:&[u8], value:UnsafeValue) {
    unsafe {
        debug!("merge_right {:?} into {:?}", left.page_offset(), right.page_offset());
        // Merge the left page into the right page.
        // TODO: maybe we need to compact `right`.
        let mut levels = [0;N_LEVELS];
        {
            let child = u64::from_le(*((right.offset(0) as *const u64).offset(2)));
            *((right.offset(0) as *mut u64).offset(2)) = *((left.offset(0) as *const u64).offset(2));
            let size = record_size(key.len(), value.len() as usize);
            let off = right.can_alloc(size);
            debug_assert!(off + size <= PAGE_SIZE as u16);
            local_insert_at(rng, right, key, value, child, off, size, &mut levels);
        }
        merge_page(rng, txn, left, right, &mut levels, forgetting, replace_page);
    }
}

// Merge a right child into a left child.
fn merge_left<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>,
                        right:&Cow, left:&mut MutPage, forgetting:u16, replace_page:u64,
                        key:&[u8], value:UnsafeValue) {
    unsafe {
        // debug!("merging {:?} and {:?}", left_child.page_offset(), right_child.page_offset());
        // Merge the left page into the right page.
        let mut levels = [0;N_LEVELS];
        // First mission: set the levels to the last entry.
        let mut l = N_LEVELS-1;
        let mut current = 0;
        loop {
            loop {
                let next = u16::from_le(*((left.offset(current as isize) as *const u16).offset(l as isize)));
                if next != NIL {
                    current = next
                } else {
                    break
                }
            }
            if l == 0 {
                break
            } else {
                l-=1
            }
        }
        {
            let child = u64::from_le(*((right.offset(0) as *const u64).offset(2)));
            let size = record_size(key.len(), value.len() as usize);
            let off = left.can_alloc(size);
            debug_assert!(off + size <= PAGE_SIZE as u16);
            local_insert_at(rng, left, key, value, child, off, size, &mut levels);
        }
        merge_page(rng, txn, right, left, &mut levels, forgetting, replace_page);
    }
}

unsafe fn merge_children_right<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:[u16;N_LEVELS],
                                         child_page:&Cow, delete:&[u16], merged:u64) -> Result<Res, Error> {

    let next_offset = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));

    let next_ptr = page.offset(next_offset as isize);
    let next_page = txn.load_cow_page(u64::from_le(*(next_ptr as *const u64).offset(2)));
    let next_size = next_page.occupied();
    let (next_key, next_value) = read_key_value(next_ptr);
    let next_record_size = record_size(next_key.len(), next_value.len() as usize);
    if next_size + child_page.occupied() - 24 + next_record_size <= PAGE_SIZE as u16 {
        // Merge child_page into its right sibling.
        let merged_next_page = {
            let levels = [0;N_LEVELS];
            let mut new_levels = [0;N_LEVELS];
            let mut next_page = try!(cow_pinpointing(rng, txn, next_page, &levels,
                                                     &mut new_levels, false, false, true, 0));
            merge_right(rng, txn, &child_page, &mut next_page, levels[0], merged, next_key, next_value);
            next_page
        };
        // Now, delete (next_key, next_value) from the current page.
        if page.occupied() - next_record_size < (PAGE_SIZE as u16)/2 {

            Ok(Res::Underfull { page:page, delete:levels, merged:merged_next_page.page_offset() })

        } else {
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels,
                                            &mut new_levels, true, false, true,
                                            merged_next_page.page_offset()));
            Ok(Res::Ok { page:page })
        }
    } else {
        Ok(Res::Nothing { page:page })
    }
}


unsafe fn merge_children_left<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, mut levels:[u16;N_LEVELS],
                                        child_page:&Cow, delete:&[u16], merged:u64) -> Result<Res, Error> {
    // Here, either next_offset == NIL, or that page was too large to merge.
    // Try to merge or rebalance with the previous page.
    let current_off = levels[0];

    set_pred(&page, &mut levels);
    let prev_ptr = page.offset(levels[0] as isize);
    let prev_page = txn.load_cow_page(u64::from_le(*(prev_ptr as *const u64).offset(2)));
    let prev_size = prev_page.occupied();

    let current_ptr = page.offset(current_off as isize);
    let (key, value) = read_key_value(current_ptr);
    let record_size = record_size(key.len(), value.len() as usize);

    if prev_size + child_page.occupied() - 24 + record_size <= PAGE_SIZE as u16 {

        // merge
        let merged_prev_page = {
            let levels = [0;N_LEVELS];
            let mut new_levels = [0;N_LEVELS];
            let mut prev_page = try!(cow_pinpointing(rng, txn, prev_page, &levels,
                                                     &mut new_levels, false, false, true, 0));
            merge_left(rng, txn, &child_page, &mut prev_page, levels[0], merged, key, value);
            prev_page
        };

        // Now, delete (key, value) from the current page.
        if page.occupied() - record_size < (PAGE_SIZE as u16)/2 {

            Ok(Res::Underfull { page:page, delete:levels, merged:merged_prev_page.page_offset() })

        } else {
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels,
                                            &mut new_levels, true, false, true,
                                            merged_prev_page.page_offset()));
            Ok(Res::Ok { page:page })
        }
        
    } else {
        Ok(Res::Nothing { page:page })
    }
}

// Handle an Res::Underfull result from the current page.
unsafe fn handle_underfull<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, mut page:Cow, levels:[u16;N_LEVELS], child_page:Cow, delete:[u16;N_LEVELS], merged:u64) -> Result<Res, Error> {
    // First try to merge with our right sibling.
    let next_offset = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));
    if next_offset != NIL {
        match try!(merge_children_right(rng, txn, page, levels, &child_page,
                                        &delete, merged)) {

            Res::Nothing { page:page_ } => {
                // If we couldn't merge:
                if levels[0] == FIRST_HEAD {
                    // Either we're at the first binding, and
                    // there's no left sibling to try to merge
                    // with. Rebalance.
                    //
                    // TODO: really rebalance.
                    let mut new_levels = [0;N_LEVELS];
                    let page = try!(cow_pinpointing(rng, txn, page_, &levels,
                                                    &mut new_levels, false, false, true,
                                                    child_page.page_offset()));
                    return Ok(Res::Ok { page:page })
                } else {
                    // Or there's a left sibling to merge with.
                    page = page_
                }
            },
            res => return Ok(res)
        }
    }
    match try!(merge_children_left(rng, txn, page, levels, &child_page,
                                   &delete, merged)) {
        Res::Nothing { page } => {
            // we couldn't merge. rebalance.
            // TODO: really rebalance, what follows just updates the reference.
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels,
                                            &mut new_levels, false, false, true,
                                            child_page.page_offset()));
            Ok(Res::Ok { page:page })
        },
        res => Ok(res)
    }
}


unsafe fn delete<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, comp:C) -> Result<(Res,Option<Smallest>), Error> {

    debug!("delete = {:?}", page);
    let mut levels:[u16;N_LEVELS] = [FIRST_HEAD;N_LEVELS];
    let mut eq = false;
    match comp {
        C::KV { key, value } => set_levels(txn, &page, key, Some(value), &mut levels, &mut eq),
        C::K { key } => set_levels(txn, &page, key, None, &mut levels, &mut eq),
        C::Smallest => {eq = true} // Levels are all 0, fine!
    }
    let child_page = u64::from_le(*((page.offset(levels[0] as isize) as *const u64).offset(2)));
    debug!("next_page = {:?}", child_page);
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
                let next_off = u16::from_le(*cur_ptr);
                debug_assert!(next_off > 0 && next_off != NIL);
                let next_ptr = page.offset(next_off as isize);
                read_key_value(next_ptr)
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
                    let mut page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false, false, false, 0));
                    Ok((Res::Ok { page:page }, smallest))
                }
            } else {
                println!("{:?} {:?}", will_be_underfull, levels);
                if will_be_underfull {
                    Ok((Res::Underfull { page:page, delete: levels, merged:0 }, None))
                } else {
                    let mut new_levels = [0;N_LEVELS];
                    let mut page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, true, true, true, 0));
                    Ok((Res::Ok { page:page }, None))
                }
            }
        },
        Some((Res::Nothing { .. }, _)) if eq => {
            // Find smallest, etc.
            unimplemented!()
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
            Ok((try!(handle_underfull(rng, txn, page, levels, child_page, delete, merged)), smallest))
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

    /*

    // Here, "comp" is smaller than or equal to the (key,value) at
    // offset "next", and strictly larger than the (key,value) at
    // offset "current".
    // First delete in the page below.
    // Then delete in the current page, depending on the results.
    match del {
        None if eq => {
            debug!("none + eq");
            // No page below, but we can delete something here.
        },
        Some((Res::Nothing { .. }, _)) if eq => {
            debug!("nothing + eq");
            // Not found below, but we can delete something here.

            // Find the matching element, and the page to its right.
            let next_off = {
                let current = page.offset(levels[0] as isize) as *mut u16;
                u16::from_le(*current)
            };
            let next = page.offset(next_off as isize);
            let child_page = u64::from_le(*((next as *const u64).offset(2)));
            let child_page = txn.load_cow_page(child_page);

            // Delete the smallest element of the current binding's right child.
            debug!("delete smallest, levels = {:?}", &levels[..]);
            match try!(delete(rng,txn, child_page, C::Smallest)) {
                (Res::Underfull { page: child_page }, Some(smallest)) => {
                    // TODO: here, there's a chance we need to
                    // merge at the level above, and yet we're
                    // rewriting this whole page.

                    // Solution: merge should not write anything on this page, but return whatever it would write.
                    
                    let mut new_levels = [0;N_LEVELS];
                    let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false, false, 0));
                    debug!("underfull");
                    let next_off = {
                        let current = page.offset(new_levels[0] as isize) as *mut u16;
                        u16::from_le(*current)
                    };
                    write_right_child(&page, next_off, child_page.page_offset());

                    let result = try!(merge(rng, txn, Cow::from_mut_page(page), &mut new_levels,
                                            Some(&smallest), false));

                    // When `page` is only one level above the
                    // leaves, and the current entry is the last
                    // one on the page, merge might have merged
                    // the right child onto the left child (instead
                    // of the opposite in all other cases).
                    if smallest.free_page > 0 && smallest.free_page != child_page.page_offset() {
                        debug!("merged, freeing {:?}, child_page = {:?}", smallest.free_page, child_page.page_offset());
                        try!(free(rng, txn, smallest.free_page, false))
                    }
                    Ok((result,None))
                },
                (Res::Ok { page: child_page }, Some(smallest)) => {

                    // Set the child page here, regardless of whether a merge is coming after this.
                    debug!("not underfull");
                    let mut new_levels = [0;N_LEVELS];
                    let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false, false, 0));
                    let mut new_new_levels = [0;N_LEVELS];
                    let result = try!(replace_with_smallest(
                        rng, txn, Cow::from_mut_page(page),
                        &new_levels[..], &mut new_new_levels[..], true,
                        child_page.page_offset(), &smallest
                    ));

                    if smallest.free_page > 0 {
                        try!(free(rng, txn, smallest.free_page, false));
                    }

                    Ok((result, None))
                },
                (Res::Split { key_len,key_ptr,value, left, right, free_page }, Some(smallest)) => {
                    // Here, reinsert smallest + left, and key + right.
                    // If smallest+left doesn't cause the current page to split, fine.
                    // If it causes the page to split, then must be space for smallest on each of the pages.
                    let mut new_levels = [0;N_LEVELS];
                    let key = std::slice::from_raw_parts(key_ptr,key_len);
                    let result = try!(replace_with_smallest(rng, txn, page, &levels[..], &mut new_levels[..],
                                                            false, left.page_offset(), &smallest));
                    let r = insert_in_res(rng, txn, result, &levels[..], &mut new_levels[..], key, value, right.page_offset());
                    try!(free(rng, txn, free_page, false));
                    r
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
        },
        Some((Res::Ok { page:child_page }, smallest)) => {
            debug!("ok, back to page {:?} with child {:?}", page.page_offset(), child_page.page_offset());
            // Update the pointer here
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false, false, child_page.page_offset()));
            Ok((Res::Ok { page:page }, smallest))
        },
        Some((Res::Underfull { page:child_page }, smallest)) => {
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false, false, child_page.page_offset()));
        
            // *((page.offset(new_levels[0] as isize) as *mut u64).offset(2)) = child_page.page_offset().to_le();
            let keep_left = if let Some(ref smallest)=smallest {
                smallest.free_page == child_page.page_offset()
            } else {
                false
            };
            Ok((try!(merge(rng, txn, Cow::from_mut_page(page), &mut new_levels, None, keep_left)), smallest))
        },
        Some((Res::Nothing {.. },_)) | None => {
            debug!("nothing or none + not eq");
            // No page below, or not found below, and not found here.
            Ok((Res::Nothing { page:page }, None))
        },
        Some((Res::Split { key_ptr,key_len,value:value_,left,right,free_page }, smallest)) => {
            debug!("split");
            // An entry was deleted from the page below, causing that page to split.

            // Now reinsert the element here.
            let size = record_size(key_len, value_.len() as usize);
            let mut new_levels = [0;N_LEVELS];
            let key_ = std::slice::from_raw_parts(key_ptr, key_len);
            let result = match try!(can_alloc_and_compact(rng,txn,page,size,&levels[..], &mut new_levels[..])) {
                Alloc::Can(mut page, off) => {
                    
                    // Either we can allocate the value here, in which case we use the updated levels to inserted it.
                    write_right_child(&page, new_levels[0], left.page_offset());
                    debug_assert!(off+size < PAGE_SIZE as u16);
                    local_insert_at(rng, &mut page, key_, value_, right.page_offset(), off, size, &mut new_levels[..]);
                    Ok((Res::Ok { page:page }, smallest))
                },
                Alloc::Cannot(page) => {
                    // Or we cannot, which means that the page needs to split.
                    Ok((try!(split_page(rng, txn, &page, key_, value_,
                                        right.page_offset(), levels[0], left.page_offset())),
                        smallest))
                }
            };
            debug!("split, freeing {:?}", free_page);
            try!(free(rng, txn, free_page, false));
            result
        },
    }
*/
}


/// Adjusts the pointers on a page to skip the next value. if argument
/// `value_must_be_freed` is `true`, also free the large values pages
/// referenced from this page.
unsafe fn local_delete_at(page:&mut MutPage, levels:&[u16], value_must_be_freed:bool) -> Result<bool,Error> {
    let mut page_becomes_underoccupied = false;

    let next_off_0 = {
        let current = page.offset(levels[0] as isize) as *const u16;
        u16::from_le(*current)
    };

    for level in 0..N_LEVELS {
        let current = page.offset(levels[level] as isize) as *mut u16;
        let next_off = u16::from_le(*(current.offset(level as isize)));
        if next_off == next_off_0 {
            // If the entry to be deleted is in the list at this level, delete it.
            let next = page.offset(next_off as isize) as *mut u16;
            if level == 0 {
                // At the first level, if we're deleting a
                // value stored in a large value page, and we
                // do not return that value, we need to
                // decrement its reference counter.
                let (key,value) = read_key_value(next as *const u8);
                if value_must_be_freed {
                    if let UnsafeValue::O { offset, .. } = value {
                        debug!("found value, freeing {:?}", offset);
                        // try!(free_value(rng,txn,offset,len))
                    }
                }
                // Mark the freed space on the page.
                let size = record_size(key.len(),value.len() as usize);
                *(page.p_occupied()) = (page.occupied() - size).to_le();
                if (page.occupied() as usize) < PAGE_SIZE >> 1 {
                    page_becomes_underoccupied = true;
                }
            }
            // Delete the entry at this level.
            let next_next_off = *(next.offset(level as isize));
            debug!("local_delete_at: {:?}.{:?} to {:?}", page.page_offset(), next_off, next_next_off);
            *current.offset(level as isize) = next_next_off; // it's already le.
        }
    }
    Ok(page_becomes_underoccupied)
}



/// Replaces the current entry with a "Smallest" struct, taking care of any required allocation/CoW.
unsafe fn replace_with_smallest<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow,
                                          levels:&[u16],
                                          new_levels:&mut [u16],
                                          value_must_be_freed:bool,
                                          child:u64,
                                          smallest:&Smallest) -> Result<Res,Error> {
    let size = record_size(smallest.key_len, smallest.value.len() as usize);

    // Evaluate what to do:
    // - if size >= smallest.size, replace, update occupied.
    // - else can_alloc_and_compact + alloc.
    let former_size = {
        let next_off = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));
        let next = page.offset(next_off as isize);
        let (key, value) = read_key_value(next);
        record_size(key.len(), value.len() as usize)
    };
    if cfg!(not(feature="delete_always_realloc")) && former_size >= size {

        let mut page = try!(cow_pinpointing(rng, txn, page, levels, new_levels, false, false, true, 0));

        let next_off = u16::from_le(*(page.offset(new_levels[0] as isize) as *const u16));
        if value_must_be_freed {
            let (_,value) = read_key_value(page.offset(next_off as isize));
            if let UnsafeValue::O { offset, len } = value {
                debug!("replace_with_smallest: freeing {:?}", offset);
                try!(free_value(rng, txn, offset, len))
            }
        }
        debug!("replacing in page {:?} at offset {:?}", page.page_offset(), next_off);
        // advance the levels containing the element before the one we're inserting.
        for l in 0..N_LEVELS {
            let current = page.offset(new_levels[l] as isize) as *mut u16;
            let next_ = u16::from_le(*(current.offset(l as isize)));
            if next_ == next_off {
                new_levels[l] = next_
            } else {
                break
            }
        }
        debug!("new_levels = {:?}", new_levels);
        page.write_key_value(next_off, smallest.key_ptr, smallest.key_len, smallest.value);
        *(page.p_occupied()) = ((page.occupied() + size) - former_size).to_le();
        *((page.offset(next_off as isize) as *mut u64).offset(2)) = child.to_le();
        Ok(Res::Ok { page:page })

    } else {
        // let mut new_levels = [0;N_LEVELS];
        let key_ = std::slice::from_raw_parts(smallest.key_ptr, smallest.key_len);
        let value_ = smallest.value;

        let next_off = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));

        let off = page.can_alloc(size - former_size);
        if off > 0 {
            if off + size < PAGE_SIZE as u16 && get_rc(txn, page.page_offset()) <= 1 {
                debug!("replace_with_smallest, no copy");
                // No need to copy, we can just delete in place.
                let mut page = try!(cow_pinpointing(rng, txn, page, levels, new_levels, false, false, true, 0));
                try!(local_delete_at(&mut page, new_levels, value_must_be_freed));
                debug_assert!(off+size < PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, key_, value_, child, off, size, new_levels);

                let underfull = (page.occupied() as usize) < (PAGE_SIZE >> 1);
                if underfull {
                    unimplemented!() // Ok(Res::Underfull { page:page })
                } else {
                    Ok(Res::Ok { page:page })
                }
            } else {
                debug!("replace_with_smallest, copy");
                let mut page = try!(cow_pinpointing(rng, txn, page.as_nonmut(), levels, new_levels, true, false, true, 0));
                let off = page.can_alloc(size);
                debug_assert!(off+size < PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, key_, value_, child, off, size, new_levels);

                let underfull = (page.occupied() as usize) < (PAGE_SIZE >> 1);
                debug!("underfull = {:?}, page {:?}", underfull, page);
                if underfull {
                    unimplemented!() // Ok(Res::Underfull { page:page })
                } else {
                    Ok(Res::Ok { page:page })
                }
            }
        } else {
            // Or we cannot, which means that the page needs to split, forgetting next_off.
            Ok(try!(split_page(rng, txn, &page, key_, value_, child, next_off, 0)))
        }
    }
}



unsafe fn insert_in_res<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, result: Res, levels:&[u16], new_levels:&mut [u16], key:&[u8], value:UnsafeValue,
                                  right_page:u64) -> Result<(Res,Option<Smallest>), Error> {
    match result {
        Res::Underfull { page, .. } => unimplemented!(),
        Res::Ok { page, .. } => {

            // We replaced the deleted element, now let's reinsert the middle element of the split.
            let size = record_size(key.len(), value.len() as usize);
            let result = match try!(can_alloc_and_compact(rng,txn,Cow::from_mut_page(page),
                                                          size,&levels[..], &mut new_levels[..])) {
                Alloc::Can(mut page, off) => {
                    // Either we can allocate on this page.
                    debug_assert!(off+size < PAGE_SIZE as u16);
                    local_insert_at(rng, &mut page, key, value, right_page, off, size, &mut new_levels[..]);
                    Ok((Res::Ok { page:page }, None))
                },
                Alloc::Cannot(page) => {
                    // Or we cannot, which means that the page needs to split.
                    Ok((try!(split_page(rng, txn, &page, key, value, right_page, NIL, 0)), None))
                }
            };
            result
        },
        Res::Split { key_ptr, key_len, value:value_,  left:left_, right:right_, free_page} => {
            let key_ = std::slice::from_raw_parts(key_ptr, key_len);

            let insert_in_left =
                match key.cmp(key_) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => {
                        let ord = (Value::from_unsafe(&value, txn)).cmp(Value::from_unsafe(&value_, txn));
                        ord == Ordering::Less || ord == Ordering::Equal
                    }
                };

            
            if insert_in_left {
                match try!(insert(rng, txn, Cow::from_mut_page(left_), key, value, right_page, false)) {
                    Res::Ok { page, .. } => {
                        Ok((Res::Split { key_ptr:key_ptr, key_len:key_len,
                                         value:value_,  left:page, right:right_,
                                         free_page:free_page }, None))
                    },
                    _ => unreachable!()
                }
            } else {
                match try!(insert(rng, txn, Cow::from_mut_page(right_), key, value, right_page, false)) {
                    Res::Ok { page, .. } => {
                        Ok((Res::Split { key_ptr:key_ptr, key_len:key_len,
                                         value:value_,  left:left_, right:page,
                                         free_page:free_page }, None))
                    },
                    _ => unreachable!()
                }
            }
        },
        Res::Nothing { .. } => unreachable!(),
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
            (Res::Underfull { page,.. }, None) => {
                unimplemented!()
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
    unsafe {
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
                    Res::Underfull { page:page_, .. } => {
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
    test_delete_all(20, 100, 20, Sorted::No)
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
    test_delete_all(200, 100, 200, Sorted::No)
}


#[test]
fn test_delete_all_large() {
    test_delete_all(200, 200, 2000, Sorted::No)
}

#[test]
fn test_delete_all_really_large() {
    test_delete_all(200, 200, 10000, Sorted::No)
}
