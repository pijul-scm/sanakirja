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


unsafe fn merge<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, levels:&mut [u16], replace: Option<Smallest>) -> Result<Res,Error> {

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
        debug!("merging");
        // Merge the left page into the right page.
        for (_, key,value,r) in PI::new(&left_child) {
            debug!("inserting {:?} into {:?}", std::str::from_utf8_unchecked(key), right_child);
            match try!(insert(rng, txn, right_child, key, value, r)) {
                Res::Ok { page, .. } => right_child = Cow::from_mut_page(page),
                _ => unreachable!()
            }
        }
        println!("size after merging: {:?}", right_child.occupied());
        // Then set the new left child for the right child, and re-insert the middle element.
        let right_left_child = u64::from_le(*((right_child.offset(0) as *const u64).offset(2)));
        *((right_child.offset(0) as *mut u64).offset(2)) = *((left_child.offset(0) as *const u64).offset(2));

        let (key,value) =
            if let Some(ref repl) = replace {
                (std::slice::from_raw_parts(repl.key_ptr, repl.key_len), repl.value)
            } else {
                let next = u16::from_le(*(page.offset(levels[0] as isize) as *const u16));
                read_key_value(page.offset(next as isize))
            };
        debug!("inserting {:?}", std::str::from_utf8_unchecked(key));
        match try!(insert(rng, txn, right_child, key, value, right_left_child)) {
            Res::Ok { page, .. } => right_child = Cow::from_mut_page(page),
            Res::Nothing { .. } => unreachable!(),
            Res::Split { .. } => unreachable!()
        }
        // Finally, delete the middle element, and update its right child.
        let mut new_levels = [0;N_LEVELS];
        let page = try!(cow_pinpointing(rng, txn, page, levels, &mut new_levels, true));

        *((page.offset(new_levels[0] as isize) as *mut u64).offset(2)) = right_child.page_offset().to_le();


        // TODO: free left_child, except if we're currently looking for the smallest element.
        
        let underfull = page.occupied() < (PAGE_SIZE/2) as u16;
        Ok(Res::Ok { page:page, underfull: underfull })
    } else {
        debug!("rebalancing");
        // Rebalance. Allocate two pages, fill the first one to ceil(size/2), which is smaller than PAGE_SIZE.
        // Delete the middle element and insert it in the appropriate page.
        //
        let mut new_left = try!(txn.alloc_page());
        new_left.init();
        let mut new_right = try!(txn.alloc_page());
        new_right.init();
        let mut middle = None;

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
            let mut it = PI::new(&left_child)
                .chain((PI { page: &page, current:next }).take(1))
                .chain(PI::new(&right_child));

            let mut left_bytes = 24;
            let mut left_levels = [FIRST_HEAD;N_LEVELS];
            let mut right_levels = [FIRST_HEAD;N_LEVELS];
            for (_, key, value, r) in it {

                let (key,value, r) = match replace {
                    Some(ref repl) if r == right_child.page_offset() => {
                        (std::slice::from_raw_parts(repl.key_ptr, repl.key_len), repl.value, right_left_child)
                    },
                    _ if r == right_child.page_offset() => (key, value, right_left_child),
                    _ => (key,value,r)
                };
                
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
            debug!("middle = {:?}", middle);
            let mut new_levels = [0;N_LEVELS];
            // Delete the current entry, insert the new one instead.
            let page = try!(cow_pinpointing(rng, txn, page, levels, &mut new_levels, true));
            *((page.offset(new_levels[0] as isize) as *mut u64).offset(2)) = new_left.page_offset().to_le();

            if let Some((key_ptr,key_len,value,r)) = middle {

                *((new_right.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = r.to_le();
                let key = std::slice::from_raw_parts(key_ptr, key_len);
                check_alloc_local_insert(rng, txn, Cow::from_mut_page(page),
                                         key, value, new_right.page_offset(), &mut new_levels)
            } else {
                unreachable!()
            }
        };

        // We can safely free the right child. TODO: Causes tests to fail, why?
        if cfg!(test) {
            match result {
                Ok(Res::Ok { ref page, .. }) => {
                    for (_,_,_,r) in PI::new(page) {
                        assert!(r != right_child.page_offset());
                        assert!(r != left_child.page_offset());
                    }
                },
                Ok(Res::Split { ref left, ref right, .. }) => {
                    for (_,_,_,r) in PI::new(left) {
                        assert!(r != right_child.page_offset());
                        assert!(r != left_child.page_offset());
                    }
                    for (_,_,_,r) in PI::new(right) {
                        assert!(r != right_child.page_offset());
                        assert!(r != left_child.page_offset());
                    }
                },
                _ => {}
            }
        }
        //try!(free(rng, txn, right_child.page_offset()));
        // TODO: free left child, except if we're currently looking for the smallest element.
        result
    }
}


unsafe fn delete<R:Rng, T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, comp:C) -> Result<(Res,Option<Smallest>), Error> {
    let mut levels:[u16;N_LEVELS] = [FIRST_HEAD;N_LEVELS];
    let mut eq = false;
    match comp {
        C::KV { key, value } => set_levels(txn, &page, key, Some(value), &mut levels, &mut eq),
        C::K { key } => set_levels(txn, &page, key, None, &mut levels, &mut eq),
        C::Smallest => {eq = true} // Levels are all 0, fine!
    }

    // Here, "comp" is smaller than or equal to the (key,value) at
    // offset "next", and strictly larger than the (key,value) at
    // offset "current".
    // First delete in the page below.
    let child_page = u64::from_le(*((page.offset(levels[0] as isize) as *const u64).offset(2)));
    debug!("next_page = {:?}", child_page);
    let del = if child_page > 0 {
        let next_page = txn.load_cow_page(child_page);
        Some(try!(delete(rng, txn, next_page, comp)))
    } else {
        None
    };
    // Then delete in the current page, depending on the results.
    match del {
        None if eq => {
            debug!("none + eq");
            // No page below, but we can delete something here.
            let mut new_levels = [0;N_LEVELS];
            let mut page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false));

            let cur_ptr = page.offset(new_levels[0] as isize) as *const u16;
            let next_off = u16::from_le(*cur_ptr);

            debug_assert!(next_off > 0 && next_off != NIL);

            let next_ptr = page.offset(next_off as isize);
            let (next_key,next_value) = read_key_value(next_ptr);

            if comp.is_smallest() {
                let page_offset = page.page_offset();
                let underfull = try!(local_delete_at(rng, txn, &mut page, &new_levels, true));
                Ok((Res::Ok { page:page, underfull:underfull },
                    Some(Smallest {
                        key_ptr: next_key.as_ptr(),
                        key_len: next_key.len(),
                        value: next_value,
                        // if the page is underfull, it will be merged
                        // with its right sibling just one level up.
                        free_page: if underfull { page_offset } else { 0 }
                    })))
            } else {
                let underfull = try!(local_delete_at(rng, txn, &mut page, &new_levels, false));
                Ok((Res::Ok { page:page, underfull:underfull }, None))
            }
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
                (Res::Ok { page: child_page, underfull }, Some(smallest)) => {
                    let mut new_levels = [0;N_LEVELS];
                    let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false));

                    // Set the child page here, regardless of whether a merge is coming after this.
                    if underfull {
                        debug!("underfull");
                        let next_off = {
                            let current = page.offset(new_levels[0] as isize) as *mut u16;
                            u16::from_le(*current)
                        };
                        let next = page.offset(next_off as isize);
                        *((next as *mut u64).offset(2)) = child_page.page_offset().to_le();
                        let result = try!(merge(rng, txn, Cow::from_mut_page(page), &mut new_levels, Some(smallest)));

                        /*if smallest.free_page > 0 {
                            try!(free(rng, txn, smallest.free_page));
                        }*/
                        Ok((result,None))
                    } else {
                        debug!("not underfull");
                        let mut new_new_levels = [0;N_LEVELS];
                        let result = try!(replace_with_smallest(
                            rng, txn, Cow::from_mut_page(page),
                            &new_levels[..], &mut new_new_levels[..], false,
                            child_page.page_offset(), &smallest
                        ));

                        if smallest.free_page > 0 {
                            try!(free(rng, txn, smallest.free_page));
                        }
                        
                        /*debug!("replace_with_smallest exited");
                        match result {
                            Res::Ok { ref page, .. } => {
                                debug!("replace_with_smallest returned {:?}", page);
                                let db = Db { root_num: -1, root: page.page_offset() };
                                txn.debug(&db, format!("/tmp/not_underfull_{}", page.page_offset()), false, false);
                            },
                            _ => {
                                debug!("split");
                            }
                        }*/
                        
                        Ok((result, None))
                    }
                },
                (Res::Split { key_len,key_ptr,value, left, right, free_page }, Some(smallest)) => {
                    // Here, reinsert smallest + left, and key + right.
                    // If smallest+left doesn't cause the current page to split, fine.
                    // If it causes the page to split, then must be space for smallest on each of the pages.
                    let mut new_levels = [0;N_LEVELS];
                    let key = std::slice::from_raw_parts(key_ptr,key_len);
                    let result = try!(replace_with_smallest(rng, txn, page, &levels[..], &mut new_levels[..],
                                                            false, left.page_offset(), &smallest));
                    insert_in_res(rng, txn, result, &levels[..], &mut new_levels[..], key, value, right.page_offset())
                },
                (Res::Ok { .. }, None) |
                (Res::Split { .. }, None) |
                (Res::Nothing { .. }, _) => {
                    if cfg!(debug_assertions) {
                        panic!("Child page {:?} was empty when trying to remove its smallest element.", page)
                    } else {
                        unreachable!()
                    }
                }
            }
        },
        Some((Res::Ok { page:child_page, underfull }, smallest)) => {
            debug!("ok");
            // Update the pointer here
            let mut new_levels = [0;N_LEVELS];
            let page = try!(cow_pinpointing(rng, txn, page, &levels, &mut new_levels, false));
            *((page.offset(new_levels[0] as isize) as *mut u64).offset(2)) = child_page.page_offset().to_le();
            if underfull {
                Ok((try!(merge(rng, txn, Cow::from_mut_page(page), &mut new_levels, None)), smallest))
            } else {
                Ok((Res::Ok { page:page, underfull:false }, smallest))
            }
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
                    let current = page.offset(new_levels[0] as isize);
                    *((current as *mut u64).offset(2)) = left.page_offset().to_le();
                    debug_assert!(off+size < PAGE_SIZE as u16);
                    local_insert_at(rng, &mut page, key_, value_, right.page_offset(), off, size, &mut new_levels[..]);
                    Ok((Res::Ok { page:page, underfull:false }, smallest))
                },
                Alloc::Cannot(page) => {
                    // Or we cannot, which means that the page needs to split.
                    Ok((try!(split_page(rng, txn, &page, key_, value_,
                                        right.page_offset(), levels[0], left.page_offset())),
                        smallest))
                }
            };
            try!(free_local(rng, txn, free_page));
            result
        },
    }
}


/// Adjusts the pointers on a page to skip the next value. if argument
/// `value_must_be_freed` is `true`, also free the large values pages
/// referenced from this page.
unsafe fn local_delete_at<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, page:&mut MutPage, levels:&[u16], value_must_be_freed:bool) -> Result<bool,Error> {
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
                        debug!(target:"free_value", "found value, freeing");
                        try!(free_value(rng,txn,offset))
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

        let mut page = try!(cow_pinpointing(rng, txn, page, levels, new_levels, false));

        let next_off = u16::from_le(*(page.offset(new_levels[0] as isize) as *const u16));
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
        Ok(Res::Ok { page:page, underfull:false })

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
                let mut page = try!(cow_pinpointing(rng, txn, page, levels, new_levels, false));
                try!(local_delete_at(rng, txn, &mut page, new_levels, value_must_be_freed));
                debug_assert!(off+size < PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, key_, value_, child, off, size, new_levels);

                let underfull = (page.occupied() as usize) < (PAGE_SIZE >> 1);
                Ok(Res::Ok { page:page, underfull:underfull })

            } else {
                debug!("replace_with_smallest, copy");
                let mut page = try!(cow_pinpointing(rng, txn, page.as_nonmut(), levels, new_levels, true));
                let off = page.can_alloc(size);
                debug_assert!(off+size < PAGE_SIZE as u16);
                local_insert_at(rng, &mut page, key_, value_, child, off, size, new_levels);

                let underfull = (page.occupied() as usize) < (PAGE_SIZE >> 1);
                debug!("underfull = {:?}, page {:?}", underfull, page);
                Ok(Res::Ok { page:page, underfull:underfull })
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
        Res::Ok { page, .. } => {

            // We replaced the deleted element, now let's reinsert the middle element of the split.
            let size = record_size(key.len(), value.len() as usize);
            let result = match try!(can_alloc_and_compact(rng,txn,Cow::from_mut_page(page),
                                                          size,&levels[..], &mut new_levels[..])) {
                Alloc::Can(mut page, off) => {
                    // Either we can allocate on this page.
                    debug_assert!(off+size < PAGE_SIZE as u16);
                    local_insert_at(rng, &mut page, key, value, right_page, off, size, &mut new_levels[..]);
                    Ok((Res::Ok { page:page, underfull:false }, None))
                },
                Alloc::Cannot(page) => {
                    // Or we cannot, which means that the page needs to split.
                    Ok((try!(split_page(rng, txn, &page, key, value, right_page, NIL, 0)), None))
                }
            };
            // free(txn, free_page)
            result
        },
        Res::Split { key_ptr, key_len, value:value_,  left:left_, right:right_, free_page} => {
            let key_ = std::slice::from_raw_parts(key_ptr, key_len);

            let insert_in_left =
                match key.cmp(key_) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => {
                        let ord = (Value { txn:Some(txn), value:value }).cmp(Value { txn:Some(txn), value:value_ });
                        ord == Ordering::Less || ord == Ordering::Equal
                    }
                };

            
            if insert_in_left {
                match try!(insert(rng, txn, Cow::from_mut_page(left_), key, value, right_page)) {
                    Res::Ok { page, .. } => {
                        Ok((Res::Split { key_ptr:key_ptr, key_len:key_len,
                                         value:value_,  left:page, right:right_,
                                         free_page:free_page }, None))
                    },
                    _ => unreachable!()
                }
            } else {
                match try!(insert(rng, txn, Cow::from_mut_page(right_), key, value, right_page)) {
                    Res::Ok { page, .. } => {
                        Ok((Res::Split { key_ptr:key_ptr, key_len:key_len,
                                         value:value_,  left:left_, right:page,
                                         free_page:free_page }, None))
                    },
                    _ => unreachable!()
                }
            }
        },
        Res::Nothing { .. } => unreachable!()
    }

}





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
                let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
                match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0) {
                    Ok(Res::Ok { page:page_,.. }) => {
                        page = page_
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
        txn.debug(&db, format!("/tmp/before"), false, false);
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
        txn.debug(&db, format!("/tmp/after"), false, false);
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
                let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
                match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0) {
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
            }
            insertions.push((key_,value_))
        }
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&db, format!("/tmp/before"), false, false);
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
        txn.debug(&db, format!("/tmp/after"), false, false);
    }
}

#[cfg(test)]
enum Sorted {
    No, Incr, Decr
}

#[cfg(test)]
fn test_delete_all(n:usize, keysize:usize, sorted:Sorted) {
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
                .take(keysize)
                .collect();
            {
                let key = key_.as_bytes();
                let value = value_.as_bytes();
                let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
                match insert(&mut rng, &mut txn, Cow::from_mut_page(page), key, value, 0) {
                    Ok(Res::Ok { page:page_,.. }) => {
                        page = page_
                    },
                    Ok(Res::Nothing { page:page_ }) => {
                        //println!("already present");
                        page = page_.unwrap_mut()
                    },
                    Ok(x) => {
                        debug!("root split");
                        page = root_split(&mut rng, &mut txn, x).unwrap()
                    },
                    _ => panic!("")
                }
            }
            debug!("put i = {:?}", i);
            debug!("key = {:?}", key_);

            let db = Db { root_num: -1, root: page.page_offset() };
            txn.debug(&db, format!("/tmp/before_{}", i), false, false);

            insertions.push((key_,value_))
        }
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&db, format!("/tmp/before"), false, false);

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
            let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
            match delete(&mut rng, &mut txn, Cow::from_mut_page(page), C::KV { key:key, value:value }).unwrap() {
                (Res::Ok { page:page_, .. }, None) => {
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
            txn.debug(&db, format!("/tmp/after_{}", i), false, false);

        }
        debug!("delete done, debugging");
        
        let db = Db { root_num: -1, root: page.page_offset() };
        txn.debug(&db, format!("/tmp/after"), false, false);
    }
}

#[test]
fn test_delete_all_sorted_20() {
    test_delete_all(200, 20, Sorted::Incr)
}
#[test]
fn test_delete_all_decr_20() {
    test_delete_all(200, 20, Sorted::Decr)
}
#[test]
fn test_delete_all_unsorted_20() {
    test_delete_all(200, 20, Sorted::No)
}

#[test]
fn test_delete_all_sorted_200() {
    test_delete_all(200, 200, Sorted::Incr)
}
#[test]
fn test_delete_all_decr_200() {
    test_delete_all(200, 200, Sorted::Decr)
}
#[test]
fn test_delete_all_unsorted_200() {
    test_delete_all(200, 200, Sorted::No)
}

#[test]
fn test_delete_all_unsorted_5() {
    test_delete_all(10, 200, Sorted::No)
}


#[test]
fn test_delete_all_large() {
    test_delete_all(2000, 2000, Sorted::No)
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
            (Res::Ok { page, .. }, None) => {
                // Maybe the root is empty. Check
                let next = u16::from_le(*(page.offset(FIRST_HEAD as isize) as *const u16));
                let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                if next == NIL && next_page != 0 {
                    db.root = next_page
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
