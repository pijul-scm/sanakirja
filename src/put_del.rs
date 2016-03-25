use super::txn::*;
use super::transaction::PAGE_SIZE;
use std;
use std::cmp::Ordering;
use super::transaction;
use rand::{Rng};


enum Result {
    Ok { page: MutPage,
         // position is the offset in the page where the insertion happened (so that cow_pinpointing can do its job), or in the case of deletions, it is a code describing what happened to the page below.
         position:u16
    },
    Split {
        key_ptr:*const u8,
        key_len:usize,
        value: UnsafeValue,
        left: MutPage,
        right: MutPage,
        free_page: u64,
    },
}

// Turn a Cow into a MutPage, copying it if it's not already mutable. In the case a copy is needed, and argument 'pinpoint' is non-zero, a non-zore offset (in bytes) to the equivalent element in the new page is returned. This can happen for instance because of compaction.
fn cow_pinpointing<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow, pinpoint:u16) -> (MutPage,u16) {
    unsafe {
        match page.cow {
            transaction::Cow::Page(p) => {
                let page_offset = p.offset;
                let p = Page { page:p };
                let mut page = txn.alloc_page();
                page.init();
                let mut current = FIRST_HEAD;
                debug!("PINPOINTING: {:?} {:?} {:?}", page, page.first_free(), page.occupied());
                let mut cow = Cow::from_mut_page(page);
                let mut pinpointed = FIRST_HEAD;
                while current != NIL {
                    let pp = p.offset(current as isize);
                    let right_page = u64::from_le(*((pp as *const u64).offset(2)));
                    if current > FIRST_HEAD {
                        let (key,value) = read_key_value(pp);
                        debug!("PINPOINT: {:?}", std::str::from_utf8(key).unwrap());
                        match insert(rng, txn, cow, key, value, right_page) {
                            Result::Ok { page, position } => {
                                if current == pinpoint {
                                    pinpointed = position
                                }
                                cow = Cow::from_mut_page(page)
                            },
                            _ => unreachable!()
                        }
                    } else {
                        *((cow.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = right_page
                    }
                    current = u16::from_le(*((p.offset(current as isize) as *const u16)));
                }
                debug!("/PINPOINTING");
                transaction::free(&mut txn.txn, page_offset);
                (cow.unwrap_mut(),pinpointed)
            }
            transaction::Cow::MutPage(p) => (MutPage { page:p }, pinpoint)
        }
    }
}

unsafe fn insert<R:Rng>(rng:&mut R, txn:&mut MutTxn, mut page:Cow, key:&[u8],value:UnsafeValue,right_page:u64) -> Result {
    let mut levels:[u16;MAX_LEVEL+1] = [0;MAX_LEVEL+1];
    let mut level = MAX_LEVEL;
    let mut current_off = FIRST_HEAD;

    let mut next_page = 0; // Next page to explore.

    let is_leaf = (*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2))) == 0;
    let size = record_size(key.len(), value.len() as usize);
    let off = page.can_alloc(size);
    debug!("INSERT, off = {:?}, {:?} {:?}, right_page={:?}, is_leaf={:?}", off, page, page.occupied(), right_page, is_leaf);
    if off > 0 && (right_page>0 || is_leaf) {
        if off + size < PAGE_SIZE as u16 {
            // No need to compact.
            //debug!("NO COMPACT");
            let (page_, _) = cow_pinpointing(rng, txn, page, 0);
            page = Cow::from_mut_page(page_)
        } else {
            debug!("COMPACT");
            let (page_, _) = cow_pinpointing(rng, txn, page.as_nonmut(), 0);
            debug!("/COMPACT");
            page = Cow::from_mut_page(page_)
        }
    }
    debug!("INSERT: {:?} {:?} {:?} {:?}",page,current_off, right_page, std::str::from_utf8(key).unwrap());
    let mut current = page.offset(current_off as isize) as *mut u16;
    loop {
        // advance in the list until there's nothing more to do.
        loop {
            let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
            //println!("first loop, next = {:?}", next);
            if next == NIL {
                levels[level] = current_off;
                break
            } else {
                let next_ptr = page.offset(next as isize);
                let (next_key,next_value) = read_key_value(next_ptr);
                match key.cmp(next_key) {
                    Ordering::Less => break,
                    Ordering::Equal =>
                        match (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:next_value}) {
                            Ordering::Less => break,
                            Ordering::Equal => {
                                break
                            },
                            Ordering::Greater => {
                                current_off = next;
                                current = page.offset(current_off as isize) as *mut u16;
                            }
                        },
                    Ordering::Greater => {
                        current_off = next;
                        current = page.offset(current_off as isize) as *mut u16;
                    }
                }
            }
        }
        if level == 0 {
            next_page = u64::from_le(*((current as *const u64).offset(2)));
            break
        } else {
            levels[level] = current_off;
            level -= 1
        }
    }
    if next_page > 0 && right_page == 0 {
        let next_page = txn.load_cow_page(next_page);
        match insert(rng, txn, next_page, key, value, right_page) {
            Result::Ok { page:next_page, .. } => {
                let (page, current_off) = cow_pinpointing(rng, txn, page, current_off);
                let current = page.offset(current_off as isize);
                debug!("page={:?}, next = {:?}", page,next_page);
                *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                Result::Ok { page:page, position: NIL }
            },
            Result::Split { key_ptr,key_len,value:value_,left,right,free_page } => {

                let size = record_size(key_len, value_.len() as usize);
                let off = page.can_alloc(size);
                // If there's enough space here, just go on inserting.
                if off > 0 {
                    let (page, current_off) = cow_pinpointing(rng, txn, page, current_off);
                    let current = page.offset(current_off as isize);
                    *((current as *mut u64).offset(2)) = left.page_offset().to_le();
                    let key_ = std::slice::from_raw_parts(key_ptr,key_len);
                    // Then, reinsert (key_,value_) in the current page.
                    let result = insert(rng,txn,Cow::from_mut_page(page),key_, value_, right.page_offset());
                    transaction::free(&mut txn.txn, free_page);
                    result
                } else {
                    // Else, split+translate first, then insert.
                    let key_ = std::slice::from_raw_parts(key_ptr,key_len);
                    let result = split_page(rng, txn, &page, size as usize,
                                            key_, value_, right.page_offset(),
                                            current_off, left.page_offset());
                    transaction::free(&mut txn.txn, free_page);
                    result
                }
            }
        }
    } else {
        // next_page == 0 || right_page > 0, i.e. is_leaf || right_page>0
        let off = page.can_alloc(size);
        if off > 0 {
            // If there's enough space, copy the page and reinsert between current_off and next.
            let mut page = page.unwrap_mut();
            let current = page.offset(current_off as isize) as *mut u16;
            let next = u16::from_le(*current);
            page.alloc_key_value(off, size, key.as_ptr(), key.len(), value);
            *((page.offset(off as isize) as *mut u64).offset(2)) = right_page.to_le();
            *(page.offset(off as isize) as *mut u16) = next.to_le();
            *current = off.to_le();

            // Add to upper levels
            level = 1;
            //debug!("levels = {:?}", &levels[..]);
            while level <= MAX_LEVEL && rng.gen() {
                let ptr = page.offset(levels[level] as isize) as *mut u16;
                let next = *(ptr.offset(level as isize));
                *((page.offset(off as isize) as *mut u16).offset(level as isize)) = next;
                *(ptr.offset(level as isize)) = off.to_le();
                level+=1
            }

            // Return the position of the new allocation.
            Result::Ok { page:page, position:off }
        } else {
            debug!("SPLIT");
            // Not enough space, split.
            split_page(rng, txn, &page, size as usize, key, value, right_page, 0, 0)
        }
    }
}


/// The arguments to split_page are non-trivial. This function splits a page, and then reinserts the new element. The middle element of the split is returned as a Result::Split { .. }.
unsafe fn split_page<R:Rng>(rng:&mut R, txn:&mut MutTxn,page:&Cow,
                            // The record size (actually redundant with key and value),
                            // key, value, right_page of the record to insert.
                            size:usize, key:&[u8], value:UnsafeValue, right_page:u64,
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
                            translate_index:u16, translate_right_page:u64)->Result {

    debug!("SPLIT");
    let mut left = txn.alloc_page();
    left.init();
    *((left.offset(FIRST_HEAD as isize) as *mut u64).offset(2))
        = *((page.offset(FIRST_HEAD as isize) as *const u64).offset(2));

    let mut left_bytes = FIRST_HEAD;
    let mut current = FIRST_HEAD;
    let mut cow_left = Cow::from_mut_page(left);

    current = u16::from_le(*((page.data() as *const u8).offset(current as isize) as *const u16));
    loop {
        let p = page.offset(current as isize);
        let right_page = u64::from_le(*((p as *const u64).offset(2)));
        let (key,value) = {
            let (key,value) = read_key_value(p);
            (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
        };
        let current_size = record_size(key.len(), value.len() as usize);
        if left_bytes + current_size < (PAGE_SIZE as u16) / 2 {
            let right_page = if current == translate_index { translate_right_page } else { right_page };
            match insert(rng, txn, cow_left, key, value, right_page) {
                Result::Ok { page,.. } => cow_left = Cow::from_mut_page(page),
                _ => unreachable!()
            }
            left_bytes += current_size as u16;
        } else {
            break
        }
        current = u16::from_le(*((page.data() as *const u8).offset(current as isize) as *const u16));
    }
    let middle = current;
    debug_assert!(middle != NIL);
    // move on to next
    current = u16::from_le(*((page.data() as *const u8).offset(current as isize) as *const u16));
    //debug_assert!(current != NIL);

    let mut right = txn.alloc_page();
    right.init();
    *((right.offset(FIRST_HEAD as isize) as *mut u64).offset(2))
        = *((page.offset(middle as isize) as *const u64).offset(2));
    let mut cow_right = Cow::from_mut_page(right);
    while current != NIL {
        if current > FIRST_HEAD {
            let p = page.offset(current as isize);
            let right_page = u64::from_le(*((p as *const u64).offset(2)));
            let right_page = if current == translate_index { translate_right_page } else { right_page };
            let (key,value) = {
                let (key,value) = read_key_value(p);
                (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
            };
            match insert(rng, txn, cow_right, key, value, right_page) {
                Result::Ok { page,.. } => cow_right = Cow::from_mut_page(page),
                _ => unreachable!()
            }
        }
        current = u16::from_le(*((page.data() as *const u8).offset(current as isize) as *const u16));
    }
    let p = page.offset(middle as isize);
    let (key_ptr,key_len,value_) = {
        let (key,value) = read_key_value(p);
        (key.as_ptr(),key.len(),value)
    };


    // We still need to reinsert key,value in one of the two pages.
    let key_ = std::slice::from_raw_parts(key_ptr,key_len);
    let (left,right) = match key.cmp(key_) {
        Ordering::Less => {
            match insert(rng,txn,cow_left,key, value, right_page) {
                Result::Ok { page, .. } => (page, cow_right.unwrap_mut()),
                _ => unreachable!()
            }
        },
        Ordering::Equal =>
            match (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:value_}) {
                Ordering::Less | Ordering::Equal =>
                    match insert(rng,txn,cow_left,key, value, right_page) {
                        Result::Ok { page, .. } => (page, cow_right.unwrap_mut()),
                        _ => unreachable!()
                    },
                Ordering::Greater =>
                    match insert(rng, txn, cow_right, key, value, right_page) {
                        Result::Ok { page, .. } => (cow_left.unwrap_mut(), page),
                        _ => unreachable!()
                    },
            },
        Ordering::Greater =>
            match insert(rng, txn, cow_right, key, value, right_page) {
                Result::Ok { page, .. } => (cow_left.unwrap_mut(), page),
                _ => unreachable!()
            }
    };
    debug!("/SPLIT");
    Result::Split {
        key_ptr: key_ptr,
        key_len: key_len,
        value: value_,
        left: left,
        right: right,
        free_page: page.page_offset()
    }
}

// This function deals with the case where the main page split, either during insert, or during delete.
fn root_split<R:Rng>(rng:&mut R, txn: &mut MutTxn, x:Result) -> Db {
    println!("ROOT SPLIT");
    if let Result::Split { left,right,key_ptr,key_len,value,free_page } = x {
        let mut page = txn.alloc_page();
        page.init();
        unsafe {
            *((page.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = left.page_offset().to_le();
            let key = std::slice::from_raw_parts(key_ptr,key_len);
            let right_offset = right.page_offset();
            let ins = insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset);
            transaction::free(&mut txn.txn, free_page);
            match ins {
                Result::Ok { page,.. } => {
                    Db { root:page.page_offset() }
                },
                _ => unreachable!() // We just inserted a small enough value into a freshly allocated page, no split can possibly happen.
            }
        }
    } else {
        unreachable!()
    }
}

pub fn put<R:Rng>(rng:&mut R, txn: &mut MutTxn, db: Db, key: &[u8], value: &[u8]) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    unsafe {
        let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
        let value = if value.len() > VALUE_SIZE_THRESHOLD {
            alloc_value(txn,value)
        } else {
            UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
        };
        debug!("value = {:?}", Value { txn:txn,value:value });
        match insert(rng, txn, root_page, key, value, 0) {
            Result::Ok { page,.. } => Db { root:page.page_offset() },
            x => {
                root_split(rng,txn,x)
            }
        }
    }
}


// This type is an instruction to page_delete below.
#[derive(Copy,Clone,Debug)]
enum C<'a> {
    KV { key:&'a [u8], value:UnsafeValue }, // delete by comparing the key and value.
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
    // root page of the B-tree from which the smallest element was taken (used to reinsert)
    reinsert_page:u64
}


unsafe fn delete<R:Rng>(rng:&mut R, txn:&mut MutTxn, mut page:Cow, comp:C) -> Option<(Result,Option<Smallest>)> {
    debug!("delete, page: {:?}", page);
    let mut levels:[u16;MAX_LEVEL+1] = [FIRST_HEAD;MAX_LEVEL+1];
    let mut level = MAX_LEVEL;
    let mut current_off = FIRST_HEAD;
    let mut current = page.offset(current_off as isize) as *mut u16;
    let mut equal = 0; // The smallest known offset to an entry matching comp.
    let mut next_page = 0; // Next page to explore.
    loop {
        // advance in the list until there's nothing more to do.
        loop {
            let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
            if let C::KV { key,value } = comp {
                if next == NIL {
                    levels[level] = current_off;
                    break
                } else {
                    let next_ptr = page.offset(next as isize);
                    let (next_key,next_value) = read_key_value(next_ptr);
                    match key.cmp(next_key) {
                        Ordering::Less => break,
                        Ordering::Equal =>
                            match (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:next_value}) {
                                Ordering::Less => break,
                                Ordering::Equal => {
                                    if equal == 0 {
                                        equal = next;
                                        let (page_, current_off_) = cow_pinpointing(rng, txn, page, current_off);
                                        page = Cow::from_mut_page(page_);
                                        current_off = current_off_;
                                        current = page.offset(current_off as isize) as *mut u16;
                                    }
                                    levels[level] = current_off;
                                    break
                                },
                                Ordering::Greater => {
                                    current_off = next;
                                    current = page.offset(current_off as isize) as *mut u16;
                                }
                            },
                        Ordering::Greater => {
                            current_off = next;
                            current = page.offset(current_off as isize) as *mut u16;
                        }
                    }
                }
            } else {
                debug!("deleting smallest element in page {:?}", page);
                levels[level] = current_off;
                equal = next;
                break
            };
        }
        if level == 0 {
            next_page = u64::from_le(*((current as *const u64).offset(2)));
            break
        } else {
            level -= 1
        }
    }
    // try in the page below.
    let del = if next_page > 0 {
        let next_page = txn.load_cow_page(next_page);
        delete(rng,txn,next_page,comp)
    } else {
        None
    };
    match del {
        None if equal>0 => {
            let mut page_becomes_empty = false;
            // Delete the entries in all lists.
            for level in 0..(MAX_LEVEL+1) {
                let &current_off = levels.get_unchecked(level);
                let current = page.offset(current_off as isize) as *mut u16;
                let next_off = u16::from_le(*(current.offset(level as isize)));
                let next = page.offset(next_off as isize) as *mut u16;
                if next_off == equal {
                    let next_next_off = *(next.offset(level as isize));
                    if level == 0 {
                        let (key,value) = read_key_value(next as *const u8);
                        let size = record_size(key.len(),value.len() as usize);
                        *(page.p_occupied()) = (page.occupied() - size).to_le();
                        if next_next_off == 0 && current_off == FIRST_HEAD {
                            page_becomes_empty = true;
                            break;
                        }
                    }
                    // Delete the entry at this level.
                    *current.offset(level as isize) = next_next_off;
                }
            }
            // If there's a page below, replace with the smallest element in that page.
            let equal_ptr = page.offset(equal as isize);
            let next_page = u64::from_le(*((equal_ptr as *const u64).offset(2)));
            if next_page > 0 {
                let next_page = txn.load_cow_page(next_page);
                match delete(rng,txn, next_page, C::Smallest) {
                    Some((Result::Ok { page:next_page, position }, Some(smallest))) => {
                        if position == 1 { // page deleted
                            let (key,value) = read_key_value(current as *const u8);
                            delete(rng,txn,page,C::KV { key:key, value:value })
                        } else {
                            let key = std::slice::from_raw_parts(smallest.key_ptr,smallest.key_len);
                            Some((insert(rng,txn,page,key, smallest.value, next_page.page_offset()), None))
                        }
                    },
                    _ => {
                        panic!("invariants broken")
                    }
                }
            } else {
                if comp.is_smallest() { // if we're currently looking for the smallest element.
                    let next_ptr = page.offset(equal as isize);
                    let (next_key,next_value) = read_key_value(next_ptr);
                    let page_offset = page.page_offset();
                    Some((Result::Ok { page:page.unwrap_mut(), position: if page_becomes_empty { 1 } else { 0 } },
                          Some(Smallest {
                              key_ptr: next_key.as_ptr(),
                              key_len: next_key.len(),
                              value: next_value,
                              free_page: page_offset,
                              reinsert_page:0
                          })))
                } else {
                    Some((Result::Ok { page:page.unwrap_mut(), position: if page_becomes_empty { 1 } else { 0 } }, None))
                }
            }
        },
        None => None,
        Some((Result::Ok { page:next_page, position }, _)) => {
            if position == 1 {
                //next_page becomes empty. Delete current entry, and reinsert.
                transaction::free(&mut txn.txn, next_page.page_offset());
                let (key,value) = read_key_value(current as *const u8);
                // Delete current entry. Since we lost the list
                // pointers, we need to search all lists and delete
                // the entry we're interested in.
                for level in 0..(MAX_LEVEL+1) {
                    let mut off = FIRST_HEAD;
                    while off != NIL {
                        let ptr = (page.offset(off as isize) as *mut u16).offset(level as isize);
                        let next = u16::from_le(*ptr);
                        if next == current_off {

                            let (key,value) = read_key_value(next as *const u8);
                            let size = record_size(key.len(),value.len() as usize);
                            *(page.p_occupied()) = (page.occupied() - size).to_le();

                            let next_next = u16::from_le(*((page.offset(next as isize) as *const u16)
                                                           .offset(level as isize)));
                            *ptr = next_next;
                            off = next_next
                        } else {
                            off = next
                        }
                    }
                }
                //
                let page_becomes_empty = *page.offset(FIRST_HEAD as isize) == 0;
                if page_becomes_empty {
                    // insert in page below, free current page, return page below.
                    let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    debug_assert!(next_page > 0);
                    let next_page = txn.load_cow_page(next_page);
                    let ins = insert(rng, txn, next_page, key, value, 0);
                    transaction::free(&mut txn.txn, page.page_offset());
                    Some((ins,None))
                } else {
                    Some((Result::Ok {
                        page:page.unwrap_mut(),
                        position: 0
                    }, None))
                }
            } else {
                *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                Some((Result::Ok { page:page.unwrap_mut(), position:0 }, None ))
            }
        },
        Some((Result::Split { key_ptr,key_len,value,left,right,free_page }, _)) => {
            *((current as *mut u64).offset(2)) = left.page_offset().to_le();
            let key = std::slice::from_raw_parts(key_ptr,key_len);
            let result = Some((insert(rng,txn,page,key, value, right.page_offset()), None));
            transaction::free(&mut txn.txn, free_page);
            result
        }
    }
}


pub fn del<R:Rng>(rng:&mut R, txn:&mut MutTxn, db:Db, key:&[u8], value:Option<&[u8]>) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = value.unwrap();
    let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
    unsafe {
        match delete(rng,txn, root_page, C::KV { key:key, value:value }) {
            Some((Result::Ok { page, .. },Some(reinsert))) => {
                let key = std::slice::from_raw_parts(reinsert.key_ptr,reinsert.key_len);
                assert!(key.len() < MAX_KEY_SIZE);
                match insert(rng, txn, Cow::from_mut_page(page), key, reinsert.value, reinsert.reinsert_page) {
                    Result::Ok { page,.. } => {
                        transaction::free(&mut txn.txn, reinsert.free_page);
                        Db { root:page.page_offset() }
                    },
                    x => {
                        let x = root_split(rng,txn,x);
                        transaction::free(&mut txn.txn, reinsert.free_page);
                        x
                    }
                }
            },
            Some((Result::Ok { page, position },None)) => {
                if position == 1 {
                    let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    transaction::free(&mut txn.txn, page.page_offset());
                    Db { root:next_page }
                } else {
                    Db { root:page.page_offset() }
                }
            },
            Some((x,_)) => {
                root_split(rng,txn,x)
            },
            None => db
        }
    }
}
