use super::txn::*;
use super::transaction::PAGE_SIZE;
use std;
use std::cmp::Ordering;
use super::transaction;
use rand::{Rng};

const FIRST_HEAD:u16 = 8;
const MAX_LEVEL:usize = 4;

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
                let p = Page { page:p };
                let mut page = txn.alloc_page();
                page.init();
                let mut current = 8;
                let mut cow = Cow::from_mut_page(page);
                let mut pinpointed = 0;
                while current > 0 {
                    if current > 4 {
                        let p = p.offset(current);
                        let right_page = u64::from_le(*((p as *const u64).offset(2)));
                        let (key,value) = read_key_value(p);
                        match insert(rng, txn, cow, key, value, right_page) {
                            Result::Ok { page, position } => {
                                if current == pinpoint as isize {
                                    pinpointed = position
                                }
                                cow = Cow::from_mut_page(page)
                            },
                            _ => unreachable!()
                        }
                    }
                    current = u16::from_le(*((cow.data() as *const u8).offset(current) as *const u16)) as isize;
                }
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
    let mut current = page.offset(current_off as isize) as *mut u16;

    let mut next_page = 0; // Next page to explore.

    let size = record_size(key.len(), value.len() as usize);
    {
        let off = page.can_alloc(size);
        if off > 0 {
            // We'll need to mute something here anyway, whether or not we're a page.
            let (page_, _) = cow_pinpointing(rng, txn, page, 0);
            page = Cow::from_mut_page(page_)
        }
    }

    loop {
        // advance in the list until there's nothing more to do.
        loop {
            let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
            if next == 0 {
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
    if next_page > 0 {
        let next_page = txn.load_cow_page(next_page);
        match insert(rng, txn, next_page, key, value, right_page) {
            Result::Ok { page:next_page, .. } => {
                let (page, current_off) = cow_pinpointing(rng, txn, page, current_off);
                let current = page.offset(current_off as isize);
                *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                Result::Ok { page:page, position: 0 }
            },
            Result::Split { key_ptr,key_len,value,left,right,free_page } => {
                *((current as *mut u64).offset(2)) = 0; //left.page_offset().to_le();
                let key = std::slice::from_raw_parts(key_ptr,key_len);
                let result = insert(rng,txn,page,key, value, right.page_offset());
                *((current as *mut u64).offset(2)) = left.page_offset().to_le();
                transaction::free(&mut txn.txn, free_page);
                result
            }
        }
    } else {
        let off = page.can_alloc(size);
        if off > 0 {
            // If there's enough space, copy the page and reinsert between current_off and next.
            let current = page.offset(current_off as isize) as *mut u16;
            let next = u16::from_le(*current);
            let off = page.can_alloc(size);
            let mut page = page.unwrap_mut();
            page.alloc_key_value(off, size, key.as_ptr(), key.len(), value);
            *((page.offset(off as isize) as *mut u64).offset(2)) = right_page.to_le();
            *(page.offset(off as isize) as *mut u16) = next.to_le();
            *current = off.to_le();

            // Add to upper levels
            level = 1;
            debug!("levels = {:?}", &levels[..]);
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
            // Not enough space, split.
            split_page(rng, txn, &page, size as usize)
        }
    }
}

unsafe fn split_page<R:Rng>(rng:&mut R, txn:&mut MutTxn,page:&Cow,size:usize)->Result {
    let mut left = txn.alloc_page();
    left.init();
    let mut left_bytes = 32;
    let mut current = 8;
    let mut cow_left = Cow::from_mut_page(left);
    while left_bytes + (size as usize) < PAGE_SIZE && left_bytes < PAGE_SIZE/2 && current > 0 {
        if current > 8 {
            let p = page.offset(current);
            let right_page = u64::from_le(*((p as *const u64).offset(2)));
            let (key,value) = {
                let (key,value) = read_key_value(p);
                (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
            };
            let size = record_size(key.len(), value.len() as usize);
            match insert(rng, txn, cow_left, key, value, right_page) {
                Result::Ok { page,.. } => cow_left = Cow::from_mut_page(page),
                _ => unreachable!()
            }
            left_bytes += size as usize;
        }
        current = u16::from_le(*((page.data() as *const u8).offset(current) as *const u16)) as isize;
    }
    let middle = current;
    debug_assert!(middle>0);
    // move on to next
    current = u16::from_le(*((page.data() as *const u8).offset(current) as *const u16)) as isize;

    let mut right = txn.alloc_page();
    right.init();
    let mut cow_right = Cow::from_mut_page(right);
    while current != 0 {
        if current > 8 {
            let p = page.offset(current);
            let right_page = u64::from_le(*((p as *const u64).offset(2)));
            let (key,value) = {
                let (key,value) = read_key_value(p);
                (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
            };
            match insert(rng, txn, cow_right, key, value, right_page) {
                Result::Ok { page,.. } => cow_right = Cow::from_mut_page(page),
                _ => unreachable!()
            }
        }
        current = u16::from_le(*((page.data() as *const u8).offset(current) as *const u16)) as isize;
    }
    let p = page.offset(middle as isize);
    let (key_ptr,key_len,value) = {
        let (key,value) = read_key_value(p);
        (key.as_ptr(),key.len(),value)
    };
    let left = cow_left.unwrap_mut();
    let right = cow_right.unwrap_mut();
    Result::Split {
        key_ptr: key_ptr,
        key_len: key_len,
        value: value,
        left: left,
        right: right,
        free_page: page.page_offset()
    }
}

// This function deals with the case where the main page split, either during insert, or during delete.
fn root_split<R:Rng>(rng:&mut R, txn: &mut MutTxn, x:Result) -> Db {
    if let Result::Split { left,right,key_ptr,key_len,value,free_page } = x {
        let mut page = txn.alloc_page();
        page.init();
        unsafe {
            let key = std::slice::from_raw_parts(key_ptr,key_len);
            let right_offset = right.page_offset();
            let ins = insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset);
            transaction::free(&mut txn.txn, free_page);
            match ins {
                Result::Ok { page,.. } => {
                    *((page.data() as *mut u64).offset(3)) = left.page_offset().to_le();
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
                if next == 0 {
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
                if current_off > 0 {
                    let current = page.offset(current_off as isize) as *mut u16;
                    let next_off = u16::from_le(*(current.offset(level as isize)));
                    let next = page.offset(next_off as isize) as *mut u16;
                    if next_off == equal {
                        // Delete the entry at this level.
                        let next_next_off = *(next.offset(level as isize));
                        if level == 0 && next_next_off == 0 && current_off == FIRST_HEAD {
                            page_becomes_empty = true;
                            break;
                        } else {
                            *current.offset(level as isize) = next_next_off;
                        }
                    }
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
                    while off != 0 {
                        let ptr = (page.offset(off as isize) as *mut u16).offset(level as isize);
                        let next = u16::from_le(*ptr);
                        if next == current_off {
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
