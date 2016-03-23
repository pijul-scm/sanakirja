use super::txn::*;
use super::transaction::PAGE_SIZE;
use std;
use std::cmp::Ordering;
use super::transaction;
use rand::{Rng};

const FIRST_HEAD:u16 = 8;
const MAX_LEVEL:isize = 4;

enum Result {
    Ok { page: MutPage, position:u16, skip:bool },
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
                        match insert(rng, txn, cow, key, value, right_page, FIRST_HEAD, MAX_LEVEL) {
                            Result::Ok { page,position,.. } => {
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

// Important: page needs to be Cow and not MutPage, because a
// MutPage would require a copy, and in the case of a split that would
// mean copying the contents twice.
//
// Note: if right_page!=0, this function inserts (key,value) in this
// page (i.e. not in a child) (possibly returning Result::Split).
unsafe fn insert<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow, key:&[u8],value:UnsafeValue,right_page:u64, current_off:u16, level:isize) -> Result {
    // Is (key,value) greater than the next element?
    let current = page.offset(current_off as isize) as *mut u16;
    let next = u16::from_le(*(current.offset(level)));
    //debug!("put: current={:?}, level={:?}, next= {:?}", current, level, next);
    let continue_ = if next == 0 {
        false
    } else {
        let next_ptr = page.offset(next as isize);
        let (next_key,next_value) = read_key_value(next_ptr);
        match key.cmp(next_key) {
            Ordering::Less => false,
            Ordering::Equal => {
                match (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:next_value}) {
                    Ordering::Less => false,
                    Ordering::Equal => false,
                    Ordering::Greater => true
                }
            },
            Ordering::Greater => true
        }
    };
    if continue_ {
        // Here, next > 0 and key > next_key. We move on to the next element at the same level.
        insert(rng,txn,page,key,value,right_page,next,level)
    } else {
        // Here, either there's no next element, or key <= next_key.
        if level>0 {
            // If we're not at the bottom level, go down...
            let ins = insert(rng,txn,page,key,value,right_page,current_off,level-1);
            match ins {
                Result::Ok { page, position, skip } => {
                    // ...and update the list at this level randomly,
                    // only if the previous level was updated (denoted by
                    // skip==true).
                    if skip && rng.gen() {
                        *(current.offset(level)) = position.to_le();
                        *(page.offset(position as isize + 2*level) as *mut u16) = next.to_le();
                        Result::Ok { page:page, position:position, skip:true }
                    } else {
                        Result::Ok { page:page, position:position, skip:false }
                    }
                },
                ins => ins
            }
        } else {
            let next_page = u64::from_le(*((current as *const u64).offset(2)));
            debug!("next_page = {:?} {:?}",(current as *const u64).offset(2), next_page);
            if next_page == 0 || right_page > 0 {
                let size = record_size(key.len(), value.len() as usize);
                let off = page.can_alloc(size);
                debug!("can_alloc = {:?}, value={:?}",off,value);
                if off > 0 {
                    // If there's enough space, copy the page and reinsert between current_off and next.
                    let (mut page,current_off) = cow_pinpointing(rng, txn, page, current_off);
                    let current = page.offset(current_off as isize) as *mut u16;
                    let next = u16::from_le(*(current.offset(level)));
                    let off = page.can_alloc(size);
                    page.alloc_key_value(off, size, key.as_ptr(), key.len(), value);
                    *((page.offset(off as isize) as *mut u64).offset(2)) = right_page.to_le();
                    *(page.offset(off as isize) as *mut u16) = next.to_le();
                    *current = off.to_le();
                    // Return the position of the new allocation, and
                    // inform the level above that this list was
                    // updated (skip = true).
                    Result::Ok { page:page, position:off, skip:true }
                } else {
                    // Not enough space, split.
                    split_page(rng, txn, &page, size as usize)
                }
            } else {
                debug!("this page: {:?}", page);
                let next_page = txn.load_cow_page(next_page);
                match insert(rng,txn,next_page,key,value,right_page, FIRST_HEAD, MAX_LEVEL) {
                    Result::Ok { page:next_page,.. } => {
                        let (page,current_off) = cow_pinpointing(rng, txn, page, current_off);
                        let current = page.offset(current_off as isize) as *mut u16;
                        *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                        Result::Ok { page:page, skip:false, position:current_off }
                    },
                    Result::Split { key_ptr,key_len,value, left,right,free_page } => {
                        debug!("free_page: {:?}", free_page);
                        let (page,current_off) = cow_pinpointing(rng, txn, page, current_off);
                        debug!("cow_page: {:?}", page);
                        let current = page.offset(current_off as isize);
                        *((current as *mut u64).offset(2)) = left.page_offset().to_le();
                        let key = std::slice::from_raw_parts(key_ptr,key_len);
                        let right_offset = right.page_offset();
                        let ins = insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset, FIRST_HEAD, MAX_LEVEL);
                        transaction::free(&mut txn.txn, free_page);
                        ins
                    }
                }
            }
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
            match insert(rng, txn, cow_left, key, value, right_page, FIRST_HEAD, MAX_LEVEL) {
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
            match insert(rng, txn, cow_right, key, value, right_page, FIRST_HEAD, MAX_LEVEL) {
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
            let ins = insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset, FIRST_HEAD, MAX_LEVEL);
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
        match insert(rng, txn, root_page, key, value, 0, FIRST_HEAD, MAX_LEVEL) {
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


// deletes one entry from a page and its children, as instructed by argument comp.
// Returns:
// - Some((Result::Ok,..)) with the position of the deleted element if the element was found. Field "position" is 1 if the deletion occurred in a different page (1 is an invalid index anyway, and whenever we test for equality to rebuild the list, it is different from "Nil", which is encoded by 0).
// - Some((Result::Split,..)) if the replacement of a root during the deletion process, caused the page given as argument to split.
// - None if the requested key wasn't found.
unsafe fn delete<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow, comp:C, current_off:u16, level:isize) -> Option<(Result,Option<Smallest>)> {

    let current = page.offset(current_off as isize) as *mut u16; // current block (64-bits aligned).
    let next = u16::from_le(*(current.offset(level))); // next in the list at the current level.
    let mut equal = false;
    let continue_ =
        if let C::KV { key,value } = comp {
            if next == 0 {
                false
            } else {
                let next_ptr = page.offset(next as isize);
                let (next_key,next_value) = read_key_value(next_ptr);
                match key.cmp(next_key) {
                    Ordering::Less => false,
                    Ordering::Equal => {
                        match (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:next_value}) {
                            Ordering::Less => false,
                            Ordering::Equal => {
                                equal = true;
                                false
                            },
                            Ordering::Greater => true
                        }
                    },
                    Ordering::Greater => true
                }
            }
        } else {
            //println!("deleting smallest element in page {:?}", page);
            equal = true;
            false
        };
    if continue_ {
        // key > next_key, et next > 0
        let deleted = delete(rng,txn,page,comp,next,level);
        match deleted {
            Some((Result::Ok { page, position, skip },smallest)) => {
                if position == next {
                    let next_next = u16::from_le(*(page.offset(next as isize + 2*level) as *const u16));
                    *(current.offset(level)) = next_next.to_le();
                }
                Some((Result::Ok { page:page,position:position, skip:skip },smallest))
            },
            Some(_) => unreachable!(),
            None => None
        }
    } else {
        // pas de next_ptr, ou key <= next_key.
        if level>0 {
            let deleted = delete(rng,txn,page,comp,current_off,level-1);
            match deleted {
                Some((Result::Ok { page, position, skip },smallest)) => {
                    if position == next {
                        let next_next = u16::from_le(*(page.offset(next as isize + 2*level) as *const u16));
                        *(current.offset(level)) = next_next.to_le();
                    }
                    Some((Result::Ok { page:page,position:position, skip:skip },smallest))
                },
                Some(_) => unreachable!(),
                None => None
            }
        } else {
            // level == 0, key <= next_key, key > key(current)
            let next_page = u64::from_le(*((current as *const u64).offset(2)));
            let del = 
                if next_page > 0 {
                    let next_page = txn.load_cow_page(next_page);
                    delete(rng,txn,next_page,comp, FIRST_HEAD, MAX_LEVEL)
                } else {
                    None
                };
            match del {
                Some((Result::Ok { page:next_page,.. }, smallest)) => {
                    let (page,current_off) = cow_pinpointing(rng,txn,page,current_off);
                    let current = page.offset(current_off as isize) as *mut u16;
                    *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                    Some((Result::Ok { page: page, position:0, skip:false },smallest))
                },
                Some((Result::Split { key_ptr,key_len,value, left,right,free_page },smallest)) => {
                    let (page,current_off) = cow_pinpointing(rng, txn, page, current_off);
                    let current = page.offset(current_off as isize);
                    *((current as *mut u64).offset(2)) = left.page_offset().to_le();
                    let key = std::slice::from_raw_parts(key_ptr,key_len);
                    let right_offset = right.page_offset();
                    let ins = insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset, FIRST_HEAD, MAX_LEVEL);
                    transaction::free(&mut txn.txn, free_page);
                    Some((ins,smallest))
                },
                None => {
                    // not found in the child page.
                    if equal {
                        debug!("deleting, next_page = {:?}",next_page);
                        if next_page == 0 {
                            // found!
                            let (page,current_off) = cow_pinpointing(rng,txn,page,current_off);
                            let current = page.offset(current_off as isize) as *mut u16;
                            let next = u16::from_le(*(current.offset(level)));
                            let (key_ptr,key_len,value) = {
                                let next_ptr = page.offset(next as isize);
                                let (key,value) = read_key_value(next_ptr);
                                (key.as_ptr(), key.len(), value)
                            };
                            let next_next = u16::from_le(*(page.offset(next as isize) as *const u16));
                            *current = next_next.to_le();
                            if current_off == FIRST_HEAD && next_next == 0 {
                                // This means we're deleting the last element on the page.
                                // return position = 2: this was the last element.
                                let page_offset = page.page_offset();
                                Some((Result::Ok { page: page, position:2, skip:false },
                                      if comp.is_smallest() {
                                          Some(Smallest {
                                              key_ptr: key_ptr,
                                              key_len: key_len,
                                              value: value,
                                              free_page: page_offset,
                                              reinsert_page:0
                                          })
                                      } else {
                                          transaction::free(&mut txn.txn,page_offset);
                                          None
                                      }))
                            } else {
                                Some((Result::Ok { page: page, position:next, skip:false },
                                      if comp.is_smallest() {
                                          Some(Smallest {
                                              key_ptr: key_ptr,
                                              key_len: key_len,
                                              value: value,
                                              free_page: 0,
                                              reinsert_page:0
                                          })
                                      } else {
                                          None
                                      }))
                            }
                        } else {
                            let next_page = {
                                let next_ptr = page.offset(next as isize);
                                let next_page = u64::from_le(*((next_ptr as *const u64).offset(2)));
                                txn.load_cow_page(next_page)
                            };
                            match delete(rng,txn,next_page,C::Smallest, FIRST_HEAD, MAX_LEVEL) {
                                Some((Result::Ok { page:next_page,position,.. }, Some(mut smallest))) => {
                                    // Remark: here, either we're at
                                    // the start of the page, or else
                                    // we're not looking for the
                                    // smallest element.
                                    //
                                    // If the page below becomes
                                    // empty, we can just reinsert
                                    // "smallest" in this page,
                                    // deleting the current key.
                                    let (page,current_off) = cow_pinpointing(rng,txn,page,current_off);
                                    let current = page.offset(current_off as isize);
                                    let next = u16::from_le(*(current as *const u16));
                                    let next_next = u16::from_le(*(page.offset(next as isize) as *const u16));
                                    *(current as *mut u16) = next_next.to_le();
                                    if position == 2 {
                                        // the next page vanished.
                                        smallest.free_page = next_page.page_offset();
                                    } else {
                                        smallest.reinsert_page = next_page.page_offset();
                                    }
                                    Some((Result::Ok { page:page, position: next, skip:false }, Some(smallest)))
                                }
                                None => None,
                                _ => unreachable!() // Deleting the smallest element involves no reinsertion, hence no split.
                            }
                        }
                    } else {
                        None
                    }
                }
            }
        }
    }
}


pub fn del<R:Rng>(rng:&mut R, txn:&mut MutTxn, db:Db, key:&[u8], value:Option<&[u8]>) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = value.unwrap();
    let value = UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 };
    unsafe {
        match delete(rng,txn, root_page, C::KV { key:key, value:value }, FIRST_HEAD, MAX_LEVEL) {
            Some((Result::Ok { page,.. },Some(reinsert))) => {
                let key = std::slice::from_raw_parts(reinsert.key_ptr,reinsert.key_len);
                assert!(key.len() < MAX_KEY_SIZE);
                match insert(rng, txn, Cow::from_mut_page(page), key, reinsert.value, reinsert.reinsert_page, FIRST_HEAD, MAX_LEVEL) {
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
            Some((Result::Ok { page,.. },None)) => {
                Db { root:page.page_offset() }
            },
            Some((x,_)) => {
                root_split(rng,txn,x)
            },
            None => db
        }
    }
}
