use super::txn::*;
use super::transaction::PAGE_SIZE;
use std;
use std::cmp::Ordering;
use super::transaction;
use rand::{Rng};

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
                        let (key,value) = {
                            let (key,value) = txn.read_key_value(p);
                            (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
                        };
                        match page_insert(rng, txn, cow, key, value, right_page) {
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

/// If right_page!=0, this function "tries" to insert (key,value) in this page (i.e. not in a child).
fn page_insert<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow, key:&[u8],value:UnsafeValue,right_page:u64) -> Result {
    unsafe fn insert<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow, current_off:u16, level:isize, key:&[u8],value:UnsafeValue, right_page:u64) -> Result {
        // Is (key,value) greater than the next element?
        let current = page.offset(current_off as isize) as *mut u16;
        let next = u16::from_le(*(current.offset(level)));
        //debug!("put: current={:?}, level={:?}, next= {:?}", current, level, next);
        let continue_ = if next == 0 {
            false
        } else {
            let next_ptr = page.offset(next as isize);
            let (next_key,next_value) = txn.read_key_value(next_ptr);
            if key <= next_key {
                false
            } else {
                true
            }
        };
        if continue_ {
            // key > next_key, et next > 0
            insert(rng,txn,page,next,level,key,value,right_page)
        } else {
            // pas de next_ptr, ou key <= next_key.
            if level>0 {
                let ins = insert(rng,txn,page,current_off,level-1,key,value,right_page);
                match ins {
                    Result::Ok { page, position, skip } => {
                        if skip {
                            // create fast lane on top of off
                            *(current.offset(level)) = position.to_le();
                            //debug!("{:?}",current.offset(level));
                            *(page.offset(position as isize + 2*level) as *mut u16) = next.to_le();
                            Result::Ok { page:page, skip: rng.gen(), position:position }
                        } else {
                            Result::Ok { page:page, position:position, skip:skip }
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
                        let (mut page,_) = cow_pinpointing(rng, txn, page, 0);
                        let off = page.can_alloc(size);
                        page.alloc_key_value(off, size, key.as_ptr(), key.len(), value);
                        //println!("alloc, next_page = {:?}", page.offset(off as isize));
                        *((page.offset(off as isize) as *mut u64).offset(2)) = right_page.to_le();
                        //debug!("alloc: {:?}",off);
                        // insert
                        *(page.offset(off as isize) as *mut u16) = next.to_le();
                        //debug!("{:?}",current);
                        *current = off.to_le();
                        // random number
                        Result::Ok { page:page, skip:rng.gen(), position:off }
                    } else {
                        // Split !
                        split_page(rng, txn, &page, size as usize)
                    }
                } else {
                    debug!("this page: {:?}", page);
                    let next_page = txn.load_cow_page(next_page);
                    match page_insert(rng,txn,next_page,key,value,right_page) {
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
                            let ins = page_insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset);
                            transaction::free(&mut txn.txn, free_page);
                            ins
                        }
                    }
                }
            }
        }
    }
    unsafe {
        insert(rng,
               txn,
               page,
               8,
               4,
               key,value,right_page)
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
                let (key,value) = txn.read_key_value(p);
                (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
            };
            let size = record_size(key.len(), value.len() as usize);
            match page_insert(rng, txn, cow_left, key, value, right_page) {
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
                let (key,value) = txn.read_key_value(p);
                (std::slice::from_raw_parts(key.as_ptr(), key.len()), value)
            };
            match page_insert(rng, txn, cow_right, key, value, right_page) {
                Result::Ok { page,.. } => cow_right = Cow::from_mut_page(page),
                _ => unreachable!()
            }
        }
        current = u16::from_le(*((page.data() as *const u8).offset(current) as *const u16)) as isize;
    }
    println!("SPLIT middle = {:?}", middle);
    let p = page.offset(middle as isize);
    let (key_ptr,key_len,value) = {
        let (key,value) = txn.read_key_value(p);
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

// Assumes result is of type Split.
fn root_split<R:Rng>(rng:&mut R, txn: &mut MutTxn, x:Result) -> Db {
    if let Result::Split { left,right,key_ptr,key_len,value,free_page } = x {
        println!("SPLIT, value = {:?}",value);
        let mut page = txn.alloc_page();
        page.init();
        println!("left = {:?}", left.page_offset());
        println!("right = {:?}", right.page_offset());
        unsafe {
            let key = std::slice::from_raw_parts(key_ptr,key_len);
            let right_offset = right.page_offset();
            println!("insert");
            let ins = page_insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset);
            println!("/insert");
            transaction::free(&mut txn.txn, free_page);
            match ins {
                Result::Ok { page,.. } => {
                    println!("WRITING LEFT in {:?}", (page.data() as *mut u64).offset(3));
                    *((page.data() as *mut u64).offset(3)) = left.page_offset().to_le();
                    Db { root:page.page_offset() }
                },
                _ => unreachable!()
            }
        }
    } else {
        unreachable!()
    }
}

pub fn put<R:Rng>(rng:&mut R, txn: &mut MutTxn, db: Db, key: &[u8], value: &[u8]) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = if value.len() > VALUE_SIZE_THRESHOLD {
        alloc_value(txn,value)
    } else {
        UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
    };
    debug!("value = {:?}", Value { txn:txn,value:value });
    match page_insert(rng, txn, root_page, key, value, 0) {
        Result::Ok { page,.. } => Db { root:page.page_offset() },
        x => {
            root_split(rng,txn,x)
        }
    }
}

#[derive(Copy,Clone,Debug)]
enum C<'a> {
    KV { key:&'a [u8], value:UnsafeValue },
    Smallest
}
impl<'a> C<'a> {
    fn is_smallest(&self)->bool {
        match self {
            &C::Smallest => true,
            _ => false
        }
    }
}
struct Smallest {
    key_ptr:*const u8,
    key_len:usize,
    value:UnsafeValue,
    reinsert_page:u64
}

fn page_delete<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow, comp:C) -> Option<(Result,Option<Smallest>)> {

    unsafe fn delete<R:Rng>(rng:&mut R, txn:&mut MutTxn, page:Cow,
                            current_off:u16, level:isize, comp:C) -> Option<(Result,Option<Smallest>)> {

        //println!("delete: {:?}", comp);
        let current = page.offset(current_off as isize) as *mut u16;
        let next = u16::from_le(*(current.offset(level)));
        //debug!("put: current={:?}, level={:?}, next= {:?}", current, level, next);
        let mut equal = false;
        let continue_ =
            if let C::KV { key,value } = comp {
                if next == 0 {
                    false
                } else {
                    let next_ptr = page.offset(next as isize);
                    let (next_key,next_value) = txn.read_key_value(next_ptr);
                    match key.cmp(next_key) {
                        Ordering::Less => false,
                        Ordering::Equal => {
                            equal = true;
                            false
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
            let deleted = delete(rng,txn,page,next,level,comp);
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
                let deleted = delete(rng,txn,page,current_off,level-1,comp);
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
                        page_delete(rng,txn,next_page,comp)
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
                        let ins = page_insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset);
                        transaction::free(&mut txn.txn, free_page);
                        Some((ins,smallest))
                    },
                    None => {
                        // not found in the child page.
                        if equal {
                            println!("deleting, next_page = {:?}",next_page);
                            if next_page == 0 {
                                // found!
                                let (page,current_off) = cow_pinpointing(rng,txn,page,current_off);
                                let current = page.offset(current_off as isize) as *mut u16;
                                let next = u16::from_le(*(current.offset(level)));
                                let (key_ptr,key_len,value) = {
                                    let next_ptr = page.offset(next as isize);
                                    let (key,value) = txn.read_key_value(next_ptr);
                                    (key.as_ptr(), key.len(), value)
                                };
                                let next_next = u16::from_le(*(page.offset(next as isize) as *const u16));
                                *current = next_next.to_le();
                                Some((Result::Ok { page: page, position:next, skip:false },
                                      if comp.is_smallest() {
                                          Some(Smallest {
                                              key_ptr: key_ptr,
                                              key_len: key_len,
                                              value: value,
                                              reinsert_page:0
                                          })
                                      } else {
                                          None
                                      }))
                            } else {
                                let next_page = {
                                    let next_ptr = page.offset(next as isize);
                                    let next_page = u64::from_le(*((next_ptr as *const u64).offset(2)));
                                    txn.load_cow_page(next_page)
                                };
                                match page_delete(rng,txn,next_page,C::Smallest) {
                                    Some((Result::Ok { page:next_page,.. }, Some(mut smallest))) => {

                                        let (page,current_off) = cow_pinpointing(rng,txn,page,current_off);
                                        let current = page.offset(current_off as isize);
                                        let next = u16::from_le(*(current as *const u16));
                                        let next_next = u16::from_le(*(page.offset(next as isize) as *const u16));
                                        *(current as *mut u16) = next_next.to_le();
                                        smallest.reinsert_page = next_page.page_offset();
                                        Some((Result::Ok { page:page, position: next, skip:false },Some(smallest)))
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
    unsafe {
        delete(rng,
               txn,
               page,
               8,
               4,
               comp)
    }
}




pub fn del<R:Rng>(rng:&mut R, txn:&mut MutTxn, db:Db, key:&[u8], value:Option<&[u8]>) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = value.unwrap();
    let value = if value.len() > VALUE_SIZE_THRESHOLD {
        alloc_value(txn,value)
    } else {
        UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
    };
    match page_delete(rng,txn, root_page, C::KV { key:key, value:value }) {
        Some((Result::Ok { page,.. },Some(reinsert))) => {
            unsafe {
                let key = std::slice::from_raw_parts(reinsert.key_ptr,reinsert.key_len);
                assert!(key.len() < MAX_KEY_SIZE);
                match page_insert(rng, txn, Cow::from_mut_page(page), key, reinsert.value, reinsert.reinsert_page) {
                    Result::Ok { page,.. } => Db { root:page.page_offset() },
                    x => {
                        root_split(rng,txn,x)
                    }
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
