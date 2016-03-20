use super::txn::*;
use super::transaction::PAGE_SIZE;
use std;
use std::cmp::Ordering;
use super::transaction;
use std::marker::PhantomData;
use rand::{Rng,SeedableRng,ThreadRng,thread_rng};
use rand;

enum Result {
    Ok { page: MutPage, skip:u16 },
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
                        let size = record_size(key.len(), value.len() as usize);
                        match page_insert(rng, txn, cow, key, value, right_page) {
                            Result::Ok { page,skip } => {
                                if current == pinpoint as isize {
                                    pinpointed = skip
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
                    Result::Ok { page, skip } => {
                        if skip > 0 {
                            // create fast lane on top of off
                            *(current.offset(level)) = skip.to_le();
                            //debug!("{:?}",current.offset(level));
                            *(page.offset(skip as isize + 2*level) as *mut u16) = next.to_le();
                            Result::Ok { page:page, skip: if rng.gen() { skip } else { 0 } }
                        } else {
                            Result::Ok { page:page,skip:skip }
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
                        Result::Ok { page:page, skip:if rng.gen() { off } else { 0 } }
                    } else {
                        // Split !
                        split_page(rng, txn, &page, key, value, size as usize)
                    }
                } else {
                    debug!("this page: {:?}", page);
                    let next_page = txn.load_cow_page(next_page);
                    match page_insert(rng,txn,next_page,key,value,right_page) {
                        Result::Ok { page:next_page,.. } => {
                            let (mut page,current_off) = cow_pinpointing(rng, txn, page, current_off);
                            let current = page.offset(current_off as isize) as *mut u16;
                            *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                            Result::Ok { page:page, skip:0 }
                        },
                        Result::Split { key_ptr,key_len,value, left,right,free_page } => {
                            debug!("free_page: {:?}", free_page);
                            let (mut page,current_off) = cow_pinpointing(rng, txn, page, current_off);
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

unsafe fn split_page<R:Rng>(rng:&mut R, txn:&mut MutTxn,page:&Cow,key:&[u8],value:UnsafeValue,size:usize)->Result {
    let mut left = txn.alloc_page();
    left.init();
    let mut left_bytes = 32;
    let mut current = 8;
    let mut current_pointer = left.offset(32);
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
    let mut current_pointer = right.offset(32);
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
        key_ptr: key.as_ptr(),
        key_len: key.len(),
        value: value,
        left: left,
        right: right,
        free_page: page.page_offset()
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
        //Result::Split { .. } => {
        Result::Split { left,right,key_ptr,key_len,value,free_page } => {
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
        }
    }
}



fn page_delete(txn:&mut MutTxn, page:MutPage, key:&[u8],value:UnsafeValue) -> Option<Result> {
    unsafe fn delete(txn:&mut MutTxn, page:*mut u8, current_off:u16, level:isize, key:&[u8], value:UnsafeValue) -> Option<u16> {
        // Is (key,value) greater than the next element?
        let current = page.offset(current_off as isize) as *mut u16;
        let next = u16::from_le(*(current.offset(level)));
        //debug!("put: current={:?}, level={:?}, next= {:?}", current, level, next);
        let mut equal = false;
        let continue_ = if next == 0 {
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
        };
        if continue_ {
            // key > next_key, et next > 0
            let deleted = delete(txn,page,next,level,key,value);
            if let Some(deleted) = deleted {
                if deleted == next {
                    let next_next = u16::from_le(*(page.offset(next as isize + 2*level) as *const u16));
                    *(current.offset(level)) = next_next.to_le();
                }
            }
            deleted
        } else {
            // pas de next_ptr, ou key <= next_key.
            if level>0 {
                let deleted = delete(txn,page,current_off,level-1,key,value);
                if let Some(deleted) = deleted {
                    if deleted == next {
                        let next_next = u16::from_le(*(page.offset(next as isize + 2*level) as *const u16));
                        *(current.offset(level)) = next_next.to_le();
                    }
                }
                deleted
            } else {
                // level == 0, key <= next_key, key > key(current)
                let mut next_page = u64::from_le(*((current as *const u64).offset(2)));
                let del = 
                    if next_page > 0 {
                        let next_page = txn.load_cow_page(next_page);
                        let mut next_page = next_page.into_mut_page(txn);
                        page_delete(txn,next_page,key,value)
                    } else {
                        None
                    };
                match del {
                    Some(Result::Ok { page:next_page,.. }) => {
                        *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                        None
                    },
                    Some(Result::Split { .. }) => {
                        unimplemented!()
                    },
                    None => {
                        // not found in the child page.
                        if equal {
                            // found!
                            let next_next = u16::from_le(*(page.offset(next as isize) as *const u16));
                            *current = next_next.to_le();
                            Some(next)
                        } else {
                            None
                        }
                    }
                }
            }
        }
    }
    unsafe {
        if let Some(_) = delete(txn,
                                page.data() as *mut u8,
                                4,
                                4,
                                key,value) {
            Some(Result::Ok { page: page, skip:0 })
        } else {
            None
        }
    }
}


pub fn del(txn:&mut MutTxn, db:Db, key:&[u8], value:Option<&[u8]>) -> Db {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
    let value = value.unwrap();
    let value = if value.len() > VALUE_SIZE_THRESHOLD {
        alloc_value(txn,value)
    } else {
        UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
    };
    let mut page = root_page.into_mut_page(txn);
    match page_delete(txn, page, key, value) {
        Some(Result::Ok { page,.. }) => {
            Db { root:page.page_offset() }
        },
        Some(Result::Split { .. }) => {
            unimplemented!()
        },
        None => db
    }
}
