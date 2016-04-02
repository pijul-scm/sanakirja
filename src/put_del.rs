use super::txn::*;
use super::transaction::{PAGE_SIZE,Error};
use std;
use std::cmp::Ordering;
use super::transaction;
use rand::{Rng};


enum Res {
    Ok { page: MutPage,
         // position is the offset in the page where the insertion
         // happened (cow_pinpointing uses that information to
         // pinpoint stuff), or in the case of deletions, it is a code
         // describing what happened to the page below.
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


pub fn fork_db<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64) -> Result<u64,Error> {
    match txn.load_cow_page(off).cow {
        transaction::Cow::Page(p) => {
            incr_rc(rng,txn,p.offset);
            Ok(off)
        },
        transaction::Cow::MutPage(p) => {
            incr_rc(rng,txn,p.offset);
            let (page,_) = try!(cow_pinpointing(rng, txn, Cow { cow:transaction::Cow::Page(p.as_page()) }, 0));
            Ok(page.page_offset())
        }
    }
}

/// Increase the reference count of a page.
fn incr_rc<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64)->Result<(),Error> {
    let mut rc = if let Some(rc) = txn.rc() { rc } else { try!(txn.create_db()) };
    let count = txn.get_u64(&rc, off).unwrap_or(1);
    txn.replace_u64(rng, &mut rc, off, count+1);
    txn.set_rc(rc);
    Ok(())
}

/// Get the reference count of a page. Returns 0 if the page is not reference-counted.
fn get_rc<T>(txn:&mut MutTxn<T>, off:u64) -> u64 {
    if let Some(rc) = txn.rc() {
        txn.get_u64(&rc, off).unwrap_or(1)
    } else {
        0
    }
}


/// Decrease the reference count of a page, freeing it if it's no longer referenced.
fn free<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, off:u64) {
    debug!("freeing {:?}", off);
    unsafe {
        let really_free = {
            if let Some(mut rc) = txn.rc() {
                if let Some(count) = txn.get_u64(&rc, off) {
                    if count>1 {
                        debug!("rc: {:?}, off: {:?}, count: {:?}", rc, off, rc);
                        txn.replace_u64(rng, &mut rc, off, count-1);
                        txn.set_rc(rc);
                        false
                    } else {
                        txn.del_u64(rng,&mut rc,off);
                        txn.set_rc(rc);
                        true
                    }
                } else {
                    true
                }
            } else {
                true
            }
        };
        if really_free {
            let p = txn.load_cow_page(off);
            // Decrement all children and values.
            let mut current = FIRST_HEAD;
            while current != NIL {
                let pp = p.offset(current as isize);
                let right_page = u64::from_le(*((pp as *const u64).offset(2)));
                if current > FIRST_HEAD {
                    let (key,value) = read_key_value(pp);
                    // Decrease count of value
                    if let UnsafeValue::O { offset,.. } = value {
                        //free_value(rng, txn, offset)
                    }
                }
                // Decrease count of right_page
                if right_page > 0 {
                    free(rng, txn, right_page)
                }
                current = u16::from_le(*((p.offset(current as isize) as *const u16)));
            }
            transaction::free(&mut txn.txn, off)
        }
    }
}

/// Allocate one large values, spanning over at least one page.
fn alloc_value<T>(txn:&mut MutTxn<T>, value: &[u8]) -> Result<UnsafeValue,Error> {
    debug!("alloc_value");
    let mut len = value.len();
    let mut p_value = value.as_ptr();
    let mut ptr:*mut u64 = std::ptr::null_mut();
    let mut first_page = 0;
    unsafe {
        while len > 0 {
            let page = try!(txn.alloc_page());
            debug!("PAGE= {:?}", page.page_offset());
            if !ptr.is_null() {
                *((ptr as *mut u64)) = page.page_offset().to_le()
            } else {
                first_page = page.page_offset();
            }
            ptr = page.data() as *mut u64;
            *ptr = 0;
            if len > PAGE_SIZE - VALUE_HEADER_LEN {
                std::ptr::copy_nonoverlapping(p_value,
                                              (ptr as *mut u8).offset(VALUE_HEADER_LEN as isize),
                                              PAGE_SIZE - VALUE_HEADER_LEN);
                len -= PAGE_SIZE - VALUE_HEADER_LEN;
                p_value = p_value.offset((PAGE_SIZE-VALUE_HEADER_LEN) as isize);
            } else {
                std::ptr::copy_nonoverlapping(p_value,
                                              (ptr as *mut u8).offset(VALUE_HEADER_LEN as isize),
                                              len);
                len = 0;
            }
        }
    }
    debug_assert!(first_page > 0);
    debug!("/alloc_value");
    Ok(UnsafeValue::O { offset: first_page, len: value.len() as u32 })
}



fn free_value<T,R:Rng>(rng:&mut R, txn:&mut MutTxn<T>, mut offset:u64) {
    debug!("freeing value {:?}", offset);
    let really_free =
        if let Some(mut rc) = txn.rc() {
            if let Some(count) = txn.get_u64(&mut rc, offset) {
                if count>1 {
                    txn.replace_u64(rng, &mut rc, offset, count-1);
                    txn.set_rc(rc);
                    false
                } else {
                    txn.del_u64(rng, &mut rc, offset);
                    txn.set_rc(rc);
                    true
                }
            } else {
                true
            }
        } else {
            true
        };
    if really_free {
        unsafe {
            while offset!=0 {
                let off = offset;
                debug!("free value: {:?}", off);
                let page = txn.load_cow_page(off).data();
                offset = u64::from_le(*(page as *const u64));
                transaction::free(&mut txn.txn, off)
            }
        }
    }
}

/// Turn a Cow into a MutPage, copying it if it's not already mutable. In the case a copy is needed, and argument 'pinpoint' is non-zero, a non-zero offset (in bytes) to the equivalent element in the new page is returned. This can happen for instance because of compaction.
fn cow_pinpointing<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, pinpoint:u16) -> Result<(MutPage,u16),Error> {
    unsafe {
        match page.cow {
            transaction::Cow::Page(p) => {
                let page_offset = p.offset;
                let page_rc = get_rc(txn, page_offset);
                let p = Page { page:p };
                let mut page = try!(txn.alloc_page());
                page.init();
                let mut current = FIRST_HEAD;
                debug!("PINPOINTING: {:?} {:?} {:?}", page, page.first_free(), page.occupied());
                let mut cow = Cow::from_mut_page(page);
                let mut pinpointed = FIRST_HEAD;
                while current != NIL {
                    let pp = p.offset(current as isize);
                    let right_page = u64::from_le(*((pp as *const u64).offset(2)));
                    // Increase count of right_page
                    if right_page > 0 && page_rc > 1 {
                        try!(incr_rc(rng, txn, right_page))
                    }
                    if current > FIRST_HEAD {
                        let (key,value) = read_key_value(pp);
                        // Increase count of value
                        if page_rc > 1 {
                            if let UnsafeValue::O { offset,.. } = value {
                                try!(incr_rc(rng, txn, offset))
                            }
                        }
                        debug!("PINPOINT: {:?}", std::str::from_utf8(key).unwrap());
                        match try!(insert(rng, txn, cow, key, value, right_page, false)) {
                            Res::Ok { page, position } => {
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
                debug!("free cow: {:?}", page_offset);
                if page_rc <= 1 {
                    if page_rc == 1 {
                        let mut rc = txn.rc().unwrap();
                        txn.del_u64(rng, &mut rc, page_offset);
                        txn.set_rc(rc);
                    }
                    transaction::free(&mut(txn.txn),page_offset)
                }
                Ok((cow.unwrap_mut(),pinpointed))
            }
            transaction::Cow::MutPage(p) => Ok((MutPage { page:p }, pinpoint))
        }
    }
}



/// Insert a key and a value to a tree. If right_page != 0, the
/// binding is inserted at the root (possibly splitting it). Else, it
/// is inserted at a leaf.
unsafe fn insert<R:Rng,T>(
    rng:&mut R, txn:&mut MutTxn<T>,
    mut page:Cow, // Root of the current subtree
    key:&[u8], value:UnsafeValue, right_page:u64,
    mut needs_copying:bool // At least one mutable page with RC >= 2 was traversed from the root, hence even mutable pages need to be copied.
) -> Result<Res,Error> {

    let mut levels:[u16;MAX_LEVEL+1] = [0;MAX_LEVEL+1];
    let mut level = MAX_LEVEL;
    let mut current_off = FIRST_HEAD;

    let mut next_page = 0; // Next page to explore, set during the traversal.

    let size = record_size(key.len(), value.len() as usize);
    needs_copying = needs_copying || get_rc(txn,page.page_offset()) >= 2;
    {
        let is_leaf = (*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2))) == 0;
        let off = page.can_alloc(size);
        debug!("INSERT, off = {:?}, {:?} {:?}, right_page={:?}, is_leaf={:?}", off, page, page.occupied(), right_page, is_leaf);

        // If there is enough space on this page, and either
        // right_page!=0, or else this page is a leaf, the page needs to
        // be made mutable, by calling cow_pinpointing, before we start to
        // search the skip list.
        if off > 0 && (right_page>0 || is_leaf) {
            if !needs_copying && off + size < PAGE_SIZE as u16 {
                // No need to copy nor compact the page, the value can be written right away.
                let (page_, _) = try!(cow_pinpointing(rng, txn, page, 0));
                page = Cow::from_mut_page(page_)
            } else {
                // Either this page is referenced several times, or it is
                // scarce (some keys and values were deleted in the middle
                // of the page). We need to copy the page, even if it is
                // mutable (i.e. if it was allocated by this transaction).
                let (page_, _) = try!(cow_pinpointing(rng, txn, page.as_nonmut(), 0));
                page = Cow::from_mut_page(page_)
            }
        }
    }
    let mut current = page.offset(current_off as isize) as *mut u16;
    debug!("INSERT: {:?} {:?} {:?} {:?}",page,current_off, right_page, std::str::from_utf8(key).unwrap());
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
        match try!(insert(rng, txn, next_page, key, value, right_page, needs_copying)) {
            Res::Ok { page:next_page, .. } => {
                let (page, current_off) = try!(cow_pinpointing(rng, txn, page, current_off));
                let current = page.offset(current_off as isize);
                *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                Ok(Res::Ok { page:page, position: NIL })
            },
            Res::Split { key_ptr,key_len,value:value_,left,right,free_page } => {

                let size = record_size(key_len, value_.len() as usize);
                let off = page.can_alloc(size);
                // If there's enough space here, just go on inserting.
                if off > 0 {
                    let (page, current_off) = try!(cow_pinpointing(rng, txn, page, current_off));
                    let current = page.offset(current_off as isize);
                    *((current as *mut u64).offset(2)) = left.page_offset().to_le();
                    let key_ = std::slice::from_raw_parts(key_ptr,key_len);
                    // Then, reinsert (key_,value_) in the current page.
                    let result = insert(rng,txn,Cow::from_mut_page(page),key_, value_, right.page_offset(), needs_copying);
                    debug!("free split 1: {:?}", free_page);
                    free(rng, txn, free_page);
                    result
                } else {
                    // Else, split+translate first, then insert.
                    let key_ = std::slice::from_raw_parts(key_ptr,key_len);
                    let result = split_page(rng, txn, &page,
                                            key_, value_, right.page_offset(),
                                            current_off, left.page_offset());
                    debug!("free split 2: {:?}", free_page);
                    free(rng, txn, free_page);
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
            Ok(Res::Ok { page:page, position:off })
        } else {
            debug!("SPLIT");
            // Not enough space, split.
            split_page(rng, txn, &page, key, value, right_page, 0, 0)
        }
    }
}


/// The arguments to split_page are non-trivial. This function takes a page and an element to insert, to large to fit in the page. It splits the page, inserts the new element, and returns the middle element of the split as a Res::Split { .. }.
unsafe fn split_page<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>,page:&Cow,
                            // (key, value, right_page) of the record to insert.
                            key:&[u8], value:UnsafeValue, right_page:u64,
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
                            translate_index:u16, translate_right_page:u64)->Result<Res,Error> {

    debug!("SPLIT");
    let mut left = try!(txn.alloc_page());
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
            match try!(insert(rng, txn, cow_left, key, value, right_page, false)) {
                Res::Ok { page,.. } => cow_left = Cow::from_mut_page(page),
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

    let mut right = try!(txn.alloc_page());
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
            match try!(insert(rng, txn, cow_right, key, value, right_page, false)) {
                Res::Ok { page,.. } => cow_right = Cow::from_mut_page(page),
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
            match try!(insert(rng,txn,cow_left,key, value, right_page, false)) {
                Res::Ok { page, .. } => (page, cow_right.unwrap_mut()),
                _ => unreachable!()
            }
        },
        Ordering::Equal =>
            match (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:value_}) {
                Ordering::Less | Ordering::Equal =>
                    match try!(insert(rng,txn,cow_left,key, value, right_page, false)) {
                        Res::Ok { page, .. } => (page, cow_right.unwrap_mut()),
                        _ => unreachable!()
                    },
                Ordering::Greater =>
                    match try!(insert(rng, txn, cow_right, key, value, right_page, false)) {
                        Res::Ok { page, .. } => (cow_left.unwrap_mut(), page),
                        _ => unreachable!()
                    },
            },
        Ordering::Greater =>
            match try!(insert(rng, txn, cow_right, key, value, right_page, false)) {
                Res::Ok { page, .. } => (cow_left.unwrap_mut(), page),
                _ => unreachable!()
            }
    };
    debug!("/SPLIT");
    Ok(Res::Split {
        key_ptr: key_ptr,
        key_len: key_len,
        value: value_,
        left: left,
        right: right,
        free_page: page.page_offset()
    })
}

// This function deals with the case where the main page split, either during insert, or during delete.
fn root_split<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, x:Res) -> Result<Db,Error> {
    debug!("ROOT SPLIT");
    if let Res::Split { left,right,key_ptr,key_len,value,free_page } = x {
        let mut page = try!(txn.alloc_page());
        page.init();
        unsafe {
            *((page.offset(FIRST_HEAD as isize) as *mut u64).offset(2)) = left.page_offset().to_le();
            let key = std::slice::from_raw_parts(key_ptr,key_len);
            let right_offset = right.page_offset();
            let ins = try!(insert(rng, txn, Cow::from_mut_page(page), key, value, right_offset, false));
            debug!("free root_split: {:?}", free_page);
            free(rng, txn, free_page);
            match ins {
                Res::Ok { page,.. } => {
                    Ok(Db { root:page.page_offset() })
                },
                _ => unreachable!() // We just inserted a small enough value into a freshly allocated page, no split can possibly happen.
            }
        }
    } else {
        unreachable!()
    }
}


pub fn put<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, db: &mut Db, key: &[u8], value: &[u8])->Result<(),Error> {
    assert!(key.len() < MAX_KEY_SIZE);
    unsafe {
        let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };
        let value = if value.len() > VALUE_SIZE_THRESHOLD {
            try!(alloc_value(txn,value))
        } else {
            UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 }
        };
        debug!("value = {:?}", Value { txn:txn,value:value });
        match try!(insert(rng, txn, root_page, key, value, 0, false)) {
            Res::Ok { page,.. } => db.root = page.page_offset(),
            x => {
                db.root = try!(root_split(rng,txn,x)).root
            }
        }
        Ok(())
    }
}


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
    fn compare<T>(&self, txn:&mut MutTxn<T>, key_:&[u8], value_:UnsafeValue) -> Ordering {
        match *self {
            C::KV { key, value } => {
                match key.cmp(key_) {
                    Ordering::Equal => (Value{txn:txn,value:value}).cmp(Value{txn:txn,value:value_}),
                    x => x
                }
            },
            C::K { key } => key.cmp(key_),
            C::Smallest => Ordering::Less
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


unsafe fn delete<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, page:Cow, comp:C, mut needs_copying:bool) -> Result<Option<(Res,Option<Smallest>)>,Error> {
    debug!("delete, page: {:?}", page);
    let mut levels:[u16;MAX_LEVEL+1] = [FIRST_HEAD;MAX_LEVEL+1];
    let mut level = MAX_LEVEL;
    let mut current_off = FIRST_HEAD;
    let mut current = page.offset(current_off as isize) as *mut u16;
    let mut first_matching_offset = 0; // The smallest known offset to an entry matching comp.
    let mut next_page = 0; // Next page to explore, will be set during the search.

    needs_copying = needs_copying || get_rc(txn,page.page_offset()) >= 2;

    // In all cases, make the page mutable, possibly copying it if it
    // is referenced by several other pages. Possible optimization:
    // wait until we really find a key.
    let page = if needs_copying {
        let (page, _) = try!(cow_pinpointing(rng, txn, page.as_nonmut(), 0));
        Cow::from_mut_page(page)
    } else {
        let (page,_) = try!(cow_pinpointing(rng,txn, page, 0));
        Cow::from_mut_page(page)
    };

    // The following loop goes down the levels.
    loop {

        // The following loop advances in the list until there's a match, or the list becomes empty.
        // Loop invariant: comp is strictly larger than the (key, value) at "current".
        loop {
            let next = u16::from_le(*(current.offset(level as isize))); // next in the list at the current level.
            if next == NIL {
                // We're already at the end of the list, we need to go down one level, or stop.
                levels[level] = current_off;
                break
            } else {
                // Else, compare with the next element.
                let next_ptr = page.offset(next as isize);
                let (next_key,next_value) = read_key_value(next_ptr);
                match comp.compare(txn,next_key,next_value) {
                    Ordering::Less => break,
                    Ordering::Equal => {
                        if first_matching_offset == 0 {
                            first_matching_offset = next;
                            current = page.offset(current_off as isize) as *mut u16;
                        }
                        levels[level] = current_off;
                        break
                    },
                    Ordering::Greater => {
                        current_off = next;
                        current = page.offset(current_off as isize) as *mut u16;
                    }
                }
            }
        }
        if level == 0 {
            // If we're at the end of the search, set the next page.
            next_page = u64::from_le(*((current as *const u64).offset(2)));
            break
        } else {
            level -= 1
        }
    }

    // Here, "comp" is smaller than or equal to the (key,value) at
    // offset "next", and strictly larger than the (key,value) at
    // offset "current".
    
    // First delete in the page below.
    let del = if next_page > 0 {
        let next_page = txn.load_cow_page(next_page);
        try!(delete(rng, txn, next_page, comp, needs_copying))
    } else {
        None
    };
    // Then delete in the current page, depending on the results.
    match del {
        None if first_matching_offset>0 => {
            // We deleted nothing in the next page, and there's a
            // match between comp and the (key,value) at offset next.
            // Note that the match can be either a key-value match, a
            // key match, or a smallest element match.

            let mut page_becomes_empty = false;

            // Delete the entries of the matched key in all lists. We
            // simply update all non-NIL pointers to tails of a list
            // into a pointer to the tail of the tail.
            for level in 0..(MAX_LEVEL+1) {
                let &current_off = levels.get_unchecked(level);
                debug!("level = {:?}, current_off = {:?}", level, current_off);
                let current = page.offset(current_off as isize) as *mut u16;
                let next_off = u16::from_le(*(current.offset(level as isize)));
                if next_off == first_matching_offset {
                    // If the entry to be deleted is in the list at this level, delete it.
                    let next = page.offset(next_off as isize) as *mut u16;
                    let next_next_off = *(next.offset(level as isize));
                    if level == 0 {
                        // At the first level, if we're deleting a
                        // value stored in a large value page, and we
                        // do not return that value, we need to
                        // decrement its reference counter.
                        let (key,value) = read_key_value(next as *const u8);
                        if let UnsafeValue::O { offset, .. } = value {
                            if !comp.is_smallest() {
                                free_value(rng,txn,offset)
                            }
                        }
                        // Mark the freed space on the page.
                        let size = record_size(key.len(),value.len() as usize);
                        *(page.p_occupied()) = (page.occupied() - size).to_le();
                        if next_next_off == NIL && current_off == FIRST_HEAD {
                            page_becomes_empty = true;
                        }
                    }
                    // Delete the entry at this level.
                    *current.offset(level as isize) = next_next_off;
                }
            }
            // Now, the entry is not in the list anymore.  If there's
            // a page below, replace the (key,value) at the position
            // of "next" in the list (but not necessarily at the same
            // offset), with the smallest element in that page.
            let matching_ptr = page.offset(first_matching_offset as isize);
            let next_page = u64::from_le(*((matching_ptr as *const u64).offset(2)));
            if next_page > 0 {
                // Delete the smallest element in the page below.
                let next_page = txn.load_cow_page(next_page);
                match try!(delete(rng,txn, next_page, C::Smallest, needs_copying)) {
                    Some((Res::Ok { page:next_page, position }, Some(smallest))) => {
                        if position == 1 {
                            // Removing the smallest element emptied next_page.

                            //let (key,value) = read_key_value(current as *const u8);
                            //delete(rng,txn,page,C::KV { key:key, value:value })
                            unimplemented!()
                        } else {
                            // We succeeded in removing the smallest
                            // element of next_page. This operation
                            // returned a "new" next_page, which we
                            // reinsert in the current page, following
                            // the smallest element of the former
                            // next_page.
                            let key = std::slice::from_raw_parts(smallest.key_ptr,smallest.key_len);
                            Ok(Some((try!(insert(rng,txn,page,key, smallest.value, next_page.page_offset(), false)), None)))
                        }
                    },
                    _ => {
                        // This case can actually happen, for instance
                        // if some rebalancing operation causes a
                        // split.
                        unimplemented!()
                    }
                }
            } else {
                if comp.is_smallest() {
                    // If we're currently looking for the smallest element, return it.
                    let next_ptr = page.offset(first_matching_offset as isize);
                    let (next_key,next_value) = read_key_value(next_ptr);
                    let page_offset = page.page_offset();
                    Ok(
                        Some((Res::Ok { page:page.unwrap_mut(), position: if page_becomes_empty { 1 } else { 0 } },
                              Some(Smallest {
                                  key_ptr: next_key.as_ptr(),
                                  key_len: next_key.len(),
                                  value: next_value,

                                  // The above levels need to free
                                  // this page if it is no longer
                                  // referenced. We cannot do this
                                  // here, because this page contains
                                  // next_key and next_value, which
                                  // have not yet been copied anywhere
                                  // else. "needs_copying" tells
                                  // whether this page is referenced
                                  // in strictly more than one tree.
                                  free_page: if needs_copying { 0 } else { page_offset },

                                  // If this is the smallest element,
                                  // we're necessarily at a leaf,
                                  // hence there's no page to the
                                  // right of this element.
                                  reinsert_page:0
                              })))
                    )
                } else {
                    // Else, simply return the new version of the page.
                    Ok(
                        Some((Res::Ok { page:page.unwrap_mut(), position: if page_becomes_empty { 1 } else { 0 } }, None))
                    )
                }
            }
        },
        None =>
            // If we didn't delete anything in the page below, and
            // there is no match in this page, we have nothing to
            // delete.
            Ok(None),
        Some((Res::Ok { page:next_page, position }, _)) => {
            // If we deleted something in a page below, we need to
            // update the pointer to the "new" next_page, and rebalance.

            if position == 1 {
                // next_page becomes empty. Delete current entry, and reinsert.
                
                // remove reference before freeing (so that the page below is not freed yet).
                *((current as *mut u64).offset(2)) = 0;
                free(rng, txn, next_page.page_offset());  // then free.

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
                    // If the current page becomes empty by removing
                    // the current key, we need to reinsert the
                    // current key anyway. In this case, we insert the
                    // current key and value in the left child of the
                    // current page, and return that left child.

                    // Doesn't it make the tree unbalanced?

                    let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    debug_assert!(next_page > 0);
                    let next_page = txn.load_cow_page(next_page);
                    let ins = try!(insert(rng, txn, next_page, key, value, 0, unimplemented!()));
                    free(rng, txn, page.page_offset());
                    Ok(Some((ins,None)))
                } else {
                    Ok(Some((Res::Ok {
                        page:page.unwrap_mut(),
                        position: 0
                    }, None)))
                }
            } else {
                *((current as *mut u64).offset(2)) = next_page.page_offset().to_le();
                Ok(Some((Res::Ok { page:page.unwrap_mut(), position:0 }, None )))
            }
        },
        Some((Res::Split { key_ptr,key_len,value,left,right,free_page }, _)) => {
            // Deleting something in the previous page caused it to
            // split (probably by some rebalancing operation). We
            // update the pointer to the next page into one to the
            // left list, and insert the middle element of the split
            // into this list, followed by the right page.
            *((current as *mut u64).offset(2)) = left.page_offset().to_le();
            let key = std::slice::from_raw_parts(key_ptr,key_len);
            let result = Some((try!(insert(rng,txn,page,key, value, right.page_offset(), false)), None));
            // After reinserting, we can free the page containing the middle element.
            free(rng, txn, free_page);
            Ok(result)
        }
    }
}


pub fn del<R:Rng,T>(rng:&mut R, txn:&mut MutTxn<T>, db:&mut Db, key:&[u8], value:Option<&[u8]>)->Result<(),Error> {
    assert!(key.len() < MAX_KEY_SIZE);
    let root_page = Cow { cow: txn.txn.load_cow_page(db.root) };

    let comp = if let Some(value) = value {
        C::KV { key: key,
                value: UnsafeValue::S { p:value.as_ptr(), len:value.len() as u32 } }
    } else {
        C::K { key:key }
    };
    unsafe {
        match try!(delete(rng,txn, root_page, comp, false)) {
            Some((Res::Ok { page, .. },Some(reinsert))) => {
                let key = std::slice::from_raw_parts(reinsert.key_ptr,reinsert.key_len);
                assert!(key.len() < MAX_KEY_SIZE);
                match try!(insert(rng, txn, Cow::from_mut_page(page), key, reinsert.value, reinsert.reinsert_page, false)) {
                    Res::Ok { page,.. } => {
                        free(rng, txn, reinsert.free_page);
                        db.root = page.page_offset()
                    },
                    x => {
                        let x = try!(root_split(rng,txn,x));
                        debug!("free del: {:?}", reinsert.free_page);
                        free(rng, txn, reinsert.free_page);
                        db.root = x.root
                    }
                }
                Ok(())
            },
            Some((Res::Ok { page, position },None)) => {
                if position == 1 {
                    let next_page = u64::from_le(*((page.offset(FIRST_HEAD as isize) as *const u64).offset(2)));
                    debug!("free del none: {:?}", page.page_offset());
                    free(rng, txn, page.page_offset());
                    db.root = next_page
                } else {
                    db.root = page.page_offset()
                }
                Ok(())
            },
            Some((x,_)) => {
                db.root = try!(root_split(rng,txn,x)).root;
                Ok(())
            },
            None => Ok(())
        }
    }
}

pub fn replace<R:Rng,T>(rng:&mut R, txn: &mut MutTxn<T>, db: &mut Db, key: &[u8], value: &[u8])->Result<(),Error> {
    try!(del(rng,txn,db,key,None));
    put(rng,txn,db,key,value)
}

