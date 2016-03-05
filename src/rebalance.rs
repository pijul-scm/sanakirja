use super::txn::*;

/// Converts v(u(a,b),c) into u(a,v(b,c))
pub fn tree_rotate_clockwise(page: &mut MutPage, v: u16) -> u16 {
    debug!("rotate clockwise");
    unsafe {
        let ptr = page.offset(v as isize) as *mut u32;

        let u_is_local = u32::to_le(*ptr) == 1;
        if u_is_local {
            let off_u = *(ptr.offset(1));
            let ptr_u = page.offset(off_u as isize) as *mut u32;

            // fetch node size
            let v_size = u16::from_le(*(ptr as *const u16).offset(11));
            let u_size = u16::from_le(*((ptr_u as *const u16).offset(11)));

            let b_size = {
                if u32::from_le(*(ptr_u.offset(2))) == 1 {
                    let off_b = u32::from_le(*(ptr_u.offset(3)));
                    if off_b != 0 {
                        let ptr_b = page.offset(off_b as isize);
                        u16::from_le(*((ptr_b as *const u16).offset(11)))
                    } else {
                        0
                    }
                } else {
                    // let off=u64::from_le(*((ptr_u as *const u64).offset(1)));
                    0//if off!=0 { 1 } else { 0 }
                }
            };

            // Change the left of v to b
            *(ptr as *mut u64) = *((ptr_u as *const u64).offset(1));

            // Change the right of u to v
            *(ptr_u.offset(2)) = (1 as u32).to_le();
            *(ptr_u.offset(3)) = (v as u32).to_le();
            // debug!("overflow? {} {} {}",v_size,b_size,u_size);
            *(ptr as *mut u16).offset(11) = ((v_size + b_size) - u_size).to_le();
            *(ptr_u as *mut u16).offset(11) = v_size.to_le();
            //
            off_u as u16
        } else {
            // Cannot rotate
            v
        }
    }
}

/// Converts u(a,v(b,c)) into v(u(a,b),c)
pub fn tree_rotate_anticlockwise(page: &mut MutPage, u: u16) -> u16 {
    debug!("rotate anticlockwise");
    unsafe {
        let ptr = page.offset(u as isize) as *mut u32;

        let v_is_local = u32::from_le(*(ptr.offset(2))) == 1;
        if v_is_local {
            let off_v = u32::from_le(*(ptr.offset(3)));
            let ptr_v = page.offset(off_v as isize) as *mut u32;

            // fetch node size
            let u_size = u16::from_le(*(ptr as *const u16).offset(11));
            let v_size = u16::from_le(*((ptr_v as *const u16).offset(11)));
            let b_size = {
                if u32::from_le(*ptr_v) == 1 {
                    let off_b = u32::from_le(*(ptr_v.offset(1)));
                    if off_b != 0 {
                        let ptr_b = page.offset(off_b as isize) as *const u16;
                        u16::from_le(*(ptr_b.offset(11)))
                    } else {
                        0
                    }
                } else {
                    // if this is really a child
                    // let off=u64::from_le(*(ptr_v as *const u64));
                    0//if off!=0 { 1 } else { 0 }
                }
            };

            // Change the right of u to b
            *((ptr as *mut u64).offset(1)) = *(ptr_v as *const u64);
            // Change the left of v to u
            *ptr_v = (1 as u32).to_le();
            *(ptr_v.offset(1)) = (u as u32).to_le();
            *(ptr as *mut u16).offset(11) = ((u_size + b_size) - v_size).to_le();
            *(ptr_v as *mut u16).offset(11) = u_size.to_le();
            //
            off_v as u16
        } else {
            // Cannot rotate
            u
        }
    }
}

/// Rebalances a binary tree.
pub fn rebalance(page: &mut MutPage, node: u16) -> u16 {
    debug!("rebalance");
    let x = unsafe {
        let ptr = page.offset(node as isize) as *mut u32;
        let left_local = u32::from_le(*ptr);
        let right_local = u32::from_le(*(ptr.offset(2)));
        let left_cardinal = {
            if left_local == 1 {
                let left = u32::from_le(*(ptr.offset(1)));
                let left_ptr = page.offset(left as isize) as *const u16;
                u16::from_le(*(left_ptr.offset(11)))
            } else {
                0//1
            }
        };
        let right_cardinal = {
            if right_local == 1 {
                let right = u32::from_le(*(ptr.offset(3)));
                let right_ptr = page.offset(right as isize) as *const u16;
                u16::from_le(*(right_ptr.offset(11)))
            } else {
                0//1
            }
        };
        if left_cardinal + 2 < right_cardinal {
            tree_rotate_anticlockwise(page, node)
        } else if right_cardinal + 2 < left_cardinal {
            tree_rotate_clockwise(page, node)
        } else {
            node
        }
    };
    debug!("/rebalance");
    x
}
