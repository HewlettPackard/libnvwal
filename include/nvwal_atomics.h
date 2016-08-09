/*
 * Copyright (c) 2014-2016, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef NVWAL_ATOMICS_H_
#define NVWAL_ATOMICS_H_

/**
 * @file nvwal_atomics.h
 * This file wraps atomic functions defined in C11 in case the client program
 * does not allow using C11 features.
 * nvwal_xxx wraps xxx function/macro in C11.
 * Do NOT directly use xxx in our code. Always use nvwal_xxx enums/macros/functions.
 *
 * So far, we don't have the non-C11 implementation yet.
 * We simply forward to stdatomic.h.
 * @see http://en.cppreference.com/w/c/atomic
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#ifdef __STDC_NO_ATOMICS__  // we should also check STDC version. later, later..

#error "nvwal_atomics.h without C11 is not yet implemented..."
// We will add such an implementation when we really need.
// Will use GCC's traditional sync_xxx functions?

#else  // __STDC_NO_ATOMICS__

#include <stdatomic.h>

typedef enum {
  nvwal_memory_order_relaxed = memory_order_relaxed,
  nvwal_memory_order_consume = memory_order_consume,
  nvwal_memory_order_acquire = memory_order_acquire,
  nvwal_memory_order_release = memory_order_release,
  nvwal_memory_order_acq_rel = memory_order_acq_rel,
  nvwal_memory_order_seq_cst = memory_order_seq_cst,
} nvwal_memory_order;

#define nvwal_atomic_init(PTR, VAL) atomic_init(PTR, VAL)

#define nvwal_atomic_store(PTR, VAL) atomic_store(PTR, VAL)
#define nvwal_atomic_store_explicit(PTR, VAL, ORD)\
  atomic_store_explicit(PTR, VAL, ORD)

#define nvwal_atomic_load(PTR) atomic_load(PTR)
#define nvwal_atomic_load_explicit(PTR, ORD)\
  atomic_load_explicit(PTR, ORD)

#define nvwal_atomic_exchange(PTR, VAL) atomic_exchange(PTR, VAL)
#define nvwal_atomic_exchange_explicit(PTR, VAL, ORD)\
  atomic_exchange_explicit(PTR, VAL, ORD)

#define nvwal_atomic_compare_exchange_weak(PTR, VAL)\
  atomic_compare_exchange_weak(PTR, VAL)
#define nvwal_atomic_compare_exchange_weak_explicit(PTR, VAL, ORD)\
  atomic_compare_exchange_weak_explicit(PTR, VAL, ORD)
#define nvwal_atomic_compare_exchange_strong(PTR, VAL)\
  atomic_compare_exchange_strong(PTR, VAL)
#define nvwal_atomic_compare_exchange_strong_explicit(PTR, VAL, ORD)\
  atomic_compare_exchange_strong_explicit(PTR, VAL, ORD)

#define nvwal_atomic_fetch_add(PTR, VAL) atomic_fetch_add(PTR, VAL)
#define nvwal_atomic_fetch_add_explicit(PTR, VAL, ORD)\
  atomic_fetch_add_explicit(PTR, VAL, ORD)

#define nvwal_atomic_fetch_sub(PTR, VAL) atomic_fetch_sub(PTR, VAL)
#define nvwal_atomic_fetch_sub_explicit(PTR, VAL, ORD)\
  atomic_fetch_sub_explicit(PTR, VAL, ORD)

#define nvwal_atomic_fetch_or(PTR, VAL) atomic_fetch_or(PTR, VAL)
#define nvwal_atomic_fetch_or_explicit(PTR, VAL, ORD)\
  atomic_fetch_or_explicit(PTR, VAL, ORD)

#define nvwal_atomic_fetch_xor(PTR, VAL) atomic_fetch_xor(PTR, VAL)
#define nvwal_atomic_fetch_xor_explicit(PTR, VAL, ORD)\
  atomic_fetch_xor_explicit(PTR, VAL, ORD)

#define nvwal_atomic_fetch_and(PTR, VAL) atomic_fetch_and(PTR, VAL)
#define nvwal_atomic_fetch_and_explicit(PTR, VAL, ORD)\
  atomic_fetch_and_explicit(PTR, VAL, ORD)

#define nvwal_atomic_thread_fence(ORD) atomic_thread_fence(ORD)

#endif  // __STDC_NO_ATOMICS__

/** And a few, implementation-agnostic shorthand. */

#define nvwal_atomic_load_acquire(PTR)\
  atomic_load_explicit(PTR, nvwal_memory_order_acquire)

#define nvwal_atomic_load_consume(PTR)\
  atomic_load_explicit(PTR, nvwal_memory_order_consume)

#define nvwal_atomic_store_release(PTR, VAL)\
  atomic_store_explicit(PTR, VAL, nvwal_memory_order_release)

/** @} */

#endif  // NVWAL_ATOMICS_H_
