/* 
 * Copyright 2017 Hewlett Packard Enterprise Development LP
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met:
 * 
 *   1. Redistributions of source code must retain the above copyright 
 *      notice, this list of conditions and the following disclaimer.
 *
 *   2. Redistributions in binary form must reproduce the above copyright 
 *      notice, this list of conditions and the following disclaimer 
 *      in the documentation and/or other materials provided with the 
 *      distribution.
 *   
 *   3. Neither the name of the copyright holder nor the names of its 
 *      contributors may be used to endorse or promote products derived 
 *      from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
 * In order to support RHEL 7.x, we alter stdatomic.h with our own implementation.
 * This header should work for the following compilers:
 * \li gcc 4.9 and later should work trivially with stdatomic.h.
 * \li Reasonably new clang should work trivially with stdatomic.h.
 * \li gcc 4.8 is supported via our own version of atomic wrappers.
 *
 * We don't wrap macros for old clang as rigorously as for old gcc.
 * We think it's fine as few poeple \e have \e to use old clang
 * as opposed to old gcc that people can't replace tend to abound.
 * gcc 4.7 and older are \b not supported. Sorry.
 *
 * @see http://en.cppreference.com/w/c/atomic
 * @see https://gist.github.com/nhatminhle/5181506
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

/*
 * If __STDC_NO_ATOMICS__ is defined, we surely don't have stdatomic.h.
 * However, an old version of gcc (4.8) does not follow this rule.
 * If it's 4.8, we go into this route, too.
 */
#if defined(__STDC_NO_ATOMICS__)  || (defined(__GNUC__) && (__GNUC__  == 4 && __GNUC_MINOR__ == 8))

/*
 * stdatomic.h doesn't exist! We need to implement it by ourselves.
 * We assume this is gcc 4.8 or later, so at least the 'internal' version
 * of the macros (e.g., __ATOMIC_RELAXED) exist with a bit different name.
 * To support 4.7 and older, we have to really emulate all
 * logics with older gcc maro.. not worth it.
 * @see https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html
 */

typedef enum {
  nvwal_memory_order_relaxed = __ATOMIC_RELAXED,
  nvwal_memory_order_consume = __ATOMIC_CONSUME,
  nvwal_memory_order_acquire = __ATOMIC_ACQUIRE,
  nvwal_memory_order_release = __ATOMIC_RELEASE,
  nvwal_memory_order_acq_rel = __ATOMIC_ACQ_REL,
  nvwal_memory_order_seq_cst = __ATOMIC_SEQ_CST,
} nvwal_memory_order;


#define nvwal_atomic_init(PTR, VAL) atomic_init(PTR, VAL)

#define nvwal_atomic_store(PTR, VAL) __atomic_store_n(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_store_explicit(PTR, VAL, ORD)\
  __atomic_store_n(PTR, VAL, ORD)

#define nvwal_atomic_load(PTR) __atomic_load_n(PTR, __ATOMIC_SEQ_CST)
#define nvwal_atomic_load_explicit(PTR, ORD)\
  __atomic_load_n(PTR, ORD)

#define nvwal_atomic_exchange(PTR, VAL) __atomic_exchange(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_exchange_explicit(PTR, VAL, ORD)\
  __atomic_exchange(PTR, VAL, ORD)

/* bool __atomic_compare_exchange_n (type *ptr, type *expected, type desired, bool weak, int success_memorder, int failure_memorder) */
#define nvwal_atomic_compare_exchange_weak(PTR, VAL, DES)\
  __atomic_compare_exchange_n(PTR, VAL, DES, 1, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
#define nvwal_atomic_compare_exchange_weak_explicit(PTR, VAL, DES, SUCC_ORD, FAIL_ORD)\
  __atomic_compare_exchange_n(PTR, VAL, DES, 1, SUCC_ORD, FAIL_ORD)
#define nvwal_atomic_compare_exchange_strong(PTR, VAL, DES)\
  __atomic_compare_exchange_n(PTR, VAL, DES, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
#define nvwal_atomic_compare_exchange_strong_explicit(PTR, VAL, DES, SUCC_ORD, FAIL_ORD)\
  __atomic_compare_exchange_n(PTR, VAL, DES, ORD, 0, SUCC_ORD, FAIL_ORD)

#define nvwal_atomic_fetch_add(PTR, VAL) __atomic_fetch_add(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_fetch_add_explicit(PTR, VAL, ORD)\
  __atomic_fetch_add(PTR, VAL, ORD)

#define nvwal_atomic_fetch_sub(PTR, VAL) __atomic_fetch_sub(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_fetch_sub_explicit(PTR, VAL, ORD)\
  __atomic_fetch_sub(PTR, VAL, ORD)

#define nvwal_atomic_fetch_or(PTR, VAL) __atomic_fetch_or(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_fetch_or_explicit(PTR, VAL, ORD)\
  __atomic_fetch_or(PTR, VAL, ORD)

#define nvwal_atomic_fetch_xor(PTR, VAL) atomic_fetch_xor(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_fetch_xor_explicit(PTR, VAL, ORD)\
  __atomic_fetch_xor(PTR, VAL, ORD)

#define nvwal_atomic_fetch_and(PTR, VAL) atomic_fetch_and(PTR, VAL, __ATOMIC_SEQ_CST)
#define nvwal_atomic_fetch_and_explicit(PTR, VAL, ORD)\
  __atomic_fetch_and(PTR, VAL, ORD)

#define nvwal_atomic_thread_fence(ORD) __atomic_thread_fence(ORD)

#else  /* __STDC_NO_ATOMICS__ */

/* We have stdatomic.h! This case is trivial */
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

#define nvwal_atomic_compare_exchange_weak(PTR, VAL, DES)\
  atomic_compare_exchange_weak(PTR, VAL, DES)
#define nvwal_atomic_compare_exchange_weak_explicit(PTR, VAL, DES, SUCC_ORD, FAIL_ORD)\
  atomic_compare_exchange_weak_explicit(PTR, VAL, DES, SUCC_ORD, FAIL_ORD)
#define nvwal_atomic_compare_exchange_strong(PTR, VAL, DES)\
  atomic_compare_exchange_strong(PTR, VAL, DES)
#define nvwal_atomic_compare_exchange_strong_explicit(PTR, VAL, DES, SUCC_ORD, FAIL_ORD)\
  atomic_compare_exchange_strong_explicit(PTR, VAL, DES, SUCC_ORD, FAIL_ORD)

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

#endif  /* __STDC_NO_ATOMICS__ */

/** And a few, implementation-agnostic shorthand. */

#define nvwal_atomic_load_acquire(PTR)\
  nvwal_atomic_load_explicit(PTR, nvwal_memory_order_acquire)

#define nvwal_atomic_load_consume(PTR)\
  nvwal_atomic_load_explicit(PTR, nvwal_memory_order_consume)

#define nvwal_atomic_store_release(PTR, VAL)\
  nvwal_atomic_store_explicit(PTR, VAL, nvwal_memory_order_release)

/** @} */

#endif  /* NVWAL_ATOMICS_H_ */
