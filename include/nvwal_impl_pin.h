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

#ifndef NVWAL_IMPL_PIN_H_
#define NVWAL_IMPL_PIN_H_
/**
 * @file nvwal_impl_pin.h
 * Internal functions for atomic pin/unpin.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

enum NvwalPinConstants {
  /**
   * Exclusively locked by a flusher for recycle.
   */
  kNvwalPinExclusive = -1,
};

/**
 * @brief \b Try (immediately return) for a \b reader to atomically
 * take a lock (\e pin) on the given
 * NV-resident segment to disallow the flusher to recycle it.
 * @details
 * The count means the number of pinning done by readers that are
 * currently reading from a NV-segment. While this is not zero, we must not
 * recycle the segment.
 * Value -1 is reserved for "being recycled".
 * When we recycle this segment, we CAS this from 0 to -1.
 * Readers pin it by CAS-ing from a non-negative value to the value +1.
 * Unfortunately not a simple fetch_add, but should be rare to have
 * a contention here.
 * @see nvwal_pin_read_unconditional_lock()
 * @see nvwal_pin_unpin_read()
 * @return Whether we could successfully take a pin. 0 iff we observe
 * -1 (being recycled).
 */
uint8_t nvwal_pin_read_try_lock(int32_t* pin_count);

/**
 * Unconditional version of nvwal_pin_read_try_lock(), meaning
 * it spins until it acquires the lock.
 * We so far simply use this, but we might want to implement
 * a timeout.. revist later.
 */
void nvwal_pin_read_unconditional_lock(int32_t* pin_count);

/**
 * Atomically reduces the count added by
 * nvwal_pin_read_try_lock() or nvwal_pin_read_unconditional_lock().
 * @pre *pin_count > 0. If the protocol is working correctly,
 * nvwal_pin_read_try_lock()/nvwal_pin_read_unconditional_lock() should have made it >0
 * and no flusher could have made it negative.
 * @note This is lock-free and wait-free. Assuming the above, we can just do fetch_sub.
 */
void nvwal_pin_read_unlock(int32_t* pin_count);

/**
 * @brief \b Try (immediately return) for a \b flusher to atomically
 * take a lock (\e pin) on the given
 * NV-resident segment to disallow any readers to read it.
 * @details
 * Basically same as nvwal_pin_read_try_lock() except
 * @return Whether we could successfully take a pin. 0 iff we observe
 * -1 (being recycled).
 */
uint8_t nvwal_pin_flusher_try_lock(int32_t* pin_count);

/**
 * Unconditional version of nvwal_pin_flusher_try_lock.
 * We so far simply use this, but we might want to implement
 * a timeout.. revist later.
 */
void nvwal_pin_flusher_unconditional_lock(int32_t* pin_count);

/**
 * Atomically revert the count to a usable state.
 * @pre *pin_count == -1. Otherwise you haven't taken the lock!
 */
void nvwal_pin_flusher_unlock(int32_t* pin_count);



#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif  /* NVWAL_IMPL_PIN_H_ */
