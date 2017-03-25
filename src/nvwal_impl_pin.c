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

#include "nvwal_impl_pin.h"

#include <assert.h>
#include <sched.h>

#include "nvwal_atomics.h"

uint8_t nvwal_pin_read_try_lock(int32_t* pin_count) {
  int32_t cur_value = *pin_count;
  if (cur_value < 0) {
    assert(cur_value == kNvwalPinExclusive);
    return 0;
  }
  const int32_t new_value = cur_value + 1;
  return nvwal_atomic_compare_exchange_strong(
    pin_count,
    &cur_value,
    new_value);
}

void nvwal_pin_read_unconditional_lock(int32_t* pin_count) {
  while (!nvwal_pin_read_try_lock(pin_count)) {
    sched_yield();
  }
}

void nvwal_pin_read_unlock(int32_t* pin_count) {
  assert(*pin_count > 0);
  nvwal_atomic_fetch_sub(pin_count, 1);
}

uint8_t nvwal_pin_flusher_try_lock(int32_t* pin_count) {
  int32_t expected = 0;
  return nvwal_atomic_compare_exchange_strong(
    pin_count,
    &expected,
    kNvwalPinExclusive);
}

void nvwal_pin_flusher_unconditional_lock(int32_t* pin_count) {
  while (!nvwal_pin_flusher_try_lock(pin_count)) {
    sched_yield();
  }
}

void nvwal_pin_flusher_unlock(int32_t* pin_count) {
  assert(*pin_count == kNvwalPinExclusive);
  nvwal_atomic_store(pin_count, 0);
}
