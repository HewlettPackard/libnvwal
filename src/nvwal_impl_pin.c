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
