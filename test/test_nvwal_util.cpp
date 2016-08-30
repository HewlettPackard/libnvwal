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
#include <gtest/gtest.h>

#include <cstring>

#include "nvwal_test_common.hpp"
#include "nvwal_util.h"

namespace nvwaltest {

TEST(NvwalUtilTest, CircularMemcpy) {
  nvwal_byte_t circular_buffer[512];
  for (int i = 0; i < sizeof(circular_buffer); ++i) {
    circular_buffer[i] = (nvwal_byte_t) i;
  }

  nvwal_byte_t dest[128];
  std::memset(dest, 0, sizeof(dest));
  nvwal_circular_memcpy(
    dest,
    circular_buffer,
    sizeof(circular_buffer),
    0,
    sizeof(dest));

  for (int i = 0; i < sizeof(dest); ++i) {
    EXPECT_EQ((nvwal_byte_t) i, dest[i]);
  }

  std::memset(dest, 0, sizeof(dest));
  nvwal_circular_memcpy(
    dest,
    circular_buffer,
    sizeof(circular_buffer),
    42,
    sizeof(dest));
  for (int i = 0; i < sizeof(dest); ++i) {
    EXPECT_EQ((nvwal_byte_t) (42 + i), dest[i]);
  }
}

TEST(NvwalUtilTest, CircularMemcpyWrap) {
  nvwal_byte_t circular_buffer[512];
  for (int i = 0; i < sizeof(circular_buffer); ++i) {
    circular_buffer[i] = (nvwal_byte_t) i;
  }

  nvwal_byte_t dest[128];
  std::memset(dest, 0, sizeof(dest));
  nvwal_circular_memcpy(
    dest,
    circular_buffer,
    sizeof(circular_buffer),
    400,
    sizeof(dest));

  for (int i = 0; i < sizeof(dest); ++i) {
    if (i < (512 - 400)) {
      EXPECT_EQ((nvwal_byte_t) (i + 400), dest[i]);
    } else {
      EXPECT_EQ((nvwal_byte_t) (i + 400 - 512), dest[i]);
    }
  }
}

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalUtilTest);
