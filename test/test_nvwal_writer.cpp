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


#include "nvwal_api.h"

#include "nvwal_test_common.hpp"

/**
 * @file test_nvwal_writer.cpp
 * Test the writer piece separately.
 */

namespace nvwaltest {


TEST(NvwalWriterTest, OneEpoch) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = resource->wal_instance_.writers_ + 0;
  const uint32_t kBytes = 64;

  std::memset(buffer, 42, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  std::memset(buffer + kBytes, 24, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalWriterTest, TwoEpochs) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = resource->wal_instance_.writers_ + 0;
  const uint32_t kBytes = 64;

  std::memset(buffer, 42, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  std::memset(buffer + kBytes, 24, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 2));

  EXPECT_EQ(0, context.uninit_all());
}

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalWriterTest);
