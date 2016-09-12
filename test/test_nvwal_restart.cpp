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

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "nvwal_api.h"
#include "nvwal_test_common.hpp"

/**
 * @file test_nvwal_restart.cpp
 * Normal restart test.
 * No tricky case. All clean shutdowns.
 */

namespace nvwaltest {
TEST(NvwalRestartTest, NoLog) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  nvwal_epoch_t durable_epoch;
  EXPECT_EQ(0, nvwal_query_durable_epoch(wal, &durable_epoch));
  EXPECT_EQ(kNvwalInvalidEpoch, durable_epoch);

  EXPECT_EQ(0, context.restart_clean());

  resource = context.get_resource(0);
  wal = &resource->wal_instance_;
  EXPECT_EQ(0, nvwal_query_durable_epoch(wal, &durable_epoch));
  EXPECT_EQ(kNvwalInvalidEpoch, durable_epoch);

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalRestartTest, OneWriterOneEpoch) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = resource->wal_instance_.writers_ + 0;
  const uint32_t kBytes = 64;

  std::memset(buffer, 42, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, 1U));
  EXPECT_EQ(0, TestContext::wait_until_durable(wal, 1U));

  nvwal_epoch_t durable_epoch;
  EXPECT_EQ(0, nvwal_query_durable_epoch(wal, &durable_epoch));
  EXPECT_EQ(1U, durable_epoch);

  EXPECT_EQ(0, context.restart_clean());

  resource = context.get_resource(0);
  wal = &resource->wal_instance_;
  EXPECT_EQ(0, nvwal_query_durable_epoch(wal, &durable_epoch));
  EXPECT_EQ(1U, durable_epoch);

  struct NvwalLogCursor cursor;
  EXPECT_EQ(0, nvwal_open_log_cursor(wal, 1U, 1U, &cursor));
  while (nvwal_cursor_is_valid(wal, &cursor)) {
    EXPECT_EQ(1U, nvwal_cursor_get_current_epoch(wal, &cursor));
    const nvwal_byte_t* data = nvwal_cursor_get_data(wal, &cursor);
    const uint64_t len = nvwal_cursor_get_data_length(wal, &cursor);
    EXPECT_EQ(kBytes, len);
    for (uint32_t i = 0; i < len; ++i) {
      EXPECT_EQ(42, data[i]) << i;
    }
    EXPECT_EQ(0, nvwal_cursor_next(wal, &cursor));
  }
  EXPECT_EQ(0, nvwal_close_log_cursor(wal, &cursor));

  EXPECT_EQ(0, context.uninit_all());
}

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalRestartTest);
