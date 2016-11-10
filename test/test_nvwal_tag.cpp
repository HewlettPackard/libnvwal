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

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#include "nvwal_api.h"

#include "nvwal_test_common.hpp"

#include "nvwal_mds.h"
#include "nvwal_test_mds_common.hpp"
#include "nvwal_impl_mds.h"


/**
 * @file test_nvwal_writer.cpp
 * Test the epoch tagging piece separately.
 */

namespace nvwaltest {

void tag_and_persist_epoch(
  TestContext& context,
  nvwal_epoch_t epoch_id, uint64_t epoch_size, uint64_t metadata)
{
  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  auto* buffer = resource->writer_buffers_[0].get();
  const uint32_t buffer_size = wal->config_.writer_buffer_size_;
  auto* writer = wal->writers_ + 0;

  const uint32_t offset = ((epoch_id-1) * epoch_size) % buffer_size;
  std::memset(buffer+offset, 0xFF & metadata, epoch_size);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, epoch_size, epoch_id));
  EXPECT_EQ(0, nvwal_tag_epoch(writer, epoch_id, metadata));
  EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, epoch_id));
  EXPECT_EQ(0, context.wait_until_durable(wal, epoch_id));
} 

nvwal_error_t find_epoch(
  TestContext& context, uint64_t metadata, struct MdsEpochMetadata* out)
{
  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  
  return mds_find_metadata_lower_bound(wal, metadata, out);
}

TEST(NvwalTagTest, OneEpoch) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  const uint32_t kBytes = 64;

  tag_and_persist_epoch(context, 1, kBytes, 24);
 
  MdsEpochMetadata em;

  EXPECT_NE(0, find_epoch(context, 42, &em));
  EXPECT_EQ(0, find_epoch(context, 23, &em));

  EXPECT_EQ(1,em.epoch_id_);
  EXPECT_EQ(24, em.user_metadata_);

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalTagTest, MultipleEpochs) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  const uint32_t kBytes = 1024;

  for (nvwal_epoch_t e = 1; e < 8*16; e++) {
    uint64_t metadata = e*10;
    tag_and_persist_epoch(context, e, kBytes, metadata);
  }

  MdsEpochMetadata em;

  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;

  for (nvwal_epoch_t e = 1; e < 8*16; e++) {
    uint64_t query_metadata = e*10;
    EXPECT_EQ(0, find_epoch(context, query_metadata, &em));
    EXPECT_EQ(e, em.epoch_id_);
    EXPECT_EQ(query_metadata, em.user_metadata_);
    EXPECT_EQ(((e-1)*kBytes) / wal->config_.segment_size_ + 1, em.from_seg_id_);
    EXPECT_EQ(((e-1)*kBytes) % wal->config_.segment_size_, em.from_offset_);
  }

  EXPECT_EQ(0, context.uninit_all());
}


}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalWriterTest);
