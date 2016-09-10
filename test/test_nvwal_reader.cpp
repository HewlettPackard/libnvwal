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
#include <iostream>
#include <sys/mman.h>
#include "nvwal_test_common.hpp"
#include "../include/nvwal_mds_types.h"
#include "../include/nvwal_impl_reader.h"
#include "../include/nvwal_api.h"
/**
 * @file test_nvwal_reader.cpp
 */

namespace nvwaltest {
TEST(NvwalReadereTest, InitUninit) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());
  // Do something here
  //
  EXPECT_EQ(0, context.uninit_all());
}

void set_epoch_meta(
  struct MdsEpochMetadata* epoch_meta, 
  nvwal_epoch_t epoch,
  nvwal_dsid_t seg_start,
  uint32_t start_offset,
  nvwal_dsid_t seg_end,
  uint32_t end_offset)
{
  epoch_meta->from_seg_id_ = seg_start;
  epoch_meta->to_seg_id_ = seg_end;
  epoch_meta->from_offset_ = start_offset;
  epoch_meta->to_off_ = end_offset;
  epoch_meta->epoch_id_ = epoch;
}

TEST(NvwalReaderTest, FindPrefetchedEpoch) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* wal = context.get_wal(0);

  struct MdsEpochMetadata epoch_meta;
  struct NvwalEpochMapMetadata epoch_map;
  struct NvwalLogCursor cursor;

  EXPECT_EQ(0, nvwal_open_log_cursor(wal, &cursor, 0, 10));

  /* Make a fake epoch_map and various epoch_meta to hit
   * various prefetch cases
   */
  epoch_map.seg_id_start_ = 0;
  epoch_map.seg_id_end_ = 10;
  epoch_map.seg_start_offset_ = 0;
  epoch_map.seg_end_offset_ = wal->config_.segment_size_;
  epoch_map.mmap_start_ = (nvwal_byte_t*)mmap(NULL, (epoch_map.seg_id_end_-epoch_map.seg_id_start_)*wal->config_.segment_size_, 
                               PROT_READ, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);

  /*epoch spans seg 0, off 0 to seg 0, off 100 */
  set_epoch_meta(&epoch_meta, 0, 0, 0, 0, 100);
  EXPECT_EQ(0, get_prefetched_epoch(wal, &cursor, &epoch_map, &epoch_meta));
  EXPECT_EQ(1, cursor.fetch_complete_);

  /*epoch spans seg 11, off 0 to seg 12, off 100 */
  set_epoch_meta(&epoch_meta, 0, 11, 0, 12, 100);
  EXPECT_EQ(1, get_prefetched_epoch(wal, &cursor, &epoch_map, &epoch_meta)); /*this epoch is beyond prefetch distance */
  //EXPECT_EQ(1, cursor.fetch_complete_);

  EXPECT_EQ(0, nvwal_close_log_cursor(wal, &cursor));

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalReaderTest, ReadOneEpochOneSegment) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());
  // Do something here
  EXPECT_EQ(0, context.uninit_all());
}
/*
TEST(NvwalReaderTest, Test2) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());
  // Do something here
  EXPECT_EQ(0, context.uninit_all());
}
*/
}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalReaderTest);
