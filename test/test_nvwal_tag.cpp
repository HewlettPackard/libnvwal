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
  nvwal_epoch_t epoch_id, uint64_t epoch_size, uint64_t start_lsn, uint64_t end_lsn)
{
  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  auto* buffer = resource->writer_buffers_[0].get();
  const uint32_t buffer_size = wal->config_.writer_buffer_size_;
  auto* writer = wal->writers_ + 0;

  const uint32_t offset = ((epoch_id-1) * epoch_size) % buffer_size;
  std::memset(buffer+offset, 0, epoch_size);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, epoch_size, epoch_id));
  EXPECT_EQ(0, nvwal_tag_epoch(writer, epoch_id, start_lsn, end_lsn));
  EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, epoch_id));
  EXPECT_EQ(0, context.wait_until_durable(wal, epoch_id));
} 

int compare_ge(struct NvwalPredicateClosure* predicate, uint64_t arg) 
{
  uint64_t val = (uint64_t) predicate->state_;
  return arg >= val;  
}

int compare_le(struct NvwalPredicateClosure* predicate, uint64_t arg) 
{
  uint64_t val = (uint64_t) predicate->state_;
  return arg <= val;  
}

nvwal_epoch_t query_epoch_lower_bound_ge(TestContext& context, uint64_t val, int metadata_id)
{
  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;

  struct NvwalPredicateClosure predicate = {compare_ge, (void*) val};  

  return nvwal_query_epoch_lower_bound(wal, metadata_id, &predicate);
}

nvwal_epoch_t query_epoch_range_begin(TestContext& context, uint64_t start_lsn, uint64_t end_lsn)
{
  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  
  struct NvwalPredicateClosure predicate = {compare_le, (void*) start_lsn};  

  return nvwal_query_epoch_upper_bound(wal, 0, &predicate);
}

nvwal_epoch_t query_epoch_range_end(TestContext& context, uint64_t start_lsn, uint64_t end_lsn)
{
  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  
  struct NvwalPredicateClosure predicate = {compare_ge, (void*) end_lsn};

  return nvwal_query_epoch_lower_bound(wal, 1, &predicate);
}


TEST(NvwalTagTest, OneEpoch) 
{
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  const uint64_t epoch_size = 64;

  tag_and_persist_epoch(context, 1, epoch_size, 0, epoch_size);
 
  MdsEpochMetadata em;

  EXPECT_EQ(1, query_epoch_lower_bound_ge(context, 0, 1));

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalTagTest, ThreeEpochs) 
{
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  const uint64_t epoch_size = 64;

  tag_and_persist_epoch(context, 1, epoch_size, 0, epoch_size);
  tag_and_persist_epoch(context, 2, epoch_size, epoch_size, 2*epoch_size);
  tag_and_persist_epoch(context, 3, epoch_size, 2*epoch_size, 3*epoch_size);
 
  MdsEpochMetadata em;

  EXPECT_EQ(1, query_epoch_range_begin(context, 0, epoch_size));
  EXPECT_EQ(1, query_epoch_range_end(context, 0, epoch_size));

  EXPECT_EQ(1, query_epoch_range_begin(context, 0, epoch_size-1));
  EXPECT_EQ(1, query_epoch_range_end(context, 0, epoch_size-1));

  EXPECT_EQ(2, query_epoch_range_begin(context, epoch_size, epoch_size*2));
  EXPECT_EQ(2, query_epoch_range_end(context, epoch_size, epoch_size*2));

  EXPECT_EQ(2, query_epoch_range_begin(context, epoch_size+1, epoch_size*2));
  EXPECT_EQ(2, query_epoch_range_end(context, epoch_size+1, epoch_size*2));

  EXPECT_EQ(1, query_epoch_range_begin(context, epoch_size-1, epoch_size*2));
  EXPECT_EQ(2, query_epoch_range_end(context, epoch_size-1, epoch_size*2));

  EXPECT_EQ(1, query_epoch_range_begin(context, epoch_size-1, epoch_size*2-1));
  EXPECT_EQ(2, query_epoch_range_end(context, epoch_size-1, epoch_size*2-1));

  EXPECT_EQ(1, query_epoch_range_begin(context, epoch_size-1, epoch_size*2+1));
  EXPECT_EQ(3, query_epoch_range_end(context, epoch_size-1, epoch_size*2+1));

  EXPECT_EQ(0, context.uninit_all());
}


#if 0
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
    EXPECT_EQ(query_metadata, em.user_metadata_1_);
    EXPECT_EQ(((e-1)*kBytes) / wal->config_.segment_size_ + 1, em.from_seg_id_);
    EXPECT_EQ(((e-1)*kBytes) % wal->config_.segment_size_, em.from_offset_);
  }

  EXPECT_EQ(0, context.uninit_all());
}
#endif

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalTagTest);
