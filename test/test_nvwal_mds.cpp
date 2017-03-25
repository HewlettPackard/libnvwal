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

#include <cstring>


#include "nvwal_api.h"

#include "nvwal_mds.h"
#include "nvwal_test_mds_common.hpp"
#include "nvwal_impl_mds.h"

/**
 * @file test_nvwal_mds.cpp
 * Test the metadata store piece separately.
 */

namespace nvwaltest {

TEST(NvwalMdsTest, Init)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());
  EXPECT_EQ(0, context.uninit_all());
}

static void set_durable_epoch(struct NvwalContext* wal, nvwal_epoch_t epoch) 
{
  wal->nv_control_block_->flusher_progress_.durable_epoch_ = epoch;
}

static void set_paged_mds_epoch(struct NvwalContext* wal, nvwal_epoch_t epoch) 
{
  wal->nv_control_block_->flusher_progress_.paged_mds_epoch_ = epoch;
}

void write_epoch_batch(struct NvwalContext* wal, nvwal_epoch_t low, nvwal_epoch_t high) 
{
  struct MdsEpochMetadata epoch;
  for (int i=low; i<=high; i++) {
    memset(&epoch, 0, sizeof(epoch));
    epoch.epoch_id_ = i;
    EXPECT_EQ(0, mds_write_epoch(wal, &epoch));
  }
}

/*
 * Forces the metadata store to restart and recover after writing each batch.
 */
void write_multiple_epoch_batches(
  struct MdsTestContext& context, 
  int num_epoch_batch, 
  nvwal_epoch_t batch_last_epoch[],
  bool restart_after_each_batch = true)
{
  struct NvwalContext* wal;
  struct NvwalControlBlock cb;

  for (int i=0; i < num_epoch_batch; i++) {
    /* write batch */
    wal = context.get_wal(0);
    nvwal_epoch_t low = (i==0) ? 1 : batch_last_epoch[i-1]+1;
    nvwal_epoch_t high = batch_last_epoch[i];
    write_epoch_batch(wal, low, high);

    if (restart_after_each_batch) {
      /* shutdown, restart, and recover */
      std::string root_path = context.get_root_path();
      cb = context.get_nv_control_block(0);
      EXPECT_EQ(0, context.uninit_all(false));
      EXPECT_EQ(0, context.init_all(root_path, kNvwalInitRestart, &cb));
      wal = context.get_wal(0);
      EXPECT_EQ(high, mds_latest_epoch(wal));
    }
  }
} 

void read_and_verify_expected(struct NvwalContext* wal, nvwal_epoch_t low, nvwal_epoch_t high)
{
  struct MdsEpochIterator iterator;
  nvwal_epoch_t expected_epoch_id = low; 
  for (mds_epoch_iterator_init(wal, low, high+1, &iterator);
       !mds_epoch_iterator_done(&iterator);
       mds_epoch_iterator_next(&iterator)) 
  {
    EXPECT_EQ(expected_epoch_id, iterator.epoch_metadata_->epoch_id_); 
    expected_epoch_id++;
  }
}

void shutdown_and_restart(MdsTestContext& context)
{
    std::string root_path = context.get_root_path();
    struct NvwalControlBlock cb = context.get_nv_control_block(0);
    EXPECT_EQ(0, context.uninit_all(false));
    EXPECT_EQ(0, context.init_all(root_path, kNvwalInitRestart, &cb));
}


TEST(NvwalMdsTest, WriteEpochSingle)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[1];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = 1;

  write_multiple_epoch_batches(context, 1, batch_last_epoch);
  EXPECT_EQ(0, context.uninit_all());

}


TEST(NvwalMdsTest, WriteEpochOneBatchSmall)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[1];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = 10;

  write_multiple_epoch_batches(context, 1, batch_last_epoch);
  EXPECT_EQ(0, context.uninit_all());

}


TEST(NvwalMdsTest, WriteEpochOneBatchLarge)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[1];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = max_epochs_per_page(&wal->mds_)+1;

  write_multiple_epoch_batches(context, 1, batch_last_epoch);
  EXPECT_EQ(0, context.uninit_all());

}

TEST(NvwalMdsTest, WriteEpochTwoBatches)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[2];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = max_epochs_per_page(&wal->mds_)+1;
  batch_last_epoch[1] = max_epochs_per_page(&wal->mds_)+5;

  write_multiple_epoch_batches(context, 2, batch_last_epoch);
  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalMdsTest, ReadEpochOnePage)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[2];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = 5;

  write_multiple_epoch_batches(context, 1, batch_last_epoch, true);

  struct MdsEpochIterator iterator;
  nvwal_epoch_t expected_epoch_id = 1; 
  for (mds_epoch_iterator_init(wal, 1, batch_last_epoch[0]+1, &iterator);
       !mds_epoch_iterator_done(&iterator);
       mds_epoch_iterator_next(&iterator)) 
  {
    EXPECT_EQ(expected_epoch_id, iterator.epoch_metadata_->epoch_id_); 
    expected_epoch_id++;
  }
  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalMdsTest, ReadEpochTwoPages)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[2];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = max_epochs_per_page(&wal->mds_)+5;

  write_multiple_epoch_batches(context, 1, batch_last_epoch, true);

  struct MdsEpochIterator iterator;
  nvwal_epoch_t expected_epoch_id = 1; 
  for (mds_epoch_iterator_init(wal, 1, batch_last_epoch[0]+1, &iterator);
       !mds_epoch_iterator_done(&iterator);
       mds_epoch_iterator_next(&iterator)) 
  {
    EXPECT_EQ(expected_epoch_id, iterator.epoch_metadata_->epoch_id_); 
    expected_epoch_id++;
  }
  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalMdsTest, Rollback)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_all());

  nvwal_epoch_t batch_last_epoch[2];
  
  struct NvwalContext* wal = context.get_wal(0);
  batch_last_epoch[0] = max_epochs_per_page(&wal->mds_)+5;

  write_multiple_epoch_batches(context, 1, batch_last_epoch, true);

  read_and_verify_expected(wal, 1, batch_last_epoch[0]);

  mds_rollback_to_epoch(wal, max_epochs_per_page(&wal->mds_)+2);

  read_and_verify_expected(wal, 1, max_epochs_per_page(&wal->mds_)+2);

  shutdown_and_restart(context);

  read_and_verify_expected(wal, 1, max_epochs_per_page(&wal->mds_)+2);

  EXPECT_EQ(0, mds_rollback_to_epoch(wal, 5));

  read_and_verify_expected(wal, 1, 5);

  EXPECT_EQ(0, context.uninit_all());
}




}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalMdsTest);
