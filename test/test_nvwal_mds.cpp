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
  for (int i=0; i < num_epoch_batch; i++) {
    /* write batch */
    wal = context.get_wal(0);
    nvwal_epoch_t low = (i==0) ? 1 : batch_last_epoch[i-1]+1;
    nvwal_epoch_t high = batch_last_epoch[i];
    write_epoch_batch(wal, low, high);

    if (restart_after_each_batch) {
      /* shutdown, restart, and recover */
      std::string root_path = context.get_root_path();
      EXPECT_EQ(0, context.uninit_all(false));
      EXPECT_EQ(0, context.init_all(root_path, kNvwalInitRestart));
      wal = context.get_wal(0);
      EXPECT_EQ(high, mds_latest_epoch(wal));
    }
  }
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
  for (mds_epoch_iterator_init(wal, 1, batch_last_epoch[0], &iterator);
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
  for (mds_epoch_iterator_init(wal, 1, batch_last_epoch[0], &iterator);
       !mds_epoch_iterator_done(&iterator);
       mds_epoch_iterator_next(&iterator)) 
  {
    EXPECT_EQ(expected_epoch_id, iterator.epoch_metadata_->epoch_id_); 
    expected_epoch_id++;
  }
  EXPECT_EQ(0, context.uninit_all());
}



}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalMdsTest);
