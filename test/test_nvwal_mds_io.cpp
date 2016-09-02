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

#include "nvwal_test_mds_common.hpp"
#include "nvwal_impl_mds.h"

/**
 * @file test_nvwal_mds_io.cpp
 * Test the I/O subsystem of the metadata store separately.
 */

namespace nvwaltest {

TEST(NvwalMdsIoTest, Init)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_io());
  EXPECT_EQ(0, context.uninit_io());
}

TEST(NvwalMdsIoTest, AppendPage)
{
  MdsTestContext context(1);
  EXPECT_EQ(0, context.init_io());

  NvwalContext* wal = context.get_wal(0);
  struct PageFile* file = mds_io_file(&wal->mds_.io_, 0);
  void* pagebuf = malloc(wal->config_.mds_page_size_);
  memset(pagebuf, 0, wal->config_.mds_page_size_);
  ASSERT_NE((void*) NULL, pagebuf);
  mds_io_append_page(file, pagebuf);

  free(pagebuf);

  std::string root_path = context.get_root_path();
  EXPECT_EQ(0, context.uninit_io(false));

  /* test recovery code path */
  EXPECT_EQ(0, context.init_io(root_path, kNvwalInitRestart));
  EXPECT_EQ(0, context.uninit_io());
}

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalMdsIoTest);
