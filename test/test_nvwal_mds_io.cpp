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
  struct NvwalMdsPageFile* file = mds_io_file(&wal->mds_.io_, 0);
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
