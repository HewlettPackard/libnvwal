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

#include "nvwal_test_common.hpp"

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

TEST(NvwalReaderTest, ReadOneEpochOneSegment) {
  TestContext context(2);
  EXPECT_EQ(0, context.init_all());
  // Do something here
  EXPECT_EQ(0, context.uninit_all());
}
/*
TEST(NvwalReaderTest, Test2) {
  TestContext context(2);
  EXPECT_EQ(0, context.init_all());
  // Do something here
  EXPECT_EQ(0, context.uninit_all());
}
*/
}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalReaderTest);
