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
#include <boost/filesystem.hpp>

#include "nvwal_test_common.hpp"
#include "nvwal_util.h"

namespace nvwaltest {

TEST(NvwalUtilTest, CircularMemcpy) {
  nvwal_byte_t circular_buffer[512];
  for (int i = 0; i < sizeof(circular_buffer); ++i) {
    circular_buffer[i] = (nvwal_byte_t) i;
  }

  nvwal_byte_t dest[128];
  std::memset(dest, 0, sizeof(dest));
  nvwal_circular_memcpy(
    dest,
    circular_buffer,
    sizeof(circular_buffer),
    0,
    sizeof(dest));

  for (int i = 0; i < sizeof(dest); ++i) {
    EXPECT_EQ((nvwal_byte_t) i, dest[i]);
  }

  std::memset(dest, 0, sizeof(dest));
  nvwal_circular_memcpy(
    dest,
    circular_buffer,
    sizeof(circular_buffer),
    42,
    sizeof(dest));
  for (int i = 0; i < sizeof(dest); ++i) {
    EXPECT_EQ((nvwal_byte_t) (42 + i), dest[i]);
  }
}

TEST(NvwalUtilTest, CircularMemcpyWrap) {
  nvwal_byte_t circular_buffer[512];
  for (int i = 0; i < sizeof(circular_buffer); ++i) {
    circular_buffer[i] = (nvwal_byte_t) i;
  }

  nvwal_byte_t dest[128];
  std::memset(dest, 0, sizeof(dest));
  nvwal_circular_memcpy(
    dest,
    circular_buffer,
    sizeof(circular_buffer),
    400,
    sizeof(dest));

  for (int i = 0; i < sizeof(dest); ++i) {
    if (i < (512 - 400)) {
      EXPECT_EQ((nvwal_byte_t) (i + 400), dest[i]);
    } else {
      EXPECT_EQ((nvwal_byte_t) (i + 400 - 512), dest[i]);
    }
  }
}

TEST(NvwalUtilTest, ConcatSequenceFilename) {
  char path[kNvwalMaxPathLength];
  std::memset(path, 0, sizeof(path));

  nvwal_concat_sequence_filename(
    "/hoge/foo/boo",
    "bar_",
    0x7BU,
    path);
  EXPECT_STREQ("/hoge/foo/boo/bar_0000007B", path);

  nvwal_concat_sequence_filename(
    "/",
    "bar_",
    0x7BU,
    path);
  EXPECT_STREQ("/bar_0000007B", path);

  nvwal_concat_sequence_filename(
    "/hoge/foo/boo/",
    "bar_",
    0x3F0AEU,
    path);
  EXPECT_STREQ("/hoge/foo/boo/bar_0003F0AE", path);

  nvwal_concat_sequence_filename(
    "/",
    "bar_",
    0x3F0AEU,
    path);
  EXPECT_STREQ("/bar_0003F0AE", path);

  nvwal_concat_sequence_filename(
    "",
    "bar_",
    0x3F0AEU,
    path);
  EXPECT_STREQ("bar_0003F0AE", path);
}

TEST(NvwalUtilTest, IsNonemptyDir) {
  boost::filesystem::path temp_folder_path = boost::filesystem::unique_path();
  const std::string str_path = temp_folder_path.native();

  EXPECT_EQ(0, nvwal_is_nonempty_dir(str_path.c_str()));
  boost::filesystem::create_directory(temp_folder_path);
  EXPECT_EQ(0, nvwal_is_nonempty_dir(str_path.c_str()));

  boost::filesystem::path child_path(temp_folder_path);
  child_path /= "child";
  boost::filesystem::create_directory(child_path);

  EXPECT_EQ(1U, nvwal_is_nonempty_dir(str_path.c_str()));

  EXPECT_EQ(0, nvwal_is_nonempty_dir(child_path.c_str()));
  boost::filesystem::remove(child_path);

  EXPECT_EQ(0, nvwal_is_nonempty_dir(str_path.c_str()));
  boost::filesystem::remove_all(temp_folder_path);
  EXPECT_EQ(0, nvwal_is_nonempty_dir(str_path.c_str()));
}

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalUtilTest);
