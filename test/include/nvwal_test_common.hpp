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
#ifndef NVWAL_TEST_COMMON_HPP_
#define NVWAL_TEST_COMMON_HPP_

#include <string>
#include <memory>
#include <vector>

#include "nvwal_fwd.h"
#include "nvwal_types.h"

namespace nvwaltest {

/**
 * Each unit test holds one TestContext throughout the test execution.
 * See test_nvwal_example.cpp for how to use this class.
 */
class TestContext {
 public:
  enum InstanceSize {
    /**
     * Use this for most testcases to reduce resource consumption.
     * \li writer's buffer size : 4 KB
     * \li writers per WAL : 2
     * \li block_seg_size_, nv_seg_size_ : 4 KB
     * \li nv_quota_ : 1 MB
     */
    kTiny = 0,
    /**In some cases we might need this.. */
    kBig,
  };

  /** This does not invoke complex initialization. Call init_all() next. */
  TestContext(int wal_count, InstanceSize sizing)
    : wal_count_(wal_count), sizing_(sizing) {
  }
  TestContext(int wal_count)
    : wal_count_(wal_count), sizing_(kTiny) {
  }
  ~TestContext() {
    uninit_all();
  }

  /**
   * Most initialization happens here. Don't forget to check the return code!
   */
  nvwal_error_t init_all();

  /**
   * This is idempotent and the destructor automatically calls it.
   * Still, you should call this so that you can sanity-check the return value.
   */
  nvwal_error_t uninit_all();

  int get_wal_count() const { return wal_count_; }
  NvwalContext* get_wal(int wal_id) { return &wal_instances_[wal_id]; }

 private:
  TestContext(const TestContext&) = delete;
  TestContext& operator=(const TestContext&) = delete;

  /**
   * Returns one randomly generated name in "%%%%_%%%%_%%%%_%%%%" format.
   * It should be used as the root path so that all file paths are unique random.
   * This makes it possible to run an arbitrary number of tests in parallel.
   */
  static std::string get_random_name();

  const int wal_count_;
  const InstanceSize sizing_;
  std::string unique_root_path_;
  std::vector< NvwalContext > wal_instances_;

  /**
   * Writer's buffers point to pieces of this.
   */
  std::unique_ptr< char[] > writer_buffer_memory_;
};

/**
  * Register signal handlers to capture signals during testcase execution.
  */
void register_signal_handlers(
  const char* test_case_name,
  int argc,
  char** argv);

/**
  * As the name suggests, we write out an gtest's result xml file with error state so that
  * jenkins will get aware of some error if the process disappears without any trace,
  * for example ctest killed it (via SIGSTOP, which can't be captured) for timeout.
  */
void            pre_populate_error_result_xml();

}  // namespace nvwaltest

#define TEST_QUOTE(str) #str
#define TEST_EXPAND_AND_QUOTE(str) TEST_QUOTE(str)

/**
 * Put this macro to define a main() that registers signal handlers.
 * This is required to convert assertion failures (crashes) to failed tests and provide more
 * detailed information in google-test's result xml file.
 * This really should be a built-in feature in gtest...
 *
 * But, I'm not sure if I should blame ctest, jenkins, or gtest (or all of them).
 * Related URLs:
 *   https://groups.google.com/forum/#!topic/googletestframework/NK5cAEqsioY
 *   https://code.google.com/p/googletest/issues/detail?id=342
 *   https://code.google.com/p/googletest/issues/detail?id=311
 */
#define TEST_MAIN_CAPTURE_SIGNALS(test_case_name) \
  int main(int argc, char **argv) { \
    nvwaltest::register_signal_handlers( \
      TEST_EXPAND_AND_QUOTE(test_case_name), \
      argc, \
      argv); \
    nvwaltest::pre_populate_error_result_xml(); \
    ::testing::InitGoogleTest(&argc, argv); \
    return RUN_ALL_TESTS(); \
  }

#endif  // NVWAL_TEST_COMMON_HPP_
