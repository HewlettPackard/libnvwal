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
#ifndef NVWAL_TEST_MDS_COMMON_HPP_
#define NVWAL_TEST_MDS_COMMON_HPP_

#include <string>
#include <memory>
#include <thread>
#include <vector>

#include "nvwal_fwd.h"
#include "nvwal_types.h"

#include "nvwal_test_common.hpp"

namespace nvwaltest {

struct MdsWalResource {
  NvwalContext wal_instance_;
};

/**
 * Each metadata store unit test holds one MdsTestContext throughout the 
 * test execution.
 */
class MdsTestContext {
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
  MdsTestContext(int wal_count, InstanceSize sizing)
    : wal_count_(wal_count), sizing_(sizing) {
  }
  MdsTestContext(int wal_count)
    : wal_count_(wal_count), sizing_(kTiny) {
  }
  ~MdsTestContext() {
    __uninit_internal(init_io_, init_bufmgr_, true);
  }

  /**
   * Complete initialization. Don't forget to check the return code!
   */
  nvwal_error_t init_all(std::string root_path = "", enum NvwalInitMode mode = kNvwalInitCreateTruncate, struct NvwalControlBlock* cb = NULL) {
    return __init_internal(true, true, root_path, mode, cb);
  }

  /**
   * Partial initialization: initialize I/O subsystem only.
   */
  nvwal_error_t init_io(std::string root_path = "", enum NvwalInitMode mode = kNvwalInitCreateTruncate, struct NvwalControlBlock* cb = NULL) {
    return __init_internal(true, false, root_path, mode, cb);
  }

  /**
   * Partial initialization: initialize buffer subsystem only.
   */
  nvwal_error_t init_bufmgr(std::string root_path = "", enum NvwalInitMode mode = kNvwalInitCreateTruncate, struct NvwalControlBlock* cb = NULL) {
    return __init_internal(false, true, root_path, mode, cb);
  }

  /**
   * Most initialization happens here. Don't forget to check the return code!
   */
  nvwal_error_t __init_internal(bool init_io, bool init_bufmgr, std::string root_path, enum NvwalInitMode mode, struct NvwalControlBlock* cb);

  nvwal_error_t uninit_all(bool remove_all = true) {
    return __uninit_internal(true, true, remove_all);
  }

  nvwal_error_t uninit_io(bool remove_all = true) {
    return __uninit_internal(true, false, remove_all);
  }

  nvwal_error_t uninit_bufmgr(bool remove_all = true) {
    return __uninit_internal(false, true, remove_all);
  }

  /**
   * This is idempotent and the destructor automatically calls it.
   * Still, you should call this so that you can sanity-check the return value.
   */
  nvwal_error_t __uninit_internal(bool uninit_io, bool uninit_bufmgr, bool remove_all);

  int get_wal_count() const { return wal_count_; }
  MdsWalResource* get_resource(int wal_id) { return &wal_resources_[wal_id]; }
  NvwalContext* get_wal(int wal_id) { return &wal_resources_[wal_id].wal_instance_; }
  std::string get_root_path() { return unique_root_path_; }

  struct NvwalControlBlock get_nv_control_block(int wal_id) { return *(get_wal(wal_id)->nv_control_block_); }

 private:
  MdsTestContext(const MdsTestContext&) = delete;
  MdsTestContext& operator=(const MdsTestContext&) = delete;

  /**
   * Returns one randomly generated name in "%%%%_%%%%_%%%%_%%%%" format.
   * It should be used as the root path so that all file paths are unique random.
   * This makes it possible to run an arbitrary number of tests in parallel.
   */
  static std::string get_random_name();

  const int wal_count_;
  const InstanceSize sizing_;
  std::string unique_root_path_;
  std::vector< MdsWalResource > wal_resources_;

  /**
   * Record what components we initialized so that we properly uninitialize 
   * when the destructor gets called.
   */
  bool init_io_;
  bool init_bufmgr_;
};

}  // namespace nvwaltest

#endif  // NVWAL_TEST_MDS_COMMON_HPP_
