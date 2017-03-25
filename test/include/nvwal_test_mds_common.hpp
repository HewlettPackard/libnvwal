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
