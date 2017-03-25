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

#include "nvwal_test_mds_common.hpp"

#include <execinfo.h>
#include <errno.h>
#include <signal.h>
#include <tinyxml2.h>
#include <unistd.h>
#include <valgrind.h>

#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <boost/filesystem.hpp>

#include "nvwal_api.h"
#include "nvwal_debug.h"
#include "nvwal_impl_mds.h"
#include "nvwal_mds.h"
#include "nvwal_stacktrace.hpp"
#include "nvwal_types.h"

namespace nvwaltest {
nvwal_error_t MdsTestContext::__init_internal(
  bool init_io, 
  bool init_bufmgr, 
  std::string unique_root_path, 
  enum NvwalInitMode mode,
  struct NvwalControlBlock* cb) 
{
  /* 
   * Record what components we initialized so that we properly uninitialize
   * when the destructor gets called.
   */
  init_io_ = init_io;
  init_bufmgr_ = init_bufmgr;

  boost::filesystem::path root_path;

  if (unique_root_path == "") {
    std::string random_name = get_random_name();
    root_path = boost::filesystem::system_complete(random_name);
  } else {
    root_path = boost::filesystem::system_complete(unique_root_path);
  }
  unique_root_path_ = root_path.string();

  if (mode == kNvwalInitCreateTruncate) {
    boost::filesystem::remove_all(unique_root_path_);
    mode = kNvwalInitCreateIfNotExists;
  }
  
  if (mode == kNvwalInitCreateIfNotExists) {
    if (!boost::filesystem::create_directories(unique_root_path_)) {
      std::cerr << "MdsTestContext::init_all() : Fatal! failed to create the folder:"
         << unique_root_path_ << ". Check permissions etc." << std::endl;
      return ENOENT;
    }
  }

  wal_resources_.resize(wal_count_);

  // TODO(Hideaki) : following must be based on sizing_
  const uint64_t kWriterBufferSize = 1ULL << 12;
  const uint16_t kWriterCount = 2;
  const uint32_t kSegSize = 1U << 12;
  const uint64_t kNvQuota = 1ULL << 16;
  for (int w = 0; w < wal_count_; ++w) {
    auto* resource = get_resource(w);
    auto* wal = &resource->wal_instance_;
    std::memset(wal, 0, sizeof(*wal));

    std::string w_str = std::to_string(w);
    boost::filesystem::path wal_root = root_path;
    wal_root /= w_str;
    if (mode == kNvwalInitCreateIfNotExists) {
      if (!boost::filesystem::create_directory(wal_root)) {
        std::cerr << "MdsTestContext::init_all() : Fatal! failed to create the folder:"
          << wal_root.string() << ". Check permissions etc." << std::endl;
        return ENOENT;
      }
    }

    NvwalConfig config;
    std::memset(&config, 0, sizeof(config));
    // Both disk_root and nv_root are wal_root.
    std::memcpy(config.disk_root_, wal_root.string().data(), wal_root.string().length());
    std::memcpy(config.nv_root_, wal_root.string().data(), wal_root.string().length());
    config.mds_page_size_ = kNvwalMdsPageSize;
    memcpy(&wal->config_, &config, sizeof(config));

    nvwal_debug_init(WARNING);

    // allocate some pseudo nv control block as wal initialization would allocate it
    wal->nv_control_block_ = new NvwalControlBlock;
    if (cb) {
      *wal->nv_control_block_ = *cb;
    } else {
      memset(wal->nv_control_block_, 0, sizeof(NvwalControlBlock));
    }

    nvwal_error_t ret;
    if (init_io && init_bufmgr) {
      ret = mds_init(mode, wal);
    } else {
      if (init_io) {
        int did_restart;
        ret = mds_io_init(mode, wal, &did_restart);
      }
      if (init_bufmgr) {
        int did_restart;
        ret = mds_bufmgr_init(mode, wal, &did_restart);
      }
    }
    if (ret) {
      std::cerr << "MdsTestContext::init_all() : Fatal! failed to initialize metadata store instance-"
        << w << ". errno=" << ret << std::endl;
      return ret;
    }

  }

  return 0;
}

nvwal_error_t MdsTestContext::__uninit_internal(bool uninit_io, bool uninit_bufmgr, bool remove_all) {
  nvwal_error_t last_error = 0;
  for (int w = 0; w < wal_count_; ++w) {
    auto* resource = get_resource(w);
    auto* wal = &resource->wal_instance_;
    // Here, we assume uninit methods are idempotent.
    nvwal_error_t ret;
    if (uninit_bufmgr && uninit_io) {
      ret = mds_uninit(wal);
    } else {
      if (uninit_bufmgr) {
        ret = mds_bufmgr_uninit(wal);
      }
      if (uninit_io) {
        ret = mds_io_uninit(wal);
      }
    }
    if (wal->nv_control_block_) {
      delete wal->nv_control_block_;
      wal->nv_control_block_ = NULL;
    }
    if (ret) {
      last_error = ret;
    }
  }
  if (remove_all) {
    boost::filesystem::remove_all(unique_root_path_);
  }
  return last_error;
}

std::string MdsTestContext::get_random_name() {
  // In this unittest suite, we are lazy.
  // We just use process ID.
  // We never concurrently run multiple testcases from the same process,
  // so this should be safe.
  const char* kHexChars = "0123456789abcdef";
  uint32_t seed32 = ::getpid();
  std::string s("%%%%_%%%%_%%%%_%%%%");
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%') {                 // digit request
      seed32 = ::rand_r(&seed32);
      s[i] = kHexChars[seed32 & 0xf];  // convert to hex digit and replace
    }
  }
  return s;
}


}  // namespace nvwaltest
