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
#include "nvwal_mds.h"
#include "nvwal_stacktrace.hpp"
#include "nvwal_types.h"

namespace nvwaltest {
nvwal_error_t MdsTestContext::init_all() {
  std::string random_name = get_random_name();
  boost::filesystem::path root_path = boost::filesystem::system_complete(random_name);
  unique_root_path_ = root_path.string();

  boost::filesystem::remove_all(unique_root_path_);

  if (!boost::filesystem::create_directories(unique_root_path_)) {
    std::cerr << "MdsTestContext::init_all() : Fatal! failed to create the folder:"
       << unique_root_path_ << ". Check permissions etc." << std::endl;
    return ENOENT;
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
    if (!boost::filesystem::create_directory(wal_root)) {
      std::cerr << "MdsTestContext::init_all() : Fatal! failed to create the folder:"
        << wal_root.string() << ". Check permissions etc." << std::endl;
      return ENOENT;
    }

    NvwalConfig config;
    std::memset(&config, 0, sizeof(config));
    // Both disk_root and nv_root are wal_root.
    std::memcpy(config.disk_root_, wal_root.string().data(), wal_root.string().length());
    std::memcpy(config.nv_root_, wal_root.string().data(), wal_root.string().length());
    config.mds_page_size_ = kNvwalMdsPageSize;

    auto ret = mds_init(&config, wal);
    if (ret) {
      std::cerr << "MdsTestContext::init_all() : Fatal! failed to initialize WAL instance-"
        << w << ". errno=" << ret << std::endl;
      return ret;
    }
  }

  return 0;
}

nvwal_error_t MdsTestContext::uninit_all() {
  nvwal_error_t last_error = 0;
#if 0
  for (int w = 0; w < wal_count_; ++w) {
    auto* resource = get_resource(w);
    // Here, we assume nvwal_uninit is idempotent.
    auto ret = nvwal_uninit(&resource->wal_instance_);
    if (ret) {
      last_error = ret;
    }
  }
#endif
  boost::filesystem::remove_all(unique_root_path_);
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
