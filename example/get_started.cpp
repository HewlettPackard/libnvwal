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

/**
 * @file get_started.cpp
 * A minimal example to use libnvwal. For more advanced use cases,
 * please check out other examples under this folder.
 */

#include <assert.h>
#include <errno.h>
#include <nvwal_api.h>
#include <stdint.h>
#include <gflags/gflags.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <boost/filesystem.hpp>

DEFINE_int32(mode, 0, "mode=1 is the first stage to newly populate a WAL "
    " while mode=2 is the second stage to recover the WAL.");
DEFINE_string(nvm_folder, "",
  "Folder under which we place NVDIMM-resident files."
  "Note: Create this folder upfront AND set permission to read-writable for yourself.");
DEFINE_string(disk_folder, "",
  "Folder under which we place Disk-resident files."
  "Note: Create this folder upfront AND set permission to read-writable for yourself.");

/**
 * libnvwal is completely agnostic to the format of log entryies.
 * Each of them could be even a set fo smaller log entries.
 * To libnvwal, it is just a chunk of bytes.
 */
struct MyLogEntry {
  void populate(int32_t val) {
    some_int_ = val;
    std::memset(some_string_, static_cast<char>(val), sizeof(some_string_));
  }
  void read_verify() const {
    bool error_found = false;
    for (int i = 0; i < sizeof(some_string_); ++i) {
      if (some_string_[i] != static_cast<char>(some_int_)) {
        error_found = true;
        break;
      }
    }

    if (error_found) {
      std::cerr << "Incorrect log entry. int=" << some_int_ << std::endl;
    } else {
      std::cout << "Yes, a correct MyLogEntry-" << some_int_ << std::endl;
    }
  }

  int32_t some_int_;
  char some_string_[60];
};

int main(int argc, char **argv) {
  gflags::SetUsageMessage("A minimal demo/example of libnvwal."
      "The demo consists of three stages.\n"
      " Suppose you have /mnt/nvm/pmem0/nvwal_nvm as your NVDIMM-resident folder\n"
      " and /home/yourname/nvwal_disk as your disk-resident folder.\n"
      " \n"
      " First, create the folders and set permissions. \n"
      "  Otherwise you have to run this demo as sudo. \n"
      "   rm -rf /mnt/nvm/pmem0/nvwal_nvm; sudo mkdir /mnt/nvm/pmem0/nvwal_nvm; sudo chmod 777 /mnt/nvm/pmem0/nvwal_nvm\n"
      "   rm -rf ~/nvwal_disk; mkdir ~/nvwal_disk\n"
      " \n"
      " Second, run this program as follows: \n"
      "   ./get_started --mode=1 --nvm_folder=/mnt/nvm/pmem0/nvwal_nvm --disk_folder=/home/yourname/nvwal_disk\n"
      " It will stop after a message to request you to kill the machine. \n"
      " Do it! This can be power-reset, unplug, kernel-panic, etc etc. \n"
      " \n"
      " Third, after the reboot of the machine, run this program as follows: \n"
      "   ./get_started --mode=2 --nvm_folder=/mnt/nvm/pmem0/nvwal_nvm --disk_folder=/home/yourname/nvwal_disk\n"
      " It will show all log entries written right before the crash\n"
  );
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_mode != 1 && FLAGS_mode != 2) {
    std::cerr << "You must specify a valid mode." << std::endl;
    gflags::ShowUsageWithFlags(argv[0]);
    return 1;
  }
  const NvwalInitMode mode = (FLAGS_mode == 1 ? kNvwalInitCreateTruncate : kNvwalInitRestart);

  if (!boost::filesystem::exists(FLAGS_nvm_folder)) {
    std::cerr << "NVM-folder " << FLAGS_nvm_folder << " does not exist." << std::endl;
    return 1;
  }
  if (!boost::filesystem::exists(FLAGS_disk_folder)) {
    std::cerr << "Disk-folder " << FLAGS_disk_folder << " does not exist." << std::endl;
    return 1;
  }

  NvwalConfig config;
  std::memset(&config, 0, sizeof(config));
  std::strcpy(config.nv_root_, FLAGS_nvm_folder.c_str());
  std::strcpy(config.disk_root_, FLAGS_disk_folder.c_str());
  config.nv_quota_ = 1U << 12;
  config.segment_size_ = 1U << 9;

  const uint32_t kWriterBufferSize = 1U << 9;
  config.writer_count_ = 1;
  config.writer_buffer_size_ = kWriterBufferSize;
  // Circular log-buffers each application should provide
  std::unique_ptr< char[] > writer_buffer(new char[kWriterBufferSize]);
  config.writer_buffers_[0] = reinterpret_cast<nvwal_byte_t*>(writer_buffer.get());

  std::unique_ptr< NvwalContext > nvwal_context(new NvwalContext());
  NvwalContext* wal = nvwal_context.get();
  if (nvwal_init(&config, mode, wal)) {
    std::cerr << "nvwal_init failed. errno=" << errno << std::endl;
    return 1;
  }

  // Each worker thread that emits logs occupy a NvwalWriterContext
  NvwalWriterContext* writer = wal->writers_ + 0;

  // Flusher/fsyncer thread each application should provide
  std::thread flusher_thread([wal](){
    if (nvwal_flusher_main(wal)) {
      std::cerr << "nvwal_flusher_main returned errno=" << errno << std::endl;
    }
  });

  std::thread fsyncer_thread([wal](){
    if (nvwal_fsync_main(wal)) {
      std::cerr << "nvwal_fsync_main returned errno=" << errno << std::endl;
    }
  });

  nvwal_wait_for_flusher_start(wal);
  nvwal_wait_for_fsync_start(wal);

  if (FLAGS_mode == 1) {
    std::cout << "Started demo in fresh-start mode..." << std::endl;
    for (int i = 0; i < 10; ++i) {
      MyLogEntry* cur =
        reinterpret_cast<MyLogEntry*>(
          writer_buffer.get() + ((i * sizeof(MyLogEntry)) % kWriterBufferSize));
      cur->populate(i);
      const nvwal_epoch_t cur_epoch = i + 1U;
      if (nvwal_on_wal_write(writer, sizeof(*cur), cur_epoch)) {
        std::cerr << "nvwal_on_wal_write failed. errno=" << errno << std::endl;
        goto uninit;
      }
      if (nvwal_advance_stable_epoch(wal, cur_epoch)) {
        std::cerr << "nvwal_advance_stable_epoch failed. errno=" << errno << std::endl;
        goto uninit;
      }
      while (true) {
        nvwal_epoch_t durable_epoch;
        if (nvwal_query_durable_epoch(wal, &durable_epoch)) {
          std::cerr << "nvwal_query_durable_epoch failed. errno=" << errno << std::endl;
          goto uninit;
        }
        if (nvwal_is_epoch_equal_or_after(durable_epoch, cur_epoch)) {
          break;
        } else {
          std::this_thread::yield();
        }
      }
    }

    std::cout << "Has durably written 10 epochs. Now, you can KILL the machine" << std::endl;
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(5));
    }
  } else {
    std::cout << "Started demo in restart mode..." << std::endl;
    std::cout << "Epochs up to " << wal->durable_epoch_ << " are durable." << std::endl;
    NvwalLogCursor cursor;
    if (nvwal_open_log_cursor(wal, 1U, nvwal_increment_epoch(wal->durable_epoch_), &cursor)) {
      std::cerr << "nvwal_open_log_cursor failed. errno=" << errno << std::endl;
      goto uninit;
    }
    while (nvwal_cursor_is_valid(wal, &cursor)) {
      nvwal_epoch_t ep = nvwal_cursor_get_current_epoch(wal, &cursor);
      const nvwal_byte_t* data = nvwal_cursor_get_data(wal, &cursor);
      const uint64_t len = nvwal_cursor_get_data_length(wal, &cursor);
      assert(len == sizeof(MyLogEntry));
      const MyLogEntry* entry = reinterpret_cast<const MyLogEntry*>(data);
      entry->read_verify();
      if (nvwal_cursor_next(wal, &cursor)) {
        std::cerr << "nvwal_cursor_next failed. errno=" << errno << std::endl;
        nvwal_close_log_cursor(wal, &cursor);
        goto uninit;
      }
    }
    if (nvwal_close_log_cursor(wal, &cursor)) {
      std::cerr << "nvwal_close_log_cursor failed. errno=" << errno << std::endl;
      goto uninit;
    }
  }

uninit:
  if (nvwal_uninit(wal)) {
    std::cerr << "nvwal_uninit failed. errno=" << errno << std::endl;
    return 1;
  }

  if (fsyncer_thread.joinable()) {
    fsyncer_thread.join();
  }
  if (flusher_thread.joinable()) {
    flusher_thread.join();
  }

  return 0;
}
