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

#include <time.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <gflags/gflags.h>

#include <boost/filesystem.hpp>

#include <chrono>
#include <iostream>
#include <mutex>

#include "nvwal_api.h"
#include "nvwal_types.h"
#include "nvwal_util.h"

DEFINE_string(nvm_folder, "",
  "Folder under which we place NVDIMM-resident files."
  "Note: Create this folder upfront AND set permission to read-writable for yourself.");

DEFINE_string(disk_folder, "",
  "Folder under which we place Disk-resident files."
  "Note: Create this folder upfront AND set permission to read-writable for yourself.");

DEFINE_uint64(nv_quota, 1U << 12, 
  "Total size of NV-DIMM space consumed by the write-ahead log.");

DEFINE_uint64(segment_size, 1U << 9, 
  "Byte size of each segment, either on disk or NVDIMM.");

DEFINE_uint64(mds_page_size, 1U << 9, 
  "Byte size of meta-data store page. Must be a multiple of 512.");

DEFINE_uint64(writer_buffer_size, 1U << 13, 
  "Size of (volatile) buffer for each writer-thread.");

DEFINE_uint64(nops, 1000000, 
  "Number of workload operations per worker.");

DEFINE_uint64(nbytes, 1U << 7, 
  "Number of bytes written per workload operation.");



const int max_args = 11;
char const alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";


void generate_random_buffer(nvwal_byte_t *buf, size_t const len)
{
  for (unsigned int i = 0; i < len; i++)
  {
    buf[i] = alphabet[rand()% sizeof(alphabet)];
  }
} 


/**
 * @brief Implements a LSN-based style write-ahead log.
 */
class LsnLog 
{
public:
  LsnLog(NvwalConfig config);

  nvwal_error_t init();
  nvwal_error_t uninit();

  nvwal_error_t log_and_commit(const nvwal_byte_t* buf, size_t size);

private:
  std::unique_ptr< char[] > writer_buffer_;

  NvwalConfig         config_;
  NvwalContext*       wal_;
  NvwalWriterContext* writer_;
  std::thread         flusher_thread_;
  std::thread         fsyncer_thread_;
  std::mutex          mutex_;
  nvwal_epoch_t       current_epoch_;
  uint64_t            head_offset_;
};

LsnLog::LsnLog(NvwalConfig config)
  : config_(config),
    head_offset_(0)
{
  config_.writer_count_ = 1;
  current_epoch_ = kNvwalInvalidEpoch;
}

nvwal_error_t LsnLog::init()
{

  // Circular log-buffers each application should provide
  writer_buffer_ = std::unique_ptr< char[] > (new char[config_.writer_buffer_size_]);
  config_.writer_buffers_[0] = reinterpret_cast<nvwal_byte_t*>(writer_buffer_.get());

  wal_ = new NvwalContext();

  if (nvwal_init(&config_, kNvwalInitCreateTruncate, wal_)) {
    std::cerr << "nvwal_init failed. errno=" << errno << std::endl;
    return 1;
  }

  // Each worker thread that emits logs occupy a NvwalWriterContext
  writer_ = wal_->writers_ + 0;

  // Local copy so that threads can capture it
  NvwalContext* wal = wal_;

  nvwal_query_durable_epoch(wal, &current_epoch_);
  current_epoch_ = nvwal_increment_epoch(current_epoch_);

  // Flusher/fsyncer thread each application should provide
  flusher_thread_ = std::thread([wal](){
    if (nvwal_flusher_main(wal)) {
      std::cerr << "nvwal_flusher_main returned errno=" << errno << std::endl;
    }
  });

  fsyncer_thread_ = std::thread([wal](){
    if (nvwal_fsync_main(wal)) {
      std::cerr << "nvwal_fsync_main returned errno=" << errno << std::endl;
    }
  });

  nvwal_wait_for_flusher_start(wal);
  nvwal_wait_for_fsync_start(wal);

  return 0;
}

nvwal_error_t LsnLog::uninit()
{
  nvwal_error_t ret;
  if (ret = nvwal_uninit(wal_)) {
    std::cerr << "nvwal_uninit failed. errno=" << errno << std::endl;
    return ret;
  }

  if (fsyncer_thread_.joinable()) {
    fsyncer_thread_.join();
  }

  if (flusher_thread_.joinable()) {
    flusher_thread_.join();
  }
 
  if (wal_) {
    delete wal_;
    wal_ = NULL;
  }
 
  return 0;
}


nvwal_error_t LsnLog::log_and_commit(const nvwal_byte_t* buf, size_t size)
{
  // never write more than half of buffer size
  assert(size < config_.writer_buffer_size_ / 2);

  mutex_.lock();

  while (!nvwal_has_enough_writer_space(writer_)) { /* spin */ }

  nvwal_circular_dest_memcpy(writer_->buffer_, config_.writer_buffer_size_, head_offset_, buf, size);
  head_offset_ = (head_offset_ + size) % config_.writer_buffer_size_;
  nvwal_error_t rc = nvwal_on_wal_write(writer_, size, current_epoch_);
  assert(rc == 0);
  nvwal_advance_stable_epoch(wal_, current_epoch_);

  while (true) {
    nvwal_epoch_t durable_epoch;
    if (nvwal_query_durable_epoch(wal_, &durable_epoch)) {
      std::cerr << "nvwal_query_durable_epoch failed. errno=" << errno << std::endl;
      assert(0 && "BOOM");
    }
    if (nvwal_is_epoch_equal_or_after(durable_epoch, current_epoch_)) {
      break;
    } else {
      std::this_thread::yield();
    }
  }


  current_epoch_ = nvwal_increment_epoch(current_epoch_);

  mutex_.unlock();

  return 0;
}


class Workload {
public:
  Workload(LsnLog* log);   
  nvwal_error_t run(int nthreads);
  
  int do_work(int tid);
private:
  LsnLog* log_;
};


Workload::Workload(LsnLog* log)
  : log_(log)
{
  
}

const int max_logrec_size_ = 1U << 16;

int Workload::do_work(int tid)
{
  nvwal_error_t rc;
  nvwal_byte_t buf[max_logrec_size_];
  generate_random_buffer(buf, max_logrec_size_);

  assert(FLAGS_nbytes < max_logrec_size_);

  auto start = std::chrono::steady_clock::now();

  for (int i=0; i<FLAGS_nops; i++) {
    log_->log_and_commit(buf, FLAGS_nbytes);
  }

  auto end = std::chrono::steady_clock::now();
  auto diff = end - start;

  std::cout << "workload_duration " << std::chrono::duration<double, std::ratio<1, 1000000>> (diff).count() << " us" << std::endl;
}

nvwal_error_t Workload::run(int nthreads)
{
    std::thread workers[nthreads];

    for (int i=0; i<nthreads; i++)
    {
      workers[i] = std::thread([this, i](){this->do_work(i);});
    }

    for (int i=0; i<nthreads; i++)
    {
      if (workers[i].joinable()) {workers[i].join();}
    }
}


int main(int argc, char *argv[])
{
  nvwal_error_t rc;

  gflags::SetUsageMessage(
    "A mibrobenchmark emulating a LSN-based log style workload for libnvwal.");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_nvm_folder.empty()) {
    std::cerr << "NVM-folder not given" << std::endl;
    return 1;
  }
  if (!boost::filesystem::exists(FLAGS_nvm_folder)) {
    std::cerr << "NVM-folder " << FLAGS_nvm_folder << " does not exist." << std::endl;
    return 1;
  }
  if (FLAGS_disk_folder.empty()) {
    std::cerr << "Disk-folder not given" << std::endl;
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
  config.nv_quota_ = FLAGS_nv_quota;
  config.segment_size_ = FLAGS_segment_size;
  config.mds_page_size_ = FLAGS_mds_page_size;
  config.writer_buffer_size_ = FLAGS_writer_buffer_size;

  LsnLog lsn_log(config);

  if (rc = lsn_log.init()) {
    std::cerr << "log::init failed. errno=" << errno << " " << strerror(errno) << std::endl;
    return rc;
  }

  Workload workload(&lsn_log);
  workload.run(1);

  lsn_log.uninit();

  return 0;
}
