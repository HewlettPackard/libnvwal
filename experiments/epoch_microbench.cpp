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

#include <time.h>
#include <string.h>
#include <thread>
#include <assert.h>
#include <gflags/gflags.h>

#include <boost/filesystem.hpp>

#include <chrono>
#include <condition_variable>
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

DEFINE_uint64(nwriters, 1, 
  "Number of log writers");

DEFINE_uint64(nthreads, 1, 
  "Number of worker threads per log writer");

DEFINE_uint64(nops, 1000000, 
  "Number of workload operations per worker.");

DEFINE_uint64(nlogrec, 8, 
  "Number of log records written per workload operation.");

DEFINE_uint64(logrec_size, 1U << 7, 
  "Byte size of log secord");



const int max_args = 11;
char const alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";


void generate_random_buffer(nvwal_byte_t *buf, size_t const len)
{
  for (unsigned int i = 0; i < len; i++)
  {
    buf[i] = alphabet[rand()% sizeof(alphabet)];
  }
} 

/*
 * TYPE CLASS DECLARATIONS
 */

class Log;

/**
 * @brief Implements a log writer
 */
class LogWriter 
{
public:
  LogWriter(int id, NvwalConfig config, Log* log, NvwalWriterContext* writer)
    : id_(id),
      config_(config),
      log_(log),
      writer_(writer),
      head_offset_(0)
  { }

  nvwal_error_t log_and_commit(const nvwal_byte_t* buf, int nrec, size_t size);

private:
  int                 id_;
  NvwalConfig         config_;
  Log*                log_;
  NvwalWriterContext* writer_;
  std::mutex          mutex_;
  uint64_t            head_offset_;
};


/**
 * @brief Implements a write-ahead log.
 */
class Log 
{
public:
  Log(NvwalConfig config, int writer_count);

  nvwal_error_t init();
  nvwal_error_t uninit();

  nvwal_error_t log(NvwalWriterContext* writer, size_t size);
  nvwal_error_t commit(NvwalWriterContext* writer);
 
  LogWriter* log_writer(int i) 
  {
    return log_writer_[i];
  }

private:
  std::unique_ptr< char[] > writer_buffer_[64];

  NvwalConfig             config_;
  NvwalContext*           wal_;
  int                     writer_count_;
  int                     cur_epoch_writer_count_;
  LogWriter*              log_writer_[64];
  std::thread             flusher_thread_;
  std::thread             fsyncer_thread_;
  std::mutex              mutex_;
  std::condition_variable cv_;
  nvwal_epoch_t           current_epoch_;
};

Log::Log(NvwalConfig config, int writer_count)
  : config_(config),
    writer_count_(writer_count),
    cur_epoch_writer_count_(0)
{
  config_.writer_count_ = writer_count;
  current_epoch_ = kNvwalInvalidEpoch;
}

class Workload {
public:
  Workload(Log* log);   
  nvwal_error_t run(int nthreads);
  
  int do_work(int tid);
private:
  Log* log_;
};


/*
 * CLASS METHOD IMPLEMENTATION
 */

nvwal_error_t LogWriter::log_and_commit(const nvwal_byte_t* buf, int nrec, size_t size)
{
  // never write more than half of buffer size
  assert(size < config_.writer_buffer_size_ / 2);

  mutex_.lock();

  for (int i = 0; i < nrec; i++) {
    while (!nvwal_has_enough_writer_space(writer_)) { /* spin */ }

    nvwal_circular_dest_memcpy(writer_->buffer_, config_.writer_buffer_size_, head_offset_, buf, size);
    head_offset_ = (head_offset_ + size) % config_.writer_buffer_size_;

    log_->log(writer_, size);
  }
  log_->commit(writer_);

  mutex_.unlock();

  return 0;
}

nvwal_error_t Log::init()
{

  // Circular log-buffers each application should provide
  for (int i = 0; i < writer_count_; i++) {
    writer_buffer_[i] = std::unique_ptr< char[] > (new char[config_.writer_buffer_size_]);
    config_.writer_buffers_[i] = reinterpret_cast<nvwal_byte_t*>(writer_buffer_[i].get());
  }

  wal_ = new NvwalContext();

  if (nvwal_init(&config_, kNvwalInitCreateTruncate, wal_)) {
    std::cerr << "nvwal_init failed. errno=" << errno << std::endl;
    return 1;
  }

  // Each worker thread that emits logs occupy a NvwalWriterContext
  for (int i = 0; i < writer_count_; i++) {
    log_writer_[i] = new LogWriter(i, config_, this, wal_->writers_ + i);
  }

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

nvwal_error_t Log::uninit()
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

nvwal_error_t Log::log(NvwalWriterContext* writer, size_t size)
{
  nvwal_error_t rc = nvwal_on_wal_write(writer, size, current_epoch_);
  assert(rc == 0);
  return rc;
}

nvwal_error_t Log::commit(NvwalWriterContext* writer)
{
  std::unique_lock<std::mutex> lk(mutex_);

  cur_epoch_writer_count_++; 

  if (cur_epoch_writer_count_ == writer_count_) {
    /* I am the last writer; make epoch durable */

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
    cur_epoch_writer_count_ = 0;
    cv_.notify_all();
  } else {
    cv_.wait(lk);
  }

  return 0;
}

Workload::Workload(Log* log)
  : log_(log)
{
  
}

const int max_logrec_size_ = 1U << 16;

int Workload::do_work(int tid)
{
  nvwal_error_t rc;
  nvwal_byte_t buf[max_logrec_size_];
  generate_random_buffer(buf, max_logrec_size_);

  assert(FLAGS_logrec_size < max_logrec_size_);

  LogWriter* log_writer = log_->log_writer(tid % FLAGS_nwriters);

  for (int i=0; i<FLAGS_nops; i++) {
    log_writer->log_and_commit(buf, FLAGS_nlogrec, FLAGS_logrec_size);
  }

}

nvwal_error_t Workload::run(int nthreads)
{
    std::thread workers[nthreads];

    auto start = std::chrono::steady_clock::now();

    for (int i=0; i<nthreads; i++)
    {
      workers[i] = std::thread([this, i](){this->do_work(i);});
    }

    for (int i=0; i<nthreads; i++)
    {
      if (workers[i].joinable()) {workers[i].join();}
    }

    auto end = std::chrono::steady_clock::now();
    auto diff = end - start;

    std::cout << "workload_duration " << std::chrono::duration<double, std::ratio<1, 1000000>> (diff).count() << " us" << std::endl;
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
  
  Log lsn_log(config, FLAGS_nwriters);

  if (rc = lsn_log.init()) {
    std::cerr << "log::init failed. errno=" << errno << " " << strerror(errno) << std::endl;
    return rc;
  }

  Workload workload(&lsn_log);
  workload.run(FLAGS_nthreads*FLAGS_nwriters);

  lsn_log.uninit();

  return 0;
}
