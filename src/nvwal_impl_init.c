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
#include "nvwal_impl_init.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "nvwal_atomics.h"
#include "nvwal_api.h"
#include "nvwal_util.h"

/**************************************************************************
 *
 *  Thread state-changes
 *
 ***************************************************************************/

void nvwal_impl_thread_state_get_ready(uint8_t* thread_state) {
  assert(*thread_state == kNvwalThreadStateNotInitialized);
  nvwal_atomic_store(thread_state, kNvwalThreadStateAcceptStart);
}

enum NvwalThreadState nvwal_impl_thread_state_try_start(uint8_t* thread_state) {
  uint8_t expected = kNvwalThreadStateAcceptStart;
  const uint8_t desired = kNvwalThreadStateRunning;
  uint8_t swapped = nvwal_atomic_compare_exchange_strong(
    thread_state,
    &expected,
    desired);

  if (swapped) {
    return kNvwalThreadStateRunning;
  } else {
    assert(expected != kNvwalThreadStateRunning);
    return expected;
  }
}

void nvwal_impl_thread_state_stop(
  uint8_t* thread_state) {
  if ((*thread_state) == kNvwalThreadStateNotInitialized) {
    /** Then there is no race. The thread hasn't started either */
    return;
  }

  /** Try direct transition without requesting the thread */
  {
    uint8_t expected = kNvwalThreadStateAcceptStart;
    const uint8_t desired = kNvwalThreadStateProhibitStart;
    uint8_t swapped = nvwal_atomic_compare_exchange_strong(
      thread_state,
      &expected,
      desired);
    if (swapped) {
      /** The thread hasn't started */
      return;
    }
  }


  /** Ok, the thread has started. Then request the thread to stop */
  assert((*thread_state) == kNvwalThreadStateRunning
    || (*thread_state) == kNvwalThreadStateStopped);
  uint8_t expected = kNvwalThreadStateRunning;
  const uint8_t desired = kNvwalThreadStateRunningAndRequestedStop;
  uint8_t swapped = nvwal_atomic_compare_exchange_strong(
    thread_state,
    &expected,
    desired);
  if (swapped) {
    /** Sent out the request */
    /** Then, we have to wait until the thread changes it to Stopped */
    while (nvwal_atomic_load(thread_state) != kNvwalThreadStateStopped) {
      sched_yield();
    }
  } else {
    /** The thread has already stopped for whatever reason. */
    assert((*thread_state) == kNvwalThreadStateStopped);
  }
}

void nvwal_impl_thread_state_stopped(uint8_t* thread_state) {
  assert((*thread_state) == kNvwalThreadStateRunning
      || (*thread_state) == kNvwalThreadStateRunningAndRequestedStop);
  nvwal_atomic_store(thread_state, kNvwalThreadStateStopped);
}


/**************************************************************************
 *
 *  NvwalContext Initializations
 *
 ***************************************************************************/

/** subroutine of nvwal_init() used to create fresh new segment, not during restart */
nvwal_error_t init_fresh_nvram_segment(
  struct NvwalContext * wal,
  struct NvwalLogSegment* segment,
  nvwal_dsid_t dsid);

void remove_trailing_slash(char* path, uint16_t* len) {
  while ((*len) > 0 && path[(*len) - 1] == '/') {
    path[(*len) - 1] = '\0';
    --(*len);
  }
}

nvwal_error_t nvwal_impl_init(
  const struct NvwalConfig* given_config,
  struct NvwalContext* wal) {
  struct NvwalWriterContext* writer;
  struct NvwalConfig* config;
  nvwal_dsid_t resuming_dsid;

  /** otherwise the following memzero will also reset config, ouch */
  config = &(wal->config_);
  if (config == given_config) {
    return nvwal_raise_einval("Error: misuse, the WAL instance's own config object given\n");
  }

  memset(wal, 0, sizeof(*wal));
  memcpy(config, given_config, sizeof(*config));

  /** Check/adjust nv_root/disk_root */
  config->nv_root_len_ = strnlen(config->nv_root_, kNvwalMaxPathLength);
  config->disk_root_len_ = strnlen(config->disk_root_, kNvwalMaxPathLength);
  if (config->nv_root_len_ <= 1U) {
    return nvwal_raise_einval("Error: nv_root must be a valid full path\n");
  } else if (config->nv_root_len_ >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: nv_root must be null terminated\n");
  } else if (config->nv_root_len_ >= kNvwalMaxFolderPathLength) {
    return nvwal_raise_einval_llu(
      "Error: nv_root must be at most %llu characters\n",
      kNvwalMaxFolderPathLength);
  } else if (config->disk_root_len_ <= 1U) {
    return nvwal_raise_einval("Error: disk_root must be a valid full path\n");
  } else if (config->disk_root_len_ >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: disk_root_ must be null terminated\n");
  } else if (config->disk_root_len_ >= kNvwalMaxFolderPathLength) {
    return nvwal_raise_einval_llu(
      "Error: disk_root must be at most %llu characters\n",
      kNvwalMaxFolderPathLength);
  }
  remove_trailing_slash(config->nv_root_, &config->nv_root_len_);
  remove_trailing_slash(config->disk_root_, &config->disk_root_len_);

  /** Check writer_count, writer_buffer_size, writer_buffers */
  if (config->writer_count_ == 0 || config->writer_count_ > kNvwalMaxWorkers) {
    return nvwal_raise_einval_llu(
      "Error: writer_count must be 1 to %llu\n",
      kNvwalMaxWorkers);
  } else if ((config->writer_buffer_size_ % 512U) || config->writer_buffer_size_ == 0) {
    return nvwal_raise_einval(
      "Error: writer_buffer_size must be a non-zero multiply of page size (512)\n");
  }

  for (uint32_t i = 0; i < config->writer_count_; ++i) {
    if (!config->writer_buffers_[i]) {
      return nvwal_raise_einval_llu(
        "Error: writer_buffers[ %llu ] is null\n",
        i);
    }
  }

  if (config->segment_size_ % 512 != 0) {
    return nvwal_raise_einval(
      "Error: segment_size_ must be a multiply of 512\n");
  }
  if (config->segment_size_ == 0) {
    config->segment_size_ = kNvwalDefaultSegmentSize;
  }
  wal->segment_count_ = config->nv_quota_ / config->segment_size_;
  if (config->nv_quota_ % config->segment_size_ != 0) {
    return nvwal_raise_einval(
      "Error: nv_quota must be a multiply of segment size\n");
  } else if (wal->segment_count_ < 2U) {
    return nvwal_raise_einval(
      "Error: nv_quota must be at least of two segments\n");
  } else if (wal->segment_count_ > kNvwalMaxActiveSegments) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at most %llu segments\n",
      (uint64_t) kNvwalMaxActiveSegments);
  }

  if (config->resuming_epoch_ == kNvwalInvalidEpoch) {
    config->resuming_epoch_ = 1;
  }
  wal->durable_epoch_ = config->resuming_epoch_;
  wal->stable_epoch_ = config->resuming_epoch_;
  wal->next_epoch_ = nvwal_increment_epoch(config->resuming_epoch_);

  /** TODO we should retrieve from MDS in restart case */
  for (uint32_t i = 0; i < config->writer_count_; ++i) {
    writer = wal->writers_ + i;
    writer->parent_ = wal;
    writer->oldest_frame_ = 0;
    writer->active_frame_ = 0;
    writer->writer_seq_id_ = i;
    writer->last_tail_offset_ = 0;
    writer->copied_offset_ = 0;
    writer->buffer_ = config->writer_buffers_[i];
    memset(writer->epoch_frames_, 0, sizeof(writer->epoch_frames_));
    writer->epoch_frames_[0].log_epoch_ = config->resuming_epoch_;
  }

  /**
   * From now on, error-return might have to release a few things.
   * Invoke nvwal_uninit on error-return.
   */
  nvwal_error_t ret = 0;

  /* Initialize Metadata Store */
  /* riet = mds_init(&wal->config_, wal); */

  /* Initialize the reader context */
  /* ret = reader_init(&wal->reader_); */
  if (ret) {
    goto error_return;
  }

  /* Initialize all nv segments */
  resuming_dsid = 0 + 1;  /** TODO we should retrieve from MDS in restart case */
  for (uint32_t i = 0; i < wal->segment_count_; ++i) {
    memset(wal->segments_, 0, sizeof(struct NvwalLogSegment));
    ret = init_fresh_nvram_segment(wal, wal->segments_ + i, resuming_dsid + i);
    if (ret) {
      goto error_return;
    }
  }

  /* Created files on NVDIMM/Disk, fsync parent directory */
  ret = nvwal_open_and_fsync(config->nv_root_);
  if (ret) {
    goto error_return;
  }
  ret = nvwal_open_and_fsync(config->disk_root_);
  if (ret) {
    goto error_return;
  }

  /** Now we can start accepting flusher/fsyncher */
  nvwal_impl_thread_state_get_ready(&wal->flusher_thread_state_);
  nvwal_impl_thread_state_get_ready(&wal->fsyncer_thread_state_);

  return 0;

error_return:
  assert(ret);
  nvwal_impl_uninit(wal);
  /** Error code of uninit is ignored. Our own ret is probably more informative */
  errno = ret;
  return ret;
}

nvwal_error_t init_fresh_nvram_segment(
  struct NvwalContext* wal,
  struct NvwalLogSegment* segment,
  nvwal_dsid_t dsid) {
  char nv_path[kNvwalMaxPathLength];

  assert(dsid != kNvwalInvalidDsid);
  assert(wal->config_.nv_root_len_ + 32U < kNvwalMaxPathLength);
  segment->nv_fd_ = 0;
  segment->nv_baseaddr_ = 0;

  segment->parent_ = wal;
  segment->dsid_ = dsid;

  nvwal_concat_sequence_filename(
    wal->config_.nv_root_,
    "nv_segment_",
    dsid,
    nv_path);

  segment->nv_fd_ = nvwal_open_best_effort_o_direct(
    nv_path,
    O_CREAT | O_RDWR,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);

  if (segment->nv_fd_ == -1) {
    /** Failed to open/create the file! */
    assert(errno);
    return errno;
  }

  assert(segment->nv_fd_);  /** open never returns 0 */

  if (posix_fallocate(segment->nv_fd_, 0, wal->config_.segment_size_)) {
    /** posix_fallocate doesn't set errno, do it ourselves */
    errno = EINVAL;
    return EINVAL;
  }

  if (ftruncate(segment->nv_fd_, wal->config_.segment_size_)) {
    /** Failed to set the file length! */
    assert(errno);
    return errno;
  }

  if (fsync(segment->nv_fd_)) {
    /** Failed to fsync! */
    assert(errno);
    return errno;
  }

  /**
   * Don't bother (non-transparent) huge pages. Even libpmem doesn't try it.
   */
  segment->nv_baseaddr_ = mmap(0,
                  wal->config_.segment_size_,
                  PROT_READ | PROT_WRITE,
                  MAP_ANONYMOUS | MAP_PRIVATE,
                  segment->nv_fd_,
                  0);

  if (segment->nv_baseaddr_ == MAP_FAILED) {
    assert(errno);
    return errno;
  }
  assert(segment->nv_baseaddr_);

  /*
   * Even with fallocate we don't trust the metadata to be stable
   * enough for userspace durability. Actually write some bytes!
   */
  memset(segment->nv_baseaddr_, 0, wal->config_.segment_size_);
  msync(segment->nv_baseaddr_, wal->config_.segment_size_, MS_SYNC);
  return 0;
}

/**************************************************************************
 *
 *  NvwalContext Un-Initializations
 *
 ***************************************************************************/

/** subroutine of nvwal_uninit() */
nvwal_error_t uninit_log_segment(struct NvwalLogSegment* segment);

nvwal_error_t nvwal_impl_uninit(
  struct NvwalContext* wal) {
  /** Stop flusher and fsyncer */
  nvwal_impl_thread_state_stop(&wal->flusher_thread_state_);
  nvwal_impl_thread_state_stop(&wal->fsyncer_thread_state_);

  /** uninit continues as much as possible even after an error. */
  nvwal_error_t last_seen_error = 0;

  for (int i = 0; i < wal->segment_count_; ++i) {
    last_seen_error = nvwal_stock_error_code(
      last_seen_error,
      uninit_log_segment(wal->segments_ + i));
  }

  /* Uninitialize reader */
  /* last_seen_error = nvwal_stock_error_code(last_seen_error, reader_uninit(&wal->reader_)); */
  return last_seen_error;
}

nvwal_error_t uninit_log_segment(struct NvwalLogSegment* segment) {
  nvwal_error_t ret;  /* last-seen error code */

  ret = 0;

  if (segment->nv_baseaddr_ && segment->nv_baseaddr_ != MAP_FAILED) {
    if (munmap(segment->nv_baseaddr_, segment->parent_->config_.segment_size_) == -1) {
      ret = nvwal_stock_error_code(ret, errno);
    }
  }
  segment->nv_baseaddr_ = 0;

  if (segment->nv_fd_ && segment->nv_fd_ != -1) {
    if (close(segment->nv_fd_) == -1) {
      ret = nvwal_stock_error_code(ret, errno);
    }
  }
  segment->nv_fd_ = 0;

  memset(segment, 0, sizeof(struct NvwalLogSegment));

  return ret;
}
