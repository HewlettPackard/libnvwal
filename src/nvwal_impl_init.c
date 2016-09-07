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
#include <libpmem.h>
#include <sched.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "nvwal_atomics.h"
#include "nvwal_api.h"
#include "nvwal_mds.h"
#include "nvwal_mds_types.h"
#include "nvwal_util.h"


/**************************************************************************
 *
 *  Thread state-changes
 *
 ***************************************************************************/

void nvwal_impl_thread_state_get_ready(uint8_t* thread_state) {
  assert(*thread_state == kNvwalThreadStateBeingInitialized);
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

void nvwal_impl_thread_state_wait_for_start(const uint8_t* thread_state) {
  assert(*thread_state == kNvwalThreadStateAcceptStart ||
    *thread_state == kNvwalThreadStateRunning);
  while (1) {
    sched_yield();
    if (nvwal_atomic_load(thread_state) == kNvwalThreadStateRunning) {
      break;
    }
  }
}

void nvwal_impl_thread_state_request_and_wait_for_stop(
  uint8_t* thread_state) {
  if ((*thread_state) == kNvwalThreadStateBeingInitialized
    || (*thread_state) == kNvwalThreadStateProhibitStart
    || (*thread_state) == kNvwalThreadStateStopped) {
    /** Then there is no race. The thread hasn't started either */
    return;
  }

  assert(
    (*thread_state) == kNvwalThreadStateAcceptStart
    || (*thread_state) == kNvwalThreadStateRunning
    || (*thread_state) == kNvwalThreadStateStopped);
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

void remove_trailing_slash(char* path, uint16_t* len) {
  while ((*len) > 0 && path[(*len) - 1] == '/') {
    path[(*len) - 1] = '\0';
    --(*len);
  }
}

/**
 * Simple standalone pre-screening checks/adjustments on the given config.
 * This is the first step in nvwal_init().
 */
nvwal_error_t sanity_check_config(
  struct NvwalConfig* config,
  enum NvwalInitMode mode) {
  config->libnvwal_version_ = nvwal_get_version();

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

  if (!nvwal_is_valid_dir(config->nv_root_)) {
    return nvwal_raise_einval_cstr(
      "Error: Specified nv_root '%s' is not a valid folder\n",
      config->nv_root_);
  } else if (!nvwal_is_valid_dir(config->disk_root_)) {
    return nvwal_raise_einval_cstr(
      "Error: Specified disk_root '%s' is not a valid folder\n",
      config->disk_root_);
  }

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
  uint64_t segment_count_ = config->nv_quota_ / config->segment_size_;
  if (config->nv_quota_ % config->segment_size_ != 0) {
    return nvwal_raise_einval(
      "Error: nv_quota must be a multiply of segment size\n");
  } else if (segment_count_ < 2U) {
    return nvwal_raise_einval(
      "Error: nv_quota must be at least of two segments\n");
  } else if (segment_count_ > kNvwalMaxActiveSegments) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at most %llu segments\n",
      (uint64_t) kNvwalMaxActiveSegments);
  }

  if (mode != kNvwalInitRestart
    && mode != kNvwalInitCreateIfNotExists
    && mode != kNvwalInitCreateTruncate) {
    return nvwal_raise_einval_llu(
      "Error: Invalid value of init mode: %llu\n",
      (uint64_t) mode);
  }

  if (mode == kNvwalInitCreateTruncate
    && config->resuming_epoch_ != kNvwalInvalidEpoch) {
    /** This could be just a warning.. so far an error */
    return nvwal_raise_einval(
      "Error: No point to specify resuming_epoch in CreateTruncate mode\n");
  }

  if (mode == kNvwalInitRestart) {
    /**
     * kNvwalInitRestart assumes the folder contains an existing WAL remnant.
     */
    if (!nvwal_is_nonempty_dir(config->nv_root_)) {
      return nvwal_raise_einval_cstr(
        "Error: Init mode is kNvwalInitRestart, but nv_root '%s' has no files\n",
        config->nv_root_);
    } else if (!nvwal_is_nonempty_dir(config->disk_root_)) {
      return nvwal_raise_einval_cstr(
        "Error: Init mode is kNvwalInitRestart, but disk_root '%s' has no files\n",
        config->disk_root_);
    }
  } else if (mode == kNvwalInitCreateTruncate) {
    /**
     * Truncate just nukes everything there, but let's print out a warning.
     * If there are some files, it might potentially be a misuse.
     */
    if (nvwal_is_nonempty_dir(config->nv_root_)) {
      nvwal_output_warning_cstr(
        "Warning: Init mode is CreateTruncate, but nv_root '%s' has some files.\n"
        " These files will be deleted. Is it really what you inteded?\n",
        config->nv_root_);
    }
    if (nvwal_is_nonempty_dir(config->disk_root_)) {
      nvwal_output_warning_cstr(
        "Warning: Init mode is CreateTruncate, but disk_root_ '%s' has some files.\n"
        " These files will be deleted. Is it really what you inteded?\n",
        config->disk_root_);
    }
  }

  return 0;
}

nvwal_error_t open_control_file(
  enum NvwalInitMode mode,
  struct NvwalContext* wal) {
  assert(wal->nv_control_file_fd_ == 0 || wal->nv_control_file_fd_ == -1);
  assert(!wal->nv_control_block_ || wal->nv_control_block_ == MAP_FAILED);
  /**
   * Whether restart or create, we try to load the control file first.
   */
  char cf_path[kNvwalMaxPathLength];
  assert(wal->config_.nv_root_len_ + 1U + 8U < kNvwalMaxPathLength);
  memcpy(cf_path, wal->config_.nv_root_, wal->config_.nv_root_len_);
  cf_path[wal->config_.nv_root_len_] = '/';
  memcpy(cf_path + wal->config_.nv_root_len_ + 1U, "nvwal.cf", 8);
  cf_path[wal->config_.nv_root_len_ + 1U + 8U] = '\0';

  uint64_t original_filesize = 0;
  {
    struct stat st;
    if (stat(cf_path, &st) == 0) {
      original_filesize = st.st_size;
    }
  }

  if (original_filesize && original_filesize != sizeof(struct NvwalControlBlock)) {
    return nvwal_raise_einval_cstr(
    "Error: File size of the control file '%s' is not compatible\n",
    cf_path);
  }

  wal->nv_control_file_fd_ = nvwal_open_best_effort_o_direct(
    cf_path,
    (mode == kNvwalInitRestart ? 0 : O_CREAT) | O_RDWR,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);


  if (wal->nv_control_file_fd_ == -1) {
    return nvwal_raise_einval_cstr(
      "Error: We could not open the control file '%s'\n",
      cf_path);
  }

  wal->nv_control_block_ = (struct NvwalControlBlock*) mmap(
    0,
    sizeof(struct NvwalControlBlock),
    PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS | MAP_PRIVATE,
    wal->nv_control_file_fd_,
    0);

  if (wal->nv_control_block_ == MAP_FAILED) {
    return errno;
  }

  if (original_filesize != sizeof(struct NvwalControlBlock)) {
    pmem_memset_persist(wal->nv_control_block_, 0, sizeof(struct NvwalControlBlock));
  }

  /** Take the image of previous config as of this point. */
  memcpy(
    &wal->prev_config_,
    &wal->nv_control_block_->config_,
    sizeof(struct NvwalConfig));

  return 0;
}

/**
 * Checks whether there is any discrepency between the given configuration
 * and previous configuration. We so far reject almost all discrepencies,
 * refusing to start up. Later, we might implement automatic conversion
 * to tolerate some difference.
 */
nvwal_error_t check_adjust_on_prev_config(
  struct NvwalContext* wal) {
  if (wal->prev_config_.libnvwal_version_ == 0) {
    /* Meaning this is a fresh start.  */
    return 0;
  } else if (wal->prev_config_.libnvwal_version_ != nvwal_get_version()) {
    return nvwal_raise_einval_llu(
      "Error: The existing libnvwal installation has a version number %llu, different from"
      " this binary's.\n",
      wal->prev_config_.libnvwal_version_);
  } else if (wal->prev_config_.mds_page_size_ != wal->config_.mds_page_size_) {
    return nvwal_raise_einval_llu(
      "Error: The existing libnvwal installation has mds_page_size %llu, different from"
      " the given configuration.\n",
      wal->prev_config_.mds_page_size_);
  } else if (wal->prev_config_.nv_quota_ != wal->config_.nv_quota_) {
    /*
     * Note : So far this requirement on nv_quota_/segment_size_ is essential.
     * By keeping the same segment_count, we can easily know which existing
     * NVDIMM-resident segment file corresponds to which segment-ID (just mod).
     */
    return nvwal_raise_einval_llu(
      "Error: The existing libnvwal installation has nv_quota_ %llu, different from"
      " the given configuration.\n",
      wal->prev_config_.nv_quota_);
  } else if (wal->prev_config_.segment_size_ != wal->config_.segment_size_) {
    return nvwal_raise_einval_llu(
      "Error: The existing libnvwal installation has segment_size_ %llu, different from"
      " the given configuration.\n",
      wal->prev_config_.segment_size_);
  } else if (wal->prev_config_.writer_buffer_size_ != wal->config_.writer_buffer_size_) {
    return nvwal_raise_einval_llu(
      "Error: The existing libnvwal installation has writer_buffer_size_ %llu, different from"
      " the given configuration.\n",
      wal->prev_config_.writer_buffer_size_);
  } else if (wal->prev_config_.writer_count_ != wal->config_.writer_count_) {
    return nvwal_raise_einval_llu(
      "Error: The existing libnvwal installation has writer_count_ %llu, different from"
      " the given configuration.\n",
      wal->prev_config_.writer_count_);
  }

  /*
   * nv_root, disk_root can change. We should be agnostic to where we are running,
   * so we allow the user to migrate existing NVWAL folders to somewhere else.
   * The only thing we check here is that the segment/MDS files exist there.
   * NV-resident segment files are checked later by init_existing_nvram_segment().
   * MDS files' integrity is checked during mds_init().
   * Here we only check the existence/size of disk-resident segment files.
   */
  const nvwal_dsid_t max_dsid
    = wal->nv_control_block_->fsyncer_progress_.last_synced_dsid_;
  for (nvwal_dsid_t i = 1; i <= max_dsid; ++i) {
    char disk_path[kNvwalMaxPathLength];
    nvwal_construct_disk_segment_path(
      wal,
      i,
      disk_path);

    /* we open it just to check */
    uint64_t filesize = 0;
    {
      int fd = open(disk_path, O_RDONLY, 0);
      if (fd == -1) {
        return nvwal_raise_einval_cstr(
          "Error: A disk-resident segment %s does not exist or is not readable\n",
          disk_path);
      }
      assert(fd);

      struct stat st;
      if (fstat(fd, &st) == 0) {
        filesize = st.st_size;
      }
      close(fd);
    }

    if (filesize != wal->config_.segment_size_) {
      return nvwal_raise_einval_cstr(
        "Error: A disk-resident segment %s has an incompatible filesize\n",
        disk_path);
    }
  }

  return 0;
}


/** subroutine of nvwal_init() used to create fresh new segment, not during restart */
nvwal_error_t init_fresh_nvram_segment(
  struct NvwalContext * wal,
  uint32_t nv_segment_index,
  struct NvwalLogSegment* segment);


/** subroutine of nvwal_init() used to load an existing segment during restart */
nvwal_error_t init_existing_nvram_segment(
  struct NvwalContext * wal,
  uint32_t nv_segment_index,
  struct NvwalLogSegment* segment);

nvwal_error_t impl_init_no_error_handling(
  enum NvwalInitMode mode,
  struct NvwalContext* wal) {
  struct NvwalConfig* config = &(wal->config_);
  NVWAL_CHECK_ERROR(sanity_check_config(config, mode));

  /** CreateTruncate is same as Create except it nukes everything first. */
  if (mode == kNvwalInitCreateTruncate) {
    NVWAL_CHECK_ERROR(nvwal_remove_all_under(config->nv_root_));
    NVWAL_CHECK_ERROR(nvwal_remove_all_under(config->disk_root_));
  }

  NVWAL_CHECK_ERROR(open_control_file(mode, wal));
  NVWAL_CHECK_ERROR(check_adjust_on_prev_config(wal));

  wal->segment_count_ = config->nv_quota_ / config->segment_size_;

  /* Adjust resuming_epoch_ */
  const nvwal_epoch_t prev_de = wal->nv_control_block_->flusher_progress_.durable_epoch_;
  if (wal->config_.resuming_epoch_ == kNvwalInvalidEpoch
    || wal->config_.resuming_epoch_ == prev_de) {
    /* In most cases, the user won't specify resuming_epoch */
    wal->durable_epoch_ = prev_de;
    wal->stable_epoch_ = prev_de;
    wal->next_epoch_ = nvwal_increment_epoch(prev_de);
  } else {
    /** Specified resuming_epoch... so far we only allow truncation. */
    if (nvwal_is_epoch_after(wal->config_.resuming_epoch_, prev_de)) {
      return nvwal_raise_einval_llu(
        "Error: The specified resuming_epoch is after the durable epoch of"
        " the existing libnvwal installaton (%llu). libnvwal so far allows"
        " only truncation at startup, not rolling forward.\n",
        prev_de);
    }
    wal->durable_epoch_ = wal->config_.resuming_epoch_;
    wal->stable_epoch_ = wal->config_.resuming_epoch_;
    wal->next_epoch_ = nvwal_increment_epoch(wal->config_.resuming_epoch_);
    /** TODO invoke MDS to do truncation */
  }

  for (uint32_t i = 0; i < config->writer_count_; ++i) {
    struct NvwalWriterContext* writer = wal->writers_ + i;
    writer->parent_ = wal;
    writer->writer_seq_id_ = i;
    writer->buffer_ = config->writer_buffers_[i];
  }

  /* Initialize Metadata Store */
  NVWAL_CHECK_ERROR(mds_init(mode, wal));

  /* Determine flusher's state as of the previous shutdown using MDS */
  if (wal->durable_epoch_ == kNvwalInvalidEpoch) {
    wal->flusher_current_nv_segment_dsid_ = 1U;
    wal->flusher_current_epoch_head_dsid_ = 1U;
    wal->flusher_current_epoch_head_offset_ = 0;
  } else {
    struct MdsEpochMetadata durable_epoch_meta;
    NVWAL_CHECK_ERROR(mds_read_one_epoch(
      wal,
      wal->durable_epoch_,
      &durable_epoch_meta));
    wal->flusher_current_nv_segment_dsid_ = durable_epoch_meta.to_seg_id_;
    wal->flusher_current_epoch_head_dsid_ = durable_epoch_meta.from_seg_id_;
    wal->flusher_current_epoch_head_offset_ = durable_epoch_meta.from_offset_;
  }

  /* Initialize the reader context */
  /* ret = reader_init(&wal->reader_); */

  /* Initialize all nv segments */
  for (uint32_t i = 0; i < wal->segment_count_; ++i) {
    struct NvwalLogSegment* segment = wal->segments_ + i;
    if (wal->prev_config_.libnvwal_version_) {
      NVWAL_CHECK_ERROR(init_existing_nvram_segment(wal, i, segment));
    } else {
      NVWAL_CHECK_ERROR(init_fresh_nvram_segment(wal, i, segment));
    }
  }

  /*
   * Created files on NVDIMM/Disk, sync everything in filesystem level.
   * This is more efficient than individual, lots of fsync.
   * We thus don't invoke fsync() in each init_fresh_nvram_segment.
   */
  NVWAL_CHECK_ERROR(nvwal_open_and_syncfs(config->nv_root_));
  NVWAL_CHECK_ERROR(nvwal_open_and_syncfs(config->disk_root_));

  /** Now we can start accepting flusher/fsyncher */
  nvwal_impl_thread_state_get_ready(&wal->flusher_thread_state_);
  nvwal_impl_thread_state_get_ready(&wal->fsyncer_thread_state_);

  return 0;
}

nvwal_error_t nvwal_impl_init(
  const struct NvwalConfig* given_config,
  enum NvwalInitMode mode,
  struct NvwalContext* wal) {
  /** otherwise the following memzero will also reset config, ouch */
  struct NvwalConfig* config = &(wal->config_);
  if (config == given_config) {
    return nvwal_raise_einval("Error: misuse, the WAL instance's own config object given\n");
  }

  memset(wal, 0, sizeof(*wal));
  memcpy(config, given_config, sizeof(*config));

  /** Most code were moved to the following function. we just do error handling here */
  nvwal_error_t ret = impl_init_no_error_handling(mode, wal);

  if (ret) {
    nvwal_impl_uninit(wal);

    /** Error code of uninit is ignored. Our own ret is probably more informative */
    errno = ret;
    return ret;
  } else {
    return 0;
  }
}

nvwal_error_t init_fresh_nvram_segment(
  struct NvwalContext* wal,
  uint32_t nv_segment_index,
  struct NvwalLogSegment* segment) {
  segment->dsid_ = nv_segment_index + 1U;  /* As this is the "first round" */
  segment->nv_segment_index_ = nv_segment_index;
  segment->parent_ = wal;
  segment->written_bytes_ = 0;

  char nv_path[kNvwalMaxPathLength];
  nvwal_construct_nv_segment_path(wal, segment->nv_segment_index_, nv_path);
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
  pmem_memset_persist(segment->nv_baseaddr_, 0, wal->config_.segment_size_);

  /** To speed up start up, we don't do fsync here.
  We rather do syncfs at the end.
  if (fsync(segment->nv_fd_)) {
    assert(errno);
    return errno;
  }
  */

  return 0;
}

nvwal_error_t init_existing_nvram_segment(
  struct NvwalContext* wal,
  uint32_t nv_segment_index,
  struct NvwalLogSegment* segment) {
  assert(!segment->fsync_completed_);
  assert(!segment->fsync_error_);
  assert(!segment->fsync_requested_);
  assert(segment->nv_reader_pins_ == 0);
  assert(segment->written_bytes_ == 0);
  segment->nv_segment_index_ = nv_segment_index;
  segment->parent_ = wal;

  char nv_path[kNvwalMaxPathLength];
  nvwal_construct_nv_segment_path(wal, segment->nv_segment_index_, nv_path);
  segment->nv_fd_ = nvwal_open_best_effort_o_direct(nv_path, O_RDWR, 0);

  if (segment->nv_fd_ == -1) {
    /** Failed to open the file! */
    assert(errno);
    return errno;
  }

  assert(segment->nv_fd_);  /** open never returns 0 */

  struct stat st;
  if (fstat(segment->nv_fd_, &st)) {
    assert(errno);
    return errno;
  } else if (st.st_size != wal->config_.segment_size_) {
    return nvwal_raise_einval_cstr(
      "Error: The existing NV-resident segment %s has an incompatible size\n",
      nv_path);
  }

  /* we don't memset in this case. instead, MAP_POPULATE to pre-fault pages */
  segment->nv_baseaddr_ = mmap(0,
                  wal->config_.segment_size_,
                  PROT_READ | PROT_WRITE,
                  MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE,
                  segment->nv_fd_,
                  0);

  if (segment->nv_baseaddr_ == MAP_FAILED) {
    assert(errno);
    return errno;
  }
  assert(segment->nv_baseaddr_);


  /* In restart case, we need to figure out dsid and written_bytes_ */
  const nvwal_dsid_t ondisk_from
    = wal->nv_control_block_->fsyncer_progress_.last_synced_dsid_;
  const uint32_t synced_cycles = (ondisk_from - 1U) / wal->segment_count_;
  nvwal_dsid_t dsid = synced_cycles * wal->segment_count_ + nv_segment_index + 1U;
  if (dsid > ondisk_from) {
    /** This means this segment in this cycle was yet to be synced to disk */
    assert(dsid < ondisk_from + wal->segment_count_);
  } else {
    /** This segment in this cycle was synced! The NV-segment is in next cycle */
    dsid += wal->segment_count_;
  }

  assert(dsid > 0);
  assert(dsid > ondisk_from);
  assert(((dsid - 1U) % wal->segment_count_) == nv_segment_index);
  segment->dsid_ = dsid;
  assert(segment->nv_segment_index_ == 0);
  if (segment->dsid_ > wal->flusher_current_nv_segment_dsid_) {
    /* The segment was not used/recycled yet. Nothing to do then */
  } else if (segment->dsid_ == wal->flusher_current_nv_segment_dsid_) {
    /* The segment was partially filled by flusher */
    struct MdsEpochMetadata durable_epoch_meta;
    NVWAL_CHECK_ERROR(mds_read_one_epoch(
      wal,
      wal->durable_epoch_,
      &durable_epoch_meta));
    assert(wal->flusher_current_nv_segment_dsid_ == durable_epoch_meta.to_seg_id_);
    segment->written_bytes_ = durable_epoch_meta.to_off_;
    assert(segment->written_bytes_ <= wal->config_.segment_size_);
  } else {
    /* The segment was fully filled by flusher and waiting for fsyncer */
    segment->fsync_requested_ = 1U;
    segment->written_bytes_ = wal->config_.segment_size_;
  }

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
  nvwal_impl_thread_state_request_and_wait_for_stop(&wal->flusher_thread_state_);
  nvwal_impl_thread_state_request_and_wait_for_stop(&wal->fsyncer_thread_state_);

  /** uninit continues as much as possible even after an error. */
  nvwal_error_t last_seen_error = 0;

  last_seen_error = nvwal_stock_error_code(
    last_seen_error,
    mds_uninit(wal));

  for (int i = 0; i < wal->segment_count_; ++i) {
    last_seen_error = nvwal_stock_error_code(
      last_seen_error,
      uninit_log_segment(wal->segments_ + i));
  }

  /* Uninitialize reader */
  /* last_seen_error = nvwal_stock_error_code(last_seen_error, reader_uninit(&wal->reader_)); */

  if (wal->nv_control_block_ && wal->nv_control_block_ != MAP_FAILED) {
    munmap(wal->nv_control_block_, sizeof(struct NvwalControlBlock));
    wal->nv_control_block_ = 0;
  }
  if (wal->nv_control_file_fd_ && wal->nv_control_file_fd_ != -1) {
    close(wal->nv_control_file_fd_);
    wal->nv_control_file_fd_ = 0;
  }
  return last_seen_error;
}

nvwal_error_t uninit_log_segment(struct NvwalLogSegment* segment) {
  nvwal_error_t last_seen_error = 0;
  if (segment->nv_baseaddr_ && segment->nv_baseaddr_ != MAP_FAILED) {
    if (munmap(segment->nv_baseaddr_, segment->parent_->config_.segment_size_) == -1) {
      last_seen_error = nvwal_stock_error_code(last_seen_error, errno);
    }
  }
  segment->nv_baseaddr_ = 0;

  if (segment->nv_fd_ && segment->nv_fd_ != -1) {
    if (close(segment->nv_fd_) == -1) {
      last_seen_error = nvwal_stock_error_code(last_seen_error, errno);
    }
  }
  segment->nv_fd_ = 0;

  assert(segment->nv_reader_pins_ == 0);

  memset(segment, 0, sizeof(struct NvwalLogSegment));

  return last_seen_error;
}
