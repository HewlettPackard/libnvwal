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
#include "nvwal_api.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "nvwal_atomics.h"
#include "nvwal_mds.h"
#include "nvwal_util.h"
#include "nvwal_mds_types.h"

/**************************************************************************
 *
 *  Initializations
 *
 ***************************************************************************/

/** subroutine of nvwal_init() used to create fresh new segment, not during restart */
nvwal_error_t init_fresh_nvram_segment(
  struct NvwalContext * wal,
  struct NvwalLogSegment* segment,
  nvwal_dsid_t dsid);
/** subroutine of nvwal_uninit() or failed nvwal_init() */
nvwal_error_t uninit_log_segment(struct NvwalLogSegment* segment);

void remove_trailing_slash(char* path, uint16_t* len) {
  while ((*len) > 0 && path[(*len) - 1] == '/') {
    path[(*len) - 1] = '\0';
    --(*len);
  }
}

nvwal_error_t nvwal_init(
  const struct NvwalConfig* config,
  struct NvwalContext* wal) {
  uint32_t i;
  nvwal_error_t ret;
  struct NvwalWriterContext* writer;
  nvwal_dsid_t resuming_dsid;

  /** otherwise the following memzero will also reset config, ouch */
  if (config == &(wal->config_)) {
    return nvwal_raise_einval("Error: misuse, the WAL instance's own config object given\n");
  }

  memset(wal, 0, sizeof(*wal));
  memcpy(&wal->config_, config, sizeof(*config));

  /** Check/adjust nv_root/disk_root */
  wal->config_.nv_root_len_ = strnlen(config->nv_root_, kNvwalMaxPathLength);
  wal->config_.disk_root_len_ = strnlen(config->disk_root_, kNvwalMaxPathLength);
  if (wal->config_.nv_root_len_ <= 1U) {
    return nvwal_raise_einval("Error: nv_root must be a valid full path\n");
  } else if (wal->config_.nv_root_len_ >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: nv_root must be null terminated\n");
  } else if (wal->config_.nv_root_len_ >= kNvwalMaxFolderPathLength) {
    return nvwal_raise_einval_llu(
      "Error: nv_root must be at most %llu characters\n",
      kNvwalMaxFolderPathLength);
  } else if (wal->config_.disk_root_len_ <= 1U) {
    return nvwal_raise_einval("Error: disk_root must be a valid full path\n");
  } else if (wal->config_.disk_root_len_ >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: disk_root_ must be null terminated\n");
  } else if (wal->config_.disk_root_len_ >= kNvwalMaxFolderPathLength) {
    return nvwal_raise_einval_llu(
      "Error: disk_root must be at most %llu characters\n",
      kNvwalMaxFolderPathLength);
  }
  remove_trailing_slash(wal->config_.nv_root_, &wal->config_.nv_root_len_);
  remove_trailing_slash(wal->config_.disk_root_, &wal->config_.disk_root_len_);

  /** Check writer_count, writer_buffer_size, writer_buffers */
  if (config->writer_count_ == 0 || config->writer_count_ > kNvwalMaxWorkers) {
    return nvwal_raise_einval_llu(
      "Error: writer_count must be 1 to %llu\n",
      kNvwalMaxWorkers);
  } else if ((config->writer_buffer_size_ % 512U) || config->writer_buffer_size_ == 0) {
    return nvwal_raise_einval(
      "Error: writer_buffer_size must be a non-zero multiply of page size (512)\n");
  }

  for (i = 0; i < config->writer_count_; ++i) {
    if (!config->writer_buffers_[i]) {
      return nvwal_raise_einval_llu(
        "Error: writer_buffers[ %llu ] is null\n",
        i);
    }
  }

  if (wal->config_.segment_size_ % 512 != 0) {
    return nvwal_raise_einval(
      "Error: segment_size_ must be a multiply of 512\n");
  }
  if (wal->config_.segment_size_ == 0) {
    wal->config_.segment_size_ = kNvwalDefaultSegmentSize;
  }
  wal->segment_count_ = wal->config_.nv_quota_ / wal->config_.segment_size_;
  if (wal->config_.nv_quota_ % wal->config_.segment_size_ != 0) {
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

  wal->durable_ = config->resuming_epoch_;
  wal->latest_ = config->resuming_epoch_;

  /** TODO we should retrieve from MDS in restart case */
  for (i = 0; i < config->writer_count_; ++i) {
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
  ret = 0;

  /* Initialize Metadata Store */
  /* riet = mds_init(&wal->config_, wal); */

  /* Initialize the reader context */
  /* ret = reader_init(&wal->reader_); */
  if (ret) {
    goto error_return;
  }

  /* Initialize all nv segments */
  resuming_dsid = 0 + 1;  /** TODO we should retrieve from MDS in restart case */
  for (i = 0; i < wal->segment_count_; ++i) {
    memset(wal->segments_, 0, sizeof(struct NvwalLogSegment));
    ret = init_fresh_nvram_segment(wal, wal->segments_ + i, resuming_dsid + i);
    if (ret) {
      goto error_return;
    }
  }

  /* Created files on NVDIMM/Disk, fsync parent directory */
  ret = nvwal_open_and_fsync(wal->config_.nv_root_);
  if (ret) {
    goto error_return;
  }
  ret = nvwal_open_and_fsync(wal->config_.disk_root_);
  if (ret) {
    goto error_return;
  }
  return 0;

error_return:
  assert(ret);
  nvwal_uninit(wal);
  /** Error code of uninit is ignored. Our own ret is probably more informative */
  errno = ret;
  return ret;
}

nvwal_error_t init_fresh_nvram_segment(
  struct NvwalContext* wal,
  struct NvwalLogSegment* segment,
  nvwal_dsid_t dsid) {
  nvwal_error_t ret;
  char nv_path[kNvwalMaxPathLength];

  assert(dsid != kNvwalInvalidDsid);
  assert(wal->config_.nv_root_len_ + 32U < kNvwalMaxPathLength);
  ret = 0;
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
    ret = errno;
    goto error_return;
  }

  assert(segment->nv_fd_);  /** open never returns 0 */

  if (posix_fallocate(segment->nv_fd_, 0, wal->config_.segment_size_)) {
    /** posix_fallocate doesn't set errno, do it ourselves */
    ret = EINVAL;
    goto error_return;
  }

  if (ftruncate(segment->nv_fd_, wal->config_.segment_size_)) {
    /** Failed to set the file length! */
    ret = errno;
    goto error_return;
  }

  if (fsync(segment->nv_fd_)) {
    /** Failed to fsync! */
    ret = errno;
    goto error_return;
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
    ret = errno;
    goto error_return;
  }
  assert(segment->nv_baseaddr_);

  /*
   * Even with fallocate we don't trust the metadata to be stable
   * enough for userspace durability. Actually write some bytes!
   */
  memset(segment->nv_baseaddr_, 0, wal->config_.segment_size_);
  msync(segment->nv_baseaddr_, wal->config_.segment_size_, MS_SYNC);
  return 0;

error_return:
  uninit_log_segment(segment);
  errno = ret;
  return ret;
}


/**************************************************************************
 *
 *  Un-Initializations
 *
 ***************************************************************************/

nvwal_error_t nvwal_uninit(
  struct NvwalContext* wal) {
  nvwal_error_t ret;  /* last-seen error code */
  int i;

  ret = 0;

  /** Stop flusher */
  if (nvwal_atomic_load(&(wal->flusher_running_)) != 0) {
    nvwal_atomic_store(&(wal->flusher_stop_requested_), 1U);
    while (nvwal_atomic_load(&(wal->flusher_running_)) != 0) {
      /** TODO Should have a sleep or at least yield */
      /* NOTE: Do we need to kick the flusher one more time or should we assume
       * assume the application's writer threads are responsible enough to
       * call nvwal_on_wal_write one last time before the uninit happens?
       */
    }
  }

  /** Stop fsyncer */
  if (nvwal_atomic_load(&(wal->fsyncer_running_)) != 0) {
    nvwal_atomic_store(&(wal->fsyncer_stop_requested_), 1U);
    while (nvwal_atomic_load(&(wal->fsyncer_running_)) != 0) {
    }
  }

  for (i = 0; i < wal->segment_count_; ++i) {
    ret = nvwal_stock_error_code(ret, uninit_log_segment(wal->segments_ + i));
  }

  /* Uninitialize reader */
  /* ret = reader_uninit(&wal->reader_); */
  return ret;
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


/**************************************************************************
 *
 *  Writers
 *
 ***************************************************************************/

void assert_writer_current_frames(
  const struct NvwalWriterContext* writer) {
  assert(writer->oldest_frame_ < kNvwalEpochFrameCount);
  assert(writer->epoch_frames_[writer->oldest_frame_].log_epoch_);
  assert(writer->active_frame_ < kNvwalEpochFrameCount);
  assert(writer->epoch_frames_[writer->active_frame_].log_epoch_);
}

uint32_t wrap_writer_epoch_frame(
  uint32_t current_epoch_frame) {
  assert(current_epoch_frame < kNvwalEpochFrameCount * 2U);
  if (current_epoch_frame < kNvwalEpochFrameCount) {
    return current_epoch_frame;
  } else {
    return current_epoch_frame - kNvwalEpochFrameCount;
  }
}

uint64_t wrap_writer_offset(
  const struct NvwalWriterContext* writer,
  uint64_t offset) {
  uint64_t buffer_size;

  assert_writer_current_frames(writer);
  buffer_size = writer->parent_->config_.writer_buffer_size_;
  assert(offset < buffer_size * 2U);
  if (offset < buffer_size) {
    return offset;
  } else {
    return offset - buffer_size;
  }
}

uint64_t calculate_writer_offset_distance(
  const struct NvwalWriterContext* writer,
  uint64_t left_offset,
  uint64_t right_offset) {
  uint64_t buffer_size;

  assert_writer_current_frames(writer);
  buffer_size = writer->parent_->config_.writer_buffer_size_;
  if (left_offset < right_offset) {
    return right_offset - left_offset;
  } else {
    return right_offset + buffer_size - left_offset;
  }
}

/**
 * Make sure writer->active_frame corresponds to the given epoch.
 */
void assure_writer_active_frame(
  struct NvwalWriterContext* writer,
  nvwal_epoch_t log_epoch) {
  struct NvwalWriterEpochFrame* frame;

  assert_writer_current_frames(writer);
  frame = writer->epoch_frames_ + writer->active_frame_;
  assert(nvwal_is_epoch_equal_or_after(log_epoch, frame->log_epoch_));
  if (frame->log_epoch_ == log_epoch) {
    /** The epoch exists. Most likely this case. */
  } else {
    /**
     * Otherwise it wasn't purely increasing.
     */
    assert(frame->log_epoch_ == kNvwalInvalidEpoch);
    /**
     * We newly populate this frame.
     * Release offsets before publisizing the frame (==store to epoch).
     */
    writer->active_frame_ = wrap_writer_epoch_frame(writer->active_frame_ + 1U);
    /**
     * Now active_frame is surely ahead of oldest_frame.
     * If the assert below fires, this writer was issueing too new epochs,
     * violating the "upto + 2" contract.
     */
    assert(writer->active_frame_ != writer->oldest_frame_);
    frame = writer->epoch_frames_ + writer->active_frame_;
    nvwal_atomic_store_release(&frame->head_offset_, writer->last_tail_offset_);
    nvwal_atomic_store_release(&frame->tail_offset_, writer->last_tail_offset_);
    nvwal_atomic_store_release(&frame->log_epoch_, log_epoch);
  }
}

nvwal_error_t nvwal_on_wal_write(
  struct NvwalWriterContext* writer,
  uint64_t bytes_written,
  nvwal_epoch_t log_epoch) {
  struct NvwalWriterEpochFrame* frame;

  assure_writer_active_frame(writer, log_epoch);
  frame = writer->epoch_frames_ + writer->active_frame_;
  assert(frame->log_epoch_ == log_epoch);
  assert(frame->tail_offset_ == writer->last_tail_offset_);

  /**
   * We should have enough space, right?
   * Otherwise the client didn't call nvwal_assure_writer_space().
   */
  assert(
    calculate_writer_offset_distance(
      writer,
      frame->tail_offset_,
      frame->head_offset_)
    + bytes_written
      < writer->parent_->config_.writer_buffer_size_);

  writer->last_tail_offset_ = wrap_writer_offset(
    writer,
    frame->tail_offset_ + bytes_written);
  nvwal_atomic_store_release(&frame->tail_offset_, writer->last_tail_offset_);

  return 0;
}

uint8_t nvwal_has_enough_writer_space(
  struct NvwalWriterContext* writer) {
  uint32_t oldest_frame;
  uint64_t consumed_bytes;
  struct NvwalWriterEpochFrame* frame;

  oldest_frame = nvwal_atomic_load_acquire(&writer->oldest_frame_);
  frame = writer->epoch_frames_ + oldest_frame;
  consumed_bytes = calculate_writer_offset_distance(
    writer,
    frame->head_offset_,
    writer->last_tail_offset_);
  return (consumed_bytes * 2ULL <= writer->parent_->config_.writer_buffer_size_);
}

/**************************************************************************
 *
 *  Flusher/Fsyncer
 *
 ***************************************************************************/
nvwal_error_t process_one_writer(struct NvwalWriterContext* writer);
nvwal_error_t sync_one_segment_to_disk(struct NvwalLogSegment* segment);

nvwal_error_t nvwal_flusher_main(
  struct NvwalContext* wal) {
  uint32_t cur_writer_id;
  nvwal_error_t error_code;

  error_code = 0;
  /** whatever error checks here */

  nvwal_atomic_store(&(wal->flusher_running_), 1);
  while (1) {
    /** doesn't have to be seq_cst, and this code runs very frequently */
    if (nvwal_atomic_load_acquire(&(wal->flusher_stop_requested_)) != 0) {
      break;
    }

    /* Look for work */
    for (cur_writer_id = 0; cur_writer_id < wal->config_.writer_count_; ++cur_writer_id) {
      error_code = process_one_writer(wal->writers_ + cur_writer_id);
      if (error_code) {
        break;
      }
      /* Ensure writes are durable in NVM */
      /* pmem_drain(); */

      /* Some kind of metadata commit, we could use libpmemlog.
        * This needs to track which epochs are durable, what's on disk
        * etc. */
      //commit_metadata_updates(wal)

      /** Promptly react when obvious. but no need to be atomic read. */
      if (wal->flusher_stop_requested_) {
        break;
      }
    }
  }
  nvwal_atomic_store(&(wal->flusher_running_), 0);

  return error_code;
}

nvwal_error_t nvwal_fsync_main(struct NvwalContext* wal) {
  uint32_t cur_segment;
  nvwal_error_t error_code;
  struct NvwalLogSegment* segment;

  error_code = 0;
  /** whatever error checks here */

  nvwal_atomic_store(&(wal->fsyncer_running_), 1);
  while (1) {
    /** doesn't have to be seq_cst, and this code runs very frequently */
    if (nvwal_atomic_load_acquire(&(wal->fsyncer_stop_requested_)) != 0) {
      break;
    }

    for (cur_segment = 0; cur_segment < wal->segment_count_; ++cur_segment) {
      segment = wal->segments_ + cur_segment;
      if (nvwal_atomic_load_acquire(&(segment->fsync_requested_))) {
        error_code = sync_one_segment_to_disk(wal->segments_ + cur_segment);
        if (error_code) {
          break;
        }
      }

      /** Promptly react when obvious. but no need to be atomic read. */
      if (wal->fsyncer_stop_requested_) {
        break;
      }
    }
  }
  nvwal_atomic_store(&(wal->fsyncer_running_), 0);

  return error_code;
}

nvwal_error_t sync_one_segment_to_disk(struct NvwalLogSegment* segment) {
  nvwal_error_t ret;
  int disk_fd;
  char disk_path[kNvwalMaxPathLength];
  uint64_t total_writen, written;

  assert(segment->dsid_);
  assert(!segment->fsync_completed_);
  ret = 0;
  disk_fd = 0;
  total_writen = 0;
  written = 0;
  segment->fsync_error_ = 0;
  nvwal_concat_sequence_filename(
    segment->parent_->config_.disk_root_,
    "nvwal_ds",
    segment->dsid_,
    disk_path);

  disk_fd = nvwal_open_best_effort_o_direct(
    disk_path,
    O_CREAT | O_RDWR,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
  assert(disk_fd);
  if (disk_fd == -1) {
    /** Probably permission issue? */
    ret = errno;
    goto error_return;
  }

  total_writen = 0;
  /** Be aware of the case where write() doesn't finish in one call */
  while (total_writen < segment->parent_->config_.segment_size_) {
    written = write(
      disk_fd,
      segment->nv_baseaddr_ + total_writen,
      segment->parent_->config_.segment_size_ - total_writen);
    if (written == -1) {
      /** Probably full disk? */
      ret = errno;
      goto error_return;
    }
    total_writen += written;

    /** Is this fsyncher cancelled for some reason? */
    if (segment->parent_->fsyncer_stop_requested_) {
      ret = ETIMEDOUT;  /* Not sure this is appropriate.. */
      goto error_return;
    }
  }

  fsync(disk_fd);
  close(disk_fd);
  nvwal_open_and_fsync(segment->parent_->config_.disk_root_);

  nvwal_atomic_store(&(segment->fsync_completed_), 1U);
  return 0;

error_return:
  if (disk_fd && disk_fd != -1) {
    close(disk_fd);
  }
  errno = ret;
  segment->fsync_error_ = ret;
  return ret;
}


nvwal_error_t process_one_writer(
  struct NvwalWriterContext * writer) {
  return 0;
}

/**************************************************************************
 *
 *  Reader
 *
 ***************************************************************************/

nvwal_error_t nvwal_reader_init(
  struct NvwalReaderContext* reader) {

  memset(reader, 0, sizeof(*reader));

  reader->prev_epoch_ = 0;
  reader->tail_epoch_ = 0;
  reader->fetch_complete_ = 1;
  reader->seg_id_ = 0;
  reader->mmap_start_ = NULL;
  reader->mmap_len_ = 0;
  return 0;
}

nvwal_error_t nvwal_reader_uninit(
  struct NvwalReaderContext* reader) {

  memset(reader, 0, sizeof(*reader));

  return 0;
}

nvwal_error_t get_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t epoch,
  char ** buf,
  uint64_t * len) {

  struct NvwalReaderContext *reader = &wal->reader_;
  nvwal_error_t error_code = 0;
  char* mmap_addr = 0;
  uint8_t first_mmap = 1;
  *len = 0;

  /* Lookup the epoch info from the MDS */
  struct MdsEpochMetadata epoch_meta;
  //mds_read_epoch(wal->mds, epoch, &epoch_meta); //need to catch a return code

  /* Is this a retry call because we didn't finish mmapping everything 
   * for the requested epoch? */
  if (reader->fetch_complete_)
  {
    /* Initialize our segment progress for this epoch */
    reader->seg_id_ = epoch_meta.from_seg_id_;
  } else
  {
    /* We already have the last segment we tried to mmap.
     * We need to clean up the previous mapping before 
     * mapping more of this epoch.
     * */
    if (NULL != reader->mmap_start_)
    {
      munmap(reader->mmap_start_, reader->mmap_len_);
      reader->mmap_start_ = NULL;
      reader->mmap_len_ = 0;
    } /* else, the client must have called done_with_epoch before 
       * calling here again */
  }
  do 
  {

    uint64_t offset = 0;
    uint64_t map_len;
    if (reader->seg_id_ == epoch_meta.from_seg_id_)
    {
      /* This is the first segment */
      if (epoch_meta.from_seg_id_ == epoch_meta.to_seg_id_)
      {
        /* This is also the only segment. */
        map_len = epoch_meta.to_off_ - epoch_meta.from_offset_;
      } else
      {
        /* There are more segments to follow. Mmap to the end of the segment. */
        map_len = wal->config_.segment_size_ - epoch_meta.from_offset_;
      }

      offset = epoch_meta.from_offset_;
    } else if (reader->seg_id_ < epoch_meta.to_seg_id_)
    {
      /* This is a middle segment; we're going to map the entire segment. */
      map_len = kNvwalDefaultSegmentSize;
      offset = 0;
    } else
    {
      /* This is the final segment */
      map_len = epoch_meta.to_off_;
      offset = 0;
    }

    /* Lookup or infer the filename for this segment */


    /* Is it on NVDIMM or disk? */
#if 0
    char backing_path[kNvwalMaxPathLength];
    if (0)
    {   
      nvwal_concat_sequence_filename(
      wal->config_.nv_root_, 
      "nv_segment_",
      reader->seg_id_,
      backing_path);
    } else
    {
      nvwal_concat_sequence_filename(
        wal_->config_.disk_root_,
        "nvwal_ds",
        reader->seg_id_,
        backing_path);
    }
#endif
    int fd = -1; /*= open();*/
    if (-1 == fd)
    {

    }

    if (first_mmap)
    {
      /* This is the first mmap attempt for this get_epoch call.
       * Let the kernel pick where to start and save the beginning of the mmap. */
      *buf = mmap(mmap_addr, map_len, PROT_READ, MAP_SHARED, fd, offset);
      first_mmap = 0;
      if (MAP_FAILED == *buf)
      {
        /* Pretty bad to fail on the first attempt while letting the kernel pick */
        reader->fetch_complete_ = 0;
        return error_code; /*something*/
      }
    } else
    {
      char* fixed_map = mmap(mmap_addr, map_len, PROT_READ, MAP_SHARED|MAP_FIXED, fd, 0);
      if (MAP_FAILED == fixed_map)
      {
        reader->fetch_complete_ = 0;
        return error_code; /* retry */
      }
    }

    close(fd);

    /* Atomically mark the segment as in use, if it's in NVDIMM */

    *len += map_len;

    mmap_addr += map_len;

    reader->seg_id_++;
  
  } while (reader->seg_id_ <= epoch_meta.to_seg_id_);

  
  reader->mmap_start_ = *buf;
  reader->mmap_len_ = *len;
  reader->fetch_complete_ = 1;
  reader->prev_epoch_ = epoch;

  return error_code; /* no error */
}

nvwal_error_t consumed_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t const epoch)
{
  nvwal_error_t error_code = 0;
  struct NvwalReaderContext *reader = &wal->reader_;
  munmap(reader->mmap_start_, reader->mmap_len_);

  struct MdsEpochMetadata epoch_meta;
  //mds_read_epoch(wal->mds, epoch, &epoch_meta); //need to catch a return code

  nvwal_dsid_t segment_id = epoch_meta.from_seg_id_;
  do 
  {

    /* Is it on NVDIMM or disk? */

    /* Atomically mark the segment as free, if it's in NVDIMM */

    segment_id++;
  
  } while (segment_id <= epoch_meta.to_seg_id_);

  
  reader->mmap_start_ = NULL;
  reader->mmap_len_ = 0;

  return error_code; /* no error */
}

