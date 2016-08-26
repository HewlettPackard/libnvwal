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
#include "nvwal_mds.h"

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

  memset(wal, 0, sizeof(*wal));
  memcpy(&wal->config_, config, sizeof(*config));

  /** Check/adjust nv_root/disk_root */
  wal->config_.nv_root_len_ = strnlen(config->nv_root_, kNvwalMaxPathLength);
  wal->config_.disk_root_len_ = strnlen(config->disk_root_, kNvwalMaxPathLength);
  if (wal->config_.nv_root_len_ >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: nv_root must be null terminated\n");
  } else if (wal->config_.nv_root_len_ >= kNvwalMaxFolderPathLength) {
    return nvwal_raise_einval_llu(
      "Error: nv_root must be at most %llu characters\n",
      kNvwalMaxFolderPathLength);
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
      "Error: writer_buffer_size must be a non-zero multiple of page size (512)\n");
  }

  for (i = 0; i < config->writer_count_; ++i) {
    if (!config->writer_buffers_[i]) {
      return nvwal_raise_einval_llu(
        "Error: writer_buffers[ %llu ] is null\n",
        i);
    }
  }

  wal->segment_count_ = config->nv_quota_ / kNvwalSegmentSize;
  if (config->nv_quota_ % kNvwalSegmentSize) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be a multiple of %llu\n",
      kNvwalSegmentSize);
  } else if (wal->segment_count_ < 2U) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at least %llu\n",
      2ULL * kNvwalSegmentSize);
  } else if (wal->segment_count_ > kNvwalMaxActiveSegments) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at most %llu\n",
      (uint64_t) kNvwalMaxActiveSegments * kNvwalSegmentSize);
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
  /* ret = mds_init(&wal->config_, wal); */
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
  struct NvwalContext * wal,
  struct NvwalLogSegment* segment,
  nvwal_dsid_t dsid) {
  nvwal_error_t ret;
  int nv_fd, disk_fd;
  char nv_path[kNvwalMaxPathLength];
  void* nv_baseaddr;

  assert(dsid != kNvwalInvalidDsid);
  assert(wal->config_.nv_root_len_ + 32U < kNvwalMaxPathLength);
  ret = 0;
  nv_fd = 0;
  nv_baseaddr = 0;

  nvwal_concat_sequence_filename(
    wal->config_.nv_root_,
    "nv_segment_",
    dsid,
    nv_path);

  nv_fd = nvwal_open_best_effort_o_direct(
    nv_path,
    O_CREAT | O_RDWR,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);

  if (nv_fd == -1) {
    /** Failed to open/create the file! */
    ret = errno;
    goto error_return;
  }

  assert(nv_fd);  /** open never returns 0 */

  /** posix_fallocate doesn't set errno, do it ourselves */
  ret = posix_fallocate(nv_fd, 0, kNvwalSegmentSize);
  if (ret) {
    goto error_return;
  }

  ftruncate(nv_fd, kNvwalSegmentSize);
  if (errno) {
    /** Failed to set the file length! */
    ret = errno;
    goto error_return;
  }

  fsync(nv_fd);
  if (errno) {
    /** Failed to fsync! */
    ret = errno;
    goto error_return;
  }

  /**
   * Don't bother (non-transparent) huge pages. Even libpmem doesn't try it.
   */
  nv_baseaddr = mmap(0,
                  kNvwalSegmentSize,
                  PROT_READ | PROT_WRITE,
                  MAP_ANONYMOUS | MAP_PRIVATE,
                  nv_fd,
                  0);

  if (nv_baseaddr == MAP_FAILED) {
    ret = errno;
    goto error_return;
  }
  assert(nv_baseaddr);

  /*
   * Even with fallocate we don't trust the metadata to be stable
   * enough for userspace durability. Actually write some bytes!
   */
  memset(nv_baseaddr, 0, kNvwalSegmentSize);
  msync(nv_baseaddr, kNvwalSegmentSize, MS_SYNC);

  segment->dsid_ = dsid;
  segment->nv_fd_ = nv_fd;
  segment->nv_baseaddr_ = nv_baseaddr;
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

  return 0;
}

nvwal_error_t uninit_log_segment(struct NvwalLogSegment* segment) {
  nvwal_error_t ret;  /* last-seen error code */

  ret = 0;

  if (segment->nv_baseaddr_ && segment->nv_baseaddr_ != MAP_FAILED) {
    if (munmap(segment->nv_baseaddr_, kNvwalSegmentSize) == -1) {
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

  if (segment->disk_fd_ && segment->disk_fd_ != -1) {
    if (close(segment->disk_fd_) == -1) {
      ret = nvwal_stock_error_code(ret, errno);
    }
  }
  segment->disk_fd_ = 0;

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
 *  Flusher
 *
 ***************************************************************************/
nvwal_error_t process_one_writer(struct NvwalWriterContext * writer);

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
    }
  }
  nvwal_atomic_store(&(wal->flusher_running_), 0);

  return error_code;
}

nvwal_error_t nvwal_fsync_main(struct NvwalContext* wal) {
  return 0;
}


nvwal_error_t process_one_writer(
  struct NvwalWriterContext * writer) {
  return 0;
}

#ifdef BLUH /** long way to go to get the following code compile. so far disable all of them */
nvwal_error_t process_one_writer(
  struct NvwalWriterContext * writer) {
  struct NvwalContext* wal;
  uint64_t buffer_size;

  wal = writer->parent;
  buffer_size = wal->config.writer_buffer_size;
  nvwal_byte_t* complete = cur_writer->writer->complete;
  nvwal_byte_t* copied = cur_writer->copied;
  nvwal_byte_t* end = cur_writer->writer->buffer + size;
  uint64_t len;

  // If they are not equal, there's work to do since complete
  // cannot be behind copied

  while (writer->cb.complete != writer->copied) {
    // Figure out how much we can copy linearly
    total_length = circular_size(writer->copied, writer->complete, buffer_size);
    nvseg_remaining = kNvwalSegmentSize - wal->nv_offset;
    writer_length = end - copied;

    len = MIN(MIN(total_length,nvseg_remaining),writer_length);

    pmem_memcpy_nodrain(wal->cur_region+nv_offset, copied, len);

    // Record some metadata here?

    // Update pointers
    copied += len;
    assert(copied <= end);
    if (copied == end) {
      // wrap around
      copied = cur_writer->writer->buffer;
    }

    wal->nv_offset += len;

    assert(wal->nv_offset <= kNvwalSegmentSize);
    if (wal->nv_offset == kNvwalSegmentSize) {
      NvwalLogSegment * cur_segment = wal->segment[wal->cur_seg_idx];
      int next_seg_idx = wal->cur_seg_idx + 1 % wal->num_segments;
      NvwalLogSegment * next_segment = wal->segment[next_seg_idx];

      /* Transition current active segment to complete */
      assert(cur_segment->state == SEG_ACTIVE);
      cur_segment->state = SEG_COMPLETE;
      submit_write(cur_segment);

      /* Transition next segment to active */
      if (next_segment->state != SEG_UNUSED) {
          /* Should be at least submitted */
          assert(next_segment->state >= SEG_SUBMITTED);
          if (wal->flags & BG_FSYNC_THREAD) {
              /* spin or sleep waiting for completion */
          } else {
              sync_backing_file(wal, next_segment);
          }
          assert(next_segment->state == SEG_UNUSED);
      }

      assert(cur_segment->state >= SEG_SUBMITTED);

      /* Ok, we're done with the old cur_segment */
      cur_segment = next_segment;

      /* This isn't strictly necessary until we want to start
        * flushing out data, but might as well be done here. The
        * hard work can be done in batches, this function might
        * just pull an already-open descriptor out of a pool. */
      /* This seems to indicate that we will have one disk file
       * per nvram segment
       */ 
      allocate_backing_file(wal, cur_segment);

      wal->cur_seg_idx = next_seg_idx;
      wal->cur_region = cur_segment->nv_baseaddr;
      wal->nv_offset = 0;

      cur_segment->state = SEG_ACTIVE;
    }
  }

  return 0;
}


void * fsync_thread_main(void * arg) {
    struct NvwalContext * wal = arg;

    while(1) {
        int i;
        int found_work = 0;

        for(i = 0; i < wal->num_segments; i++) {
            if (wal->segments[i].state == SEG_SUBMITTED) {
                sync_backing_file(wal, &wal->segments[i]);
            }
        }
    }
}

void init_nvram_segment(
  struct NvwalContext * wal,
  int root_fd,
  int i) {
  struct NvwalLogSegment * seg = &wal->segment[i];
  int fd;
  char filename[256];
  void * baseaddr;

  snprintf(filename, "nvwal-data-%lu", i);
  fd = openat(root_fd, filename, O_CREAT|O_RDWR);
  assert(fd);

  //posix_fallocate doesn't set errno, do it ourselves
  posix_fallocate(fd, 0, kNvwalSegmentSize);
  assert(errno == 0);
  ftruncate(fd, kNvwalSegmentSize);
  assert(errno == 0);
  fsync(fd);

  /* First try for a hugetlb mapping */
  baseaddr = mmap(0,
                  kNvwalSegmentSize,
                  PROT_READ|PROT_WRITE,
                  MAP_SHARED|MAP_PREALLOC|MAP_HUGETLB
                  fd,
                  0);

  if (baseaddr == MAP_FAILED && errno == EINVAL) {
    /* If that didn't work, try for a non-hugetlb mapping */
    printf(stderr, "Failed hugetlb mapping\n");
    baseaddr = mmap(0,
                    kNvwalSegmentSize,
                    PROT_READ|PROT_WRITE,
                    MAP_SHARED|MAP_PREALLOC,
                    fd,
                    0);
  }
  /* If that didn't work then bail. */
  assert(baseaddr != MAP_FAILED);

  /* Even with fallocate we don't trust the metadata to be stable
    * enough for userspace durability. Actually write some bytes! */

  memset(baseaddr, 0x42, kNvwalSegmentSize);
  msync(baseaddr, kNvwalSegmentSize, MS_SYNC);

  seg->seq = INVALID_SEQNUM;
  seg->nvram_fd = fd;
  seg->disk_fd = -1;
  seg->nv_baseaddr = baseaddr;
  seg->state = SEG_UNUSED;
  seg->dir_synced = 0;

  /* Is this necessary? I'm assuming this is offset into the disk-backed file.
   * If we have one disk file per nvram segment, we don't need this.
   * When we submit_write a segment, the FS should be tracking the file offset
   * for us anyway. What is this actually used for?
   */
  seg->disk_offset = 0;
}

void submit_write(
  struct NvwalContext* wal,
  struct NvwalLogSegment * seg) {
  int bytes_written;

  assert(seg->state == SEG_COMPLETE);
  /* kick off write of old segment
    * This should be wrapped up in some function,
    * begin_segment_write or so. */

  bytes_written = write(seg->disk_fd,
                        seg->nv_baseaddr,
                        kNvwalSegmentSize);

  ASSERT_NO_ERROR(bytes_written != kNvwalSegmentSize);

  seg->state = SEG_SUBMITTED;
}


void sync_backing_file(
  struct NvwalContext * wal,
  struct NvwalLogSegment* seg) {

  assert(seg->state >= SEG_SUBMITTED);

  /* kick off the fsync ourselves */
  seg->state = SEG_SYNCING;
  fsync(seg->disk_fd);

  /* check file's dirfsync status, should be
    * guaranteed in this model */
  assert(seg->dir_synced);
  seg->state = SEG_SYNCED;

  /* Update durable epoch marker? */

  //wal->durable = seg->latest - 2;

  /* Notify anyone waiting on durable epoch? */

  /* Clean up. */
  close(seg->disk_fd);
  seg->disk_fd = -1;
  seg->state = SEG_UNUSED;
  seg->seq = 0;
  seg->disk_offset = 0; /* if we are bothering with this */
  seg->dir_synced = false;
}

void allocate_backing_file(
  struct NvwalContext * wal,
  struct NvwalLogSegment * seg) {
  uint64_t our_sequence = wal->log_sequence++;
  int our_fd = -1;
  int i = 0;
  char filename[256];

  if (our_sequence % PREALLOC_FILE_COUNT == 0) {
    for (i = our_sequence; i < our_sequence + PREALLOC_FILE_COUNT; i++) {
      int fd;

      snprintf(filename, "log-segment-%lu", i);
      fd = openat(wal->log_root_fd,
                  filename,
                  O_CREAT|O_RDWR|O_TRUNC|O_APPEND,
                  S_IRUSR|S_IWUSR);

      ASSERT_FD_VALID(fd);
      /* Now would be a good time to hint to the FS using
        * fallocate or whatever. */

      /* Clean up */

      /* We should really just stash these fds in a pool */
      close(fd);
    }

    /* Sync the directory, so that all these newly created (empty)
      * files are visible.
      * We may want to take care of this in the fsync thread instead
      * and set the dir_synced flag on the segment descriptor */
    fsync(wal->log_root_fd);
    seg->dir_synced = true;
  }

  /* The file should already exist */
  snprintf(filename, "log-segment-%lu", our_sequence);
  our_fd = openat(wal->log_root_fd,
                  filename,
                  O_RDWR|O_TRUNC|O_APPEND);

  ASSERT_FD_VALID(our_fd);

  seg->disk_fd = our_fd;

}

#endif  /* BLUH */

/**************************************************************************
 *
 *  Reader
 *
 ***************************************************************************/
#if 0
nvwal_error_t nvwal_reader_init(
  struct NvwalReaderContext* reader) {

  memset(reader, 0, sizeof(*reader));

  reader->prev_epoch_ = 0;
  reader->tail_epoch_ = 0;
  reader->fetch_complete_ = true;
  reader->seg_id_ = 0;
}

nvwal_error_t nvwal_reader_uninit(
  struct NvwalReaderContext* reader) {

  memset(reader, 0, sizeof(*reader));

}

nvwal_error_t get_epoch(
  NvwalReaderContext* reader,
  nvwal_epoch_t epoch,
  char ** buf,
  size_t * len) {

  char* mmap_addr = 0;
  bool first_mmap = true;
  *len = 0;

  /* Lookup the epoch info from the MDS */
  struct EpochMetadata epoch_meta;

  /* Is this a retry call because we didn't finish mmapping everything 
   * for the requested epoch? */
  if (reader->fetch_complete_)
  {
    /* Initialize our segment progress for this epoch */
    reader->seg_id_ = epoch_meta.from_seg_id;
  }
  /* else we already have the last segment we tried to mmap */

  do 
  {

    size_t offset = 0;
    if (reader->seg_id_ == epoch_meta.from_seg_id)
    {
      /* This is the first segment */
      if (epoch_meta.from_seg_id_ == epoch_meta.to_seg_id_)
      {
        /* This is also the only segment. */
        map_len = epoch_meta.to_off_ - epoch_meta.from_off_;
      } else
      {
        /* There are more segments to follow. Mmap to the end of the segment. */
        map_len = kNvwalSegmentSize - reader->offset_;
      }

      offset = epoch_meta.from_off_;
    } else if (reader->seg_id_ < epoch_meta.to_seg_id)
    {
      /* This is a middle segment; we're going to map the entire segment. */
      map_len = kNvwalSegmentSize;
      offset = 0;
    } else
    {
      /* This is the final segment */
      map_len = epoch_meta.to_off_;
      offset = 0;
    }

    /* Lookup or infer the filename for this segment */
    fd = open();
    if (-1 == fd)
    {

    }

    if (first_mmap)
    {
      /* This is the first mmap attempt for this get_epoch call.
       * Let the kernel pick where to start and save the beginning of the mmap. */
      *buf = mmap(mmap_addr, map_len, PROT_READ, MAP_SHARED, fd, offset);
      first_mmap = false;
      if (MAP_FAILED == *buf)
      {
        /* Pretty bad to fail on the first attempt while letting the kernel pick */
        reader->fetch_complete_ = false;
        return /*something*/;
      }
    } else
    {
      char* fixed_map = mmap(mmap_addr, map_len, PROT_READ, MAP_SHARED|MAP_FIXED, fd, 0);
      if (MAP_FAILED == fixed_map)
      {
        reader->fetch_complete_ = false;
        return /* retry */;
      }
    }
    *len += map_len;

    mmap_addr += map_len;

    reader->seg_id_++;
    reader->offset_ += len;
    if (kNvwalSegmentSize == reader->offset)
    {
      reader->offset = 0;
    }
  
  } while (reader->seg_id_ <= epoch_meta.to_seg_id_)

  reader->fetch_complete = true;
  reader->prev_epoch_ = epoch;

  return /* no error */;
}
#endif
