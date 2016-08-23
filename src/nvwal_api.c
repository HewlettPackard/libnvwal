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
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "nvwal_api.h"
#include "nvwal_atomics.h"
#include "nvwal_util.h"

void init_nvram_segment(
  struct nvwal_context * wal,
  int root_fd,
  int i);

/**************************************************************************
 *
 *  Initializations
 *
 ***************************************************************************/
nvwal_error_t nvwal_init(
  const struct nvwal_config* config,
  struct nvwal_context* wal) {
  int nv_root_fd;
  int block_root_fd;
  uint32_t i;

  memset(wal, 0, sizeof(*wal));
  memcpy(&wal->config, config, sizeof(*config));

  if (strnlen(config->nv_root, kNvwalMaxPathLength) >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: nv_root must be null terminated\n");
  } else if (strnlen(config->block_root, kNvwalMaxPathLength) >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: block_root must be null terminated\n");
  } else if (config->writer_count == 0 || config->writer_count > kNvwalMaxWorkers) {
    return nvwal_raise_einval_llu(
      "Error: writer_count must be 1 to %llu\n",
      kNvwalMaxWorkers);
  } else if ((config->writer_buffer_size % 512U) || config->writer_buffer_size == 0) {
    return nvwal_raise_einval(
      "Error: writer_buffer_size must be a non-zero multiple of page size (512)\n");
  }

  for (i = 0; i < config->writer_count; ++i) {
    if (!config->writer_buffers[i]) {
      return nvwal_raise_einval_llu(
        "Error: writer_buffers[ %llu ] is null\n",
        i);
    }
  }

  wal->num_active_segments = config->nv_quota / kNvwalSegmentSize;
  if (config->nv_quota % kNvwalSegmentSize) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be a multiple of %llu\n",
      kNvwalSegmentSize);
  } else if (wal->num_active_segments < 2U) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at least %llu\n",
      2ULL * kNvwalSegmentSize);
  } else if (wal->num_active_segments > kNvwalMaxActiveSegments) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at most %llu\n",
      (uint64_t) kNvwalMaxActiveSegments * kNvwalSegmentSize);
  }

  wal->durable = config->resuming_epoch;
  wal->latest = config->resuming_epoch;

  for (i = 0; i < config->writer_count; ++i) {
    wal->writers[i].parent = wal;
    wal->writers[i].oldest_frame = 0;
    wal->writers[i].active_frame = 0;
    wal->writers[i].writer_seq_id = i;
    wal->writers[i].last_tail_offset = 0;
    wal->writers[i].copied_offset = 0;
    wal->writers[i].buffer = config->writer_buffers[i];
    memset(wal->writers[i].epoch_frames, 0, sizeof(wal->writers[i].epoch_frames));
    wal->writers[i].epoch_frames[0].log_epoch = config->resuming_epoch;
  }

  nv_root_fd = open(config->nv_root, 0);

  /* Initialize all nv segments */

  for (i = 0; i < wal->num_active_segments; ++i) {
    init_nvram_segment(wal, nv_root_fd, i);
  }

  /* Created the files, fsync parent directory */
  fsync(nv_root_fd);

  close(nv_root_fd);

  block_root_fd = open(config->block_root, 0);

  wal->log_root_fd = block_root_fd;

  return 0;
}

/**************************************************************************
 *
 *  Un-Initializations
 *
 ***************************************************************************/

nvwal_error_t nvwal_uninit(
  struct nvwal_context* wal) {

  if (nvwal_atomic_load(&(wal->flusher_running)) != 0) {
    nvwal_atomic_store(&(wal->flusher_stop_requested), 1U);
    while (nvwal_atomic_load(&(wal->flusher_running)) != 0) {
      /** TODO Should have a sleep or at least yield */
      /* NOTE: Do we need to kick the flusher one more time or should we assume
       * assume the application's writer threads are responsible enough to 
       * call nvwal_on_wal_write one last time before the uninit happens?
       */
    }
  }

  return 0;
}

/**************************************************************************
 *
 *  Writers
 *
 ***************************************************************************/

void assert_writer_current_frames(
  const struct nvwal_writer_context* writer) {
  assert(writer->oldest_frame < kNvwalEpochFrameCount);
  assert(writer->epoch_frames[writer->oldest_frame].log_epoch);
  assert(writer->active_frame < kNvwalEpochFrameCount);
  assert(writer->epoch_frames[writer->active_frame].log_epoch);
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
  const struct nvwal_writer_context* writer,
  uint64_t offset) {
  uint64_t buffer_size;

  assert_writer_current_frames(writer);
  buffer_size = writer->parent->config.writer_buffer_size;
  assert(offset < buffer_size * 2U);
  if (offset < buffer_size) {
    return offset;
  } else {
    return offset - buffer_size;
  }
}

uint64_t calculate_writer_offset_distance(
  const struct nvwal_writer_context* writer,
  uint64_t left_offset,
  uint64_t right_offset) {
  uint64_t buffer_size;

  assert_writer_current_frames(writer);
  buffer_size = writer->parent->config.writer_buffer_size;
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
  struct nvwal_writer_context* writer,
  nvwal_epoch_t log_epoch) {
  struct nvwal_writer_epoch_frame* frame;

  assert_writer_current_frames(writer);
  frame = writer->epoch_frames + writer->active_frame;
  assert(nvwal_is_epoch_equal_or_after(log_epoch, frame->log_epoch));
  if (frame->log_epoch == log_epoch) {
    /** The epoch exists. Most likely this case. */
  } else {
    /**
     * Otherwise it wasn't purely increasing.
     */
    assert(frame->log_epoch == kNvwalInvalidEpoch);
    /**
     * We newly populate this frame.
     * Release offsets before publisizing the frame (==store to epoch).
     */
    writer->active_frame = wrap_writer_epoch_frame(writer->active_frame + 1U);
    frame = writer->epoch_frames + writer->active_frame;
    nvwal_atomic_store_release(&frame->head_offset, writer->last_tail_offset);
    nvwal_atomic_store_release(&frame->tail_offset, writer->last_tail_offset);
    nvwal_atomic_store_release(&frame->log_epoch, log_epoch);
  }
}

nvwal_error_t nvwal_on_wal_write(
  struct nvwal_writer_context* writer,
  uint64_t bytes_written,
  nvwal_epoch_t log_epoch) {
  struct nvwal_writer_epoch_frame* frame;

  assure_writer_active_frame(writer, log_epoch);
  frame = writer->epoch_frames + writer->active_frame;
  assert(frame->log_epoch == log_epoch);
  assert(frame->tail_offset == writer->last_tail_offset);

  /**
   * We should have enough space, right?
   * Otherwise the client didn't call nvwal_assure_writer_space().
   */
  assert(
    calculate_writer_offset_distance(
      writer,
      frame->tail_offset,
      frame->head_offset)
    + bytes_written
      < writer->parent->config.writer_buffer_size);

  writer->last_tail_offset = wrap_writer_offset(
    writer,
    frame->tail_offset + bytes_written);
  nvwal_atomic_store_release(&frame->tail_offset, writer->last_tail_offset);

  return 0;
}

bool nvwal_has_enough_writer_space(
  struct nvwal_writer_context* writer) {
  uint32_t oldest_frame;
  uint64_t consumed_bytes;
  struct nvwal_writer_epoch_frame* frame;

  oldest_frame = nvwal_atomic_load_acquire(&writer->oldest_frame);
  frame = writer->epoch_frames + oldest_frame;
  consumed_bytes = calculate_writer_offset_distance(
    writer,
    frame->head_offset,
    writer->last_tail_offset);
  return (consumed_bytes * 2ULL <= writer->parent->config.writer_buffer_size);
}

/**************************************************************************
 *
 *  Flusher
 *
 ***************************************************************************/
nvwal_error_t process_one_writer(struct nvwal_writer_context * writer);

nvwal_error_t nvwal_flusher_main(
  struct nvwal_context* wal) {
  uint32_t cur_writer_id;
  nvwal_error_t error_code;

  error_code = 0;
  /** whatever error checks here */

  nvwal_atomic_store(&(wal->flusher_running), 1);
  while (1) {
    /** doesn't have to be seq_cst, and this code runs very frequently */
    if (nvwal_atomic_load_acquire(&(wal->flusher_stop_requested)) != 0) {
      break;
    }

    /* Look for work */
    for (cur_writer_id = 0; cur_writer_id < wal->config.writer_count; ++cur_writer_id) {
      error_code = process_one_writer(wal->writers + cur_writer_id);
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
  nvwal_atomic_store(&(wal->flusher_running), 0);

  return error_code;
}

nvwal_error_t process_one_writer(
  struct nvwal_writer_context * writer) {
  return 0;
}

#ifdef BLUH /** long way to go to get the following code compile. so far disable all of them */
nvwal_error_t process_one_writer(
  struct nvwal_writer_context * writer) {
  struct nvwal_context* wal;
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
      nvwal_log_segment * cur_segment = wal->segment[wal->cur_seg_idx];
      int next_seg_idx = wal->cur_seg_idx + 1 % wal->num_segments;
      nvwal_log_segment * next_segment = wal->segment[next_seg_idx];

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
    struct nvwal_context * wal = arg;

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
  struct nvwal_context * wal,
  int root_fd,
  int i) {
  struct nvwal_log_segment * seg = &wal->segment[i];
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
  struct nvwal_context* wal,
  struct nvwal_log_segment * seg) {
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
  struct nvwal_context * wal,
  struct nvwal_log_segment* seg) {

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
  struct nvwal_context * wal,
  struct nvwal_log_segment * seg) {
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