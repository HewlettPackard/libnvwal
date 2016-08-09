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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "nvwal_api.h"
#include "nvwal_atomics.h"
#include "nvwal_util.h"

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
  memcpy(wal->config, config, sizeof(*config));

  if (strnlen(kNvwalMaxPathLength, config->nv_root) >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: nv_root must be null terminated\n");
  } else if (strnlen(kNvwalMaxPathLength, config->block_root) >= kNvwalMaxPathLength) {
    return nvwal_raise_einval("Error: block_root must be null terminated\n");
  } else if (config->writer_count == 0 || config->writer_count > kNvwalMaxWorkers) {
    return nvwal_raise_einval_llu(
      "Error: writer_count must be 1 to %llu\n",
      kNvwalMaxWorkers);
  } else if ((config->writer_buffer_size % 512U) || config->writer_buffer_size == 0) {
    return nvwal_raise_einval(
      "Error: writer_buffer_size must be a non-zero multiply of page size (512)\n");
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
      "Error: nv_quota must be a multiply of %llu\n",
      kNvwalSegmentSize);
  } else if (wal->num_active_segments < 2U) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at least %llu\n",
      2ULL * kNvwalSegmentSize);
  } else if (wal->num_active_segments > kNvwalMaxActiveSegments) {
    return nvwal_raise_einval_llu(
      "Error: nv_quota must be at most %llu\n",
      kNvwalMaxActiveSegments * kNvwalSegmentSize);
  }

  wal->durable = config->resuming_epoch;
  wal->latest = config->resuming_epoch;

  for (i = 0; i < config->writer_count; ++i) {
    wal->writers[i].writer_seq_id = i;
    wal->writers[i].parent = wal;
    wal->writers[i].copied = 0;
    wal->writers[i].flushed = 0;
    wal->writers[i].buffer = config->writer_buffers[i];
  }

  nv_root_fd = open(config->nv_root, O_DIRECTORY|O_RDONLY);
  CHECK_FD_VALID(nv_root_fd);

  /* Initialize all nv segments */

  for (i = 0; i < wal->num_active_segments; ++i) {
    init_nvram_segment(wal, nv_root_fd, i);
  }

  /* Created the files, fsync parent directory */
  fsync(nv_root_fd);

  close(nv_root_fd);

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
    }
  }

  return 0;
}

/**************************************************************************
 *
 *  Writers
 *
 ***************************************************************************/

nvwal_error_t nvwal_on_wal_write(
  struct nvwal_writer_context* writer,
  uint64_t bytes_written,
  nvwal_epoch_t current) {
  /*
  * Very simple version which does not allow gaps in the volatile log
  * and does not check possible invariant violations e.g. tail-catching
  */

  /* Do something with current epoch and wal->latest
    * Is this where we might want to force a commit if epoch
    * is getting ahead of our durability horizon? */
  assert(writer->cb.latest_written <= current);
  writer->cb.latest_written = current;

  /* Bump complete pointer and wrap around if necessary */
  writer->cb.complete += bytes_written;
  if (writer->cb.complete >= writer->parent->config.writer_buffer_size) {
    writer->cb.complete -= writer->parent->config.writer_buffer_size;
  }

  /* Perhaps do something if we have a sleepy flusher thread */

  return 0;
}

nvwal_error_t nvwal_assure_writer_space(
  struct nvwal_writer_context* writer) {
    /*
      check size > circular_size(tail, head, size). Note the reversal
      of the usual parameter ordering, since we're looking for *free*
      space in the log. We could also consider circular_size(tail, durable).

      if it's bad, we probably want to do something like:
      check
      busy wait
      check
      nanosleep
      check
      grab (ticket?) lock
      while !check
      cond_wait
      drop lock
    */
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
      pmem_drain();

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
      allocate_backing_file(cur_segment);

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
  ASSERT_FD_VALID(fd);

  //posix_fallocate doesn't set errno, do it ourselves
  errno = posix_fallocate(fd, 0, kNvwalSegmentSize);
  ASSERT_NO_ERROR(err);
  err = ftruncate(fd, kNvwalSegmentSize);
  ASSERT_NO_ERROR(err);
  fsync(fd);

  /* First try for a hugetlb mapping */
  baseaddr = mmap(0,
                  kNvwalSegmentSize,
                  PROT_READ|PROT_WRITE,
                  MAP_SHARED|MAP_PREALLOCATE|MAP_HUGETLB
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
  ASSERT_NO_ERROR(baseaddr == MAP_FAILED);

  /* Even with fallocate we don't trust the metadata to be stable
    * enough for userspace durability. Actually write some bytes! */

  memset(baseaddr, 0x42, kNvwalSegmentSize);
  msync(fd);

  seg->seq = INVALID_SEQNUM;
  seg->nvram_fd = fd;
  seg->disk_fd = -1;
  seg->nv_baseaddr = baseaddr;
  seg->state = SEG_UNUSED;
  seg->dir_synced = 0;
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
                  O_CREAT|O_RDWR|O_TRUNC,
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
  }

  /* The file should already exist */
  snprintf(filename, "log-segment-%lu", our_sequence);
  our_fd = openat(wal->log_root_fd,
                  filename,
                  O_RDWR|O_TRUNC);

  ASSERT_FD_VALID(our_fd);
}
