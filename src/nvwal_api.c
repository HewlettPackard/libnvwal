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
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "nvwal_atomics.h"
#include "nvwal_impl_init.h"
#include "nvwal_mds.h"
#include "nvwal_util.h"
#include "nvwal_mds_types.h"

/** Lengthy init/uninit were moved to nvwal_impl_init.c */
nvwal_error_t nvwal_init(
  const struct NvwalConfig* given_config,
  struct NvwalContext* wal) {
  return nvwal_impl_init(given_config, wal);
}

nvwal_error_t nvwal_uninit(
  struct NvwalContext* wal) {
  return nvwal_impl_uninit(wal);
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
  if (left_offset == right_offset) {
    return 0;
  } else  if (left_offset < right_offset) {
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

    /**
     * Otherwise we caught up on the oldest.
     * The 5-frames should be enough to prevent this.
     */
    assert(frame->log_epoch_ == kNvwalInvalidEpoch);

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
      frame->head_offset_,
      frame->tail_offset_)
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

/**
 * Fsluher calls this to copy one writer's private buffer to NV-segment.
 * This method does not drain or fsync because we expect that
 * this method is frequently called and catches up with writers
 * after a small gap.
 */
nvwal_error_t flusher_copy_one_writer_to_nv(
  struct NvwalWriterContext* writer,
  nvwal_epoch_t target_epoch,
  uint8_t is_stable_epoch);

/**
 * Flusher calls this when one NV segment becomes full.
 * It recycles and populates the next segment, potentialy waiting for something.
 * Once this method returns without an error, segments_[cur_seg_idx_] is guaranteed
 * to be non-full.
 */
nvwal_error_t flusher_move_onto_next_nv_segment(
  struct NvwalContext* wal);

/** nvwal_flusher_main() just keeps calling this */
nvwal_error_t flusher_main_loop(struct NvwalContext* wal);

void nvwal_wait_for_flusher_start(struct NvwalContext* wal) {
  nvwal_impl_thread_state_wait_for_start(&wal->flusher_thread_state_);
}

nvwal_error_t nvwal_flusher_main(
  struct NvwalContext* wal) {
  nvwal_error_t error_code = 0;
  uint8_t* const thread_state = &wal->flusher_thread_state_;

  enum NvwalThreadState state
    = nvwal_impl_thread_state_try_start(thread_state);
  if (state != kNvwalThreadStateRunning) {
    /** Either the WAL context is already stopped or not in a valid state */
    errno = EIO;  /* Not sure appropriate, but closest */
    return EIO;
  }

  while (1) {
    sched_yield();
    assert((*thread_state) == kNvwalThreadStateRunning
      || (*thread_state) == kNvwalThreadStateRunningAndRequestedStop);
    /** doesn't have to be seq_cst, and this code runs very frequently */
    if (nvwal_atomic_load_acquire(thread_state) == kNvwalThreadStateRunningAndRequestedStop) {
      break;
    }

    error_code = flusher_main_loop(wal);
    if (error_code) {
      break;
    }
  }
  nvwal_impl_thread_state_stopped(thread_state);

  return error_code;
}

nvwal_error_t flusher_main_loop(struct NvwalContext* wal) {
  uint8_t* const thread_state = &wal->flusher_thread_state_;
  /**
   * We currently take a simple policy; always write out logs in DE+1.
   * As far as there is a log in this epoch, it's always correct to write them out.
   * The only drawback is that we might waste bandwidth for a short period while
   * we have already written out all logs in DE+1 and SE==DE+1.
   * In such a case, it's okay to start writing out DE+2 before
   * we bump up DE. But, it complicates the logics here.
   * Let's keep it simple & stupid for now.
   */
  const nvwal_epoch_t target_epoch
    = nvwal_increment_epoch(wal->durable_epoch_);
  const uint8_t is_stable_epoch = (target_epoch == wal->stable_epoch_);

  /* Look for work */
  for (uint32_t cur_writer_id = 0;
        cur_writer_id < wal->config_.writer_count_;
        ++cur_writer_id) {
    nvwal_error_t error_code = flusher_copy_one_writer_to_nv(
      wal->writers_ + cur_writer_id,
      target_epoch,
      is_stable_epoch);
    if (error_code) {
      return error_code;
    }
    /* Ensure writes are durable in NVM */
    /* pmem_drain(); */

    /* Some kind of metadata commit, we could use libpmemlog.
      * This needs to track which epochs are durable, what's on disk
      * etc. */
    //commit_metadata_updates(wal)

    /** Promptly react when obvious. but no need to be atomic read. */
    if ((*thread_state) == kNvwalThreadStateRunningAndRequestedStop) {
      break;
    }
  }

  return 0;
}

nvwal_error_t flusher_copy_one_writer_to_nv(
  struct NvwalWriterContext * writer,
  nvwal_epoch_t target_epoch,
  uint8_t is_stable_epoch) {
  struct NvwalContext* const wal = writer->parent_;

  /**
   * First, we need to figure out what is the frame of the writer
   * we should copy from.
   * After this loop, lower_bound_f will be the first frame (from oldest_frame)
   * whose epoch is not older than target_epoch.
   */
  int lower_bound_f;
  for (lower_bound_f = 0; lower_bound_f < kNvwalEpochFrameCount; ++lower_bound_f) {
    const int frame_index = writer->oldest_frame_ + lower_bound_f;
    struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + frame_index;
    nvwal_epoch_t frame_epoch = nvwal_atomic_load_acquire(&frame->log_epoch_);
    if (frame_epoch == kNvwalInvalidEpoch
      || nvwal_is_epoch_equal_or_after(target_epoch, frame_epoch)) {
      break;
    }
  }
  if (lower_bound_f == kNvwalEpochFrameCount) {
    return 0;  /** No frame in target epoch or newer. Probably an idle writer */
  }

  const int frame_index = writer->oldest_frame_ + lower_bound_f;
  struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + frame_index;
  nvwal_epoch_t frame_epoch = nvwal_atomic_load_acquire(&frame->log_epoch_);
  if (frame_epoch == kNvwalInvalidEpoch
    || nvwal_is_epoch_after(target_epoch, frame_epoch)) {
    return 0;  /** It's too new. Or target_epoch logs don't exist. Skip. */
  }

  assert(target_epoch == frame_epoch);
  const uint64_t segment_size = wal->config_.segment_size_;
  const uint64_t writer_buffer_size = wal->config_.writer_buffer_size_;
  while (1) {  /** Until we write out all logs in this frame */
    const uint32_t segment_index = wal->cur_seg_idx_;
    struct NvwalLogSegment* cur_segment = wal->segments_ + segment_index;

    /** We read the markers, then the data. Must prohibit reordering */
    const uint64_t head = nvwal_atomic_load_acquire(&frame->head_offset_);
    const uint64_t tail = nvwal_atomic_load_acquire(&frame->tail_offset_);

    const uint64_t distance = calculate_writer_offset_distance(writer, head, tail);
    assert(distance);  /** Then why this frame still exists?? */

    assert(cur_segment->written_bytes_ <= segment_size);
    const uint64_t writable_bytes = cur_segment->written_bytes_ - segment_size;
    const uint64_t copied_bytes = NVWAL_MIN(writable_bytes, distance);

    /** The following memcpy must not be reordered */
    nvwal_atomic_thread_fence(nvwal_memory_order_acquire);
    nvwal_circular_memcpy(
      cur_segment->nv_baseaddr_ + cur_segment->written_bytes_,
      writer->buffer_,
      writer_buffer_size,
      head,
      copied_bytes);

    uint64_t new_head = wrap_writer_offset(writer, head + copied_bytes);
    if (new_head == tail && is_stable_epoch) {
      /** This frame is done! */
      nvwal_atomic_store(&writer->oldest_frame_, wrap_writer_epoch_frame(frame_index + 1));
    } else {
      /** This frame might receive more logs. We just remember the new head */
      /** Not atomic because only flusher reads/writes head.. except init */
      frame->head_offset_ = new_head;
    }

    cur_segment->written_bytes_ += copied_bytes;
    if (cur_segment->written_bytes_ == segment_size) {
      /* The segment is full. Move on to next, and also let the fsyncer know */
      nvwal_error_t error_code = flusher_move_onto_next_nv_segment(wal);
      if (error_code) {
        return error_code;
      }
      assert(segment_index != wal->cur_seg_idx_);
      continue;
    } else if (copied_bytes == distance) {
      break;
    }
  }

  return 0;
}


nvwal_error_t flusher_move_onto_next_nv_segment(
  struct NvwalContext* wal) {
  struct NvwalLogSegment* cur_segment = wal->segments_ + wal->cur_seg_idx_;
  assert(cur_segment->written_bytes_ == wal->config_.segment_size_);
  assert(cur_segment->fsync_requested_ == 0);
  assert(cur_segment->fsync_error_ == 0);
  assert(cur_segment->fsync_completed_ == 0);

  nvwal_atomic_store(&cur_segment->fsync_requested_, 1U);  /** Signal to fsyncer */

  uint32_t new_index = wal->cur_seg_idx_ + 1;
  if (new_index == wal->config_.segment_size_) {
    new_index = 0;
  }

  /**
   * Now, we need to recycle this segment. this might involve a wait if
   * we haven't copied it to disk, or epoch-cursor is now reading from this segment.
   */
  struct NvwalLogSegment* new_segment = wal->segments_ + new_index;
  while (!nvwal_atomic_load_acquire(&new_segment->fsync_completed_)) {
    /** Should be rare! not yet copied to disk */
    sched_yield();
    nvwal_error_t fsync_error = nvwal_atomic_load_acquire(&new_segment->fsync_error_);
    if (fsync_error) {
      /** This is critical. fsyncher for some reason failed. */
      return fsync_error;
    }
  }

  /** TODO check if any epoch-cursor is now reading from this */

  /** Ok, let's recycle */
  new_segment->dsid_ = wal->largest_dsid_ + 1;
  wal->largest_dsid_ = new_segment->dsid_;
  new_segment->written_bytes_ = 0;
  new_segment->fsync_completed_ = 0;
  new_segment->fsync_error_ = 0;
  new_segment->fsync_requested_ = 0;

  /** No need to be atomic. only flusher reads/writes it */
  wal->cur_seg_idx_ = new_index;
  return 0;
}


/**************************************************************************
 *
 *  Fsyncer
 *
 ***************************************************************************/

/**
 * Fsyncer calls this to durably copy one segment to disk.
 * On-disk file descriptor is completely contained in this method.
 * This method opens, uses, and closes the FD without leaving anything.
 */
nvwal_error_t fsyncer_sync_one_segment_to_disk(struct NvwalLogSegment* segment);

void nvwal_wait_for_fsync_start(struct NvwalContext* wal) {
  nvwal_impl_thread_state_wait_for_start(&wal->fsyncer_thread_state_);
}

nvwal_error_t nvwal_fsync_main(struct NvwalContext* wal) {
  uint32_t cur_segment;
  uint8_t* const thread_state = &wal->fsyncer_thread_state_;

  nvwal_error_t error_code = 0;
  enum NvwalThreadState state
    = nvwal_impl_thread_state_try_start(thread_state);
  if (state != kNvwalThreadStateRunning) {
    /** Either the WAL context is already stopped or not in a valid state */
    errno = EIO;  /* Not sure appropriate, but closest */
    return EIO;
  }

  while (1) {
    sched_yield();
    assert((*thread_state) == kNvwalThreadStateRunning
      || (*thread_state) == kNvwalThreadStateRunningAndRequestedStop);
    /** doesn't have to be seq_cst, and this code runs very frequently */
    if (nvwal_atomic_load_acquire(thread_state) == kNvwalThreadStateRunningAndRequestedStop) {
      break;
    }

    for (cur_segment = 0; cur_segment < wal->segment_count_; ++cur_segment) {
      struct NvwalLogSegment* segment = wal->segments_ + cur_segment;
      if (nvwal_atomic_load_acquire(&(segment->fsync_requested_))) {
        error_code = fsyncer_sync_one_segment_to_disk(wal->segments_ + cur_segment);
        if (error_code) {
          break;
        }
      }

      /** Promptly react when obvious. but no need to be atomic read. */
      if ((*thread_state) == kNvwalThreadStateRunningAndRequestedStop) {
        break;
      }
    }
  }
  nvwal_impl_thread_state_stopped(thread_state);

  return error_code;
}


nvwal_error_t fsyncer_sync_one_segment_to_disk(struct NvwalLogSegment* segment) {
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
    if (segment->parent_->fsyncer_thread_state_ == kNvwalThreadStateRunningAndRequestedStop) {
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
  reader->seg_id_start_ = 0;
  reader->seg_id_end_ = 0;
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
    reader->seg_id_start_ = epoch_meta.from_seg_id_;
    reader->seg_id_end_ = epoch_meta.from_seg_id_;
  } else
  {
    /* If epoch != prev_epoch, it means we didn't finish returning
     * all of prev_epoch, but now the client is asking for a different
     * epoch. Is this intentional? */

    /* We already have the last segment we tried to mmap.
     * We need to clean up the previous mapping before 
     * mapping more of this epoch.
     * */
    if (NULL != reader->mmap_start_)
    {
      consumed_epoch(wal, epoch);
      reader->seg_id_start_ = reader->seg_id_end_;
    } /* else, the client must have called done_with_epoch before 
       * calling here again */
  }
  do 
  {

    uint64_t offset = 0;
    uint64_t map_len = 0;
    if (reader->seg_id_end_ == epoch_meta.from_seg_id_)
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
    } else if (reader->seg_id_end_ < epoch_meta.to_seg_id_)
    {
      /* This is a middle segment; we're going to map the entire segment. */
      map_len = wal->config_.segment_size_;
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
      /* Atomically mark the segment as in use, if it's in NVDIMM */
      nvwal_concat_sequence_filename(
      wal->config_.nv_root_, 
      "nv_segment_",
      reader->seg_id_end_,
      backing_path);
      /* Did it get cleaned between time of check to time of use? 
       * Need to catch a return value here. */
    
    } else
    {
      nvwal_concat_sequence_filename(
        wal_->config_.disk_root_,
        "nvwal_ds",
        reader->seg_id_end_,
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


    *len += map_len;

    mmap_addr += map_len;

    reader->seg_id_end_++;
  
  } while (reader->seg_id_end_ <= epoch_meta.to_seg_id_);

  
  reader->mmap_start_ = *buf;
  reader->mmap_len_ = *len;
  reader->fetch_complete_ = 1;
  reader->seg_id_end_--; /* seg_id_end_ should be the last seg_id for this epoch */
  reader->prev_epoch_ = epoch;

  return error_code; /* no error */
}

nvwal_error_t consumed_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t const epoch)
{
  nvwal_error_t error_code = 0;
  struct NvwalReaderContext *reader = &wal->reader_;

  struct MdsEpochMetadata epoch_meta;
  //mds_read_epoch(wal->mds, epoch, &epoch_meta); //need to catch a return code

  if (NULL != reader->mmap_start_)
  {
    /* We only know about the last contiguous mapping we completed.
     * get_epoch() unmapped the previous region when retrying to finish mapping the
     * desired epoch. Can the client abuse this API and cause us to
     * lose track of maps and be unable munmap them? */
    munmap(reader->mmap_start_, reader->mmap_len_);
    reader->mmap_start_ = NULL;
    reader->mmap_len_ = 0;
  }

  nvwal_dsid_t segment_id = reader->seg_id_start_; 
  /* If we couldn't mmap the entire epoch in a contiguous mapping, seg_id_end_ is the one we failed to map */
  /* If !fetch_complete_ and start_ == end_, it means we failed on mapping the first segment. Nothing to do. 
   * segment_id_last < segment_id */ 
  nvwal_dsid_t segment_id_last =  (reader->fetch_complete_) ? epoch_meta.to_seg_id_ : reader->seg_id_end_-1;

  do 
  {

    /* Is it on NVDIMM or disk? */

    /* Atomically mark the segment as free or some quiesced state, if it's in NVDIMM */

    segment_id++;
  
  } while (segment_id <= segment_id_last);


  return error_code; /* no error */
}

