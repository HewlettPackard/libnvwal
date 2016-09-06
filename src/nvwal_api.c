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
  enum NvwalInitMode mode,
  struct NvwalContext* wal) {
  return nvwal_impl_init(given_config, mode, wal);
}

nvwal_error_t nvwal_uninit(
  struct NvwalContext* wal) {
  return nvwal_impl_uninit(wal);
}

nvwal_error_t nvwal_query_durable_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t* out) {
  *out = nvwal_atomic_load(&wal->durable_epoch_);
  return 0;
}

uint64_t nvwal_get_version() {
  enum NvwalVersionNumber {
    /**
    * Version of the library.
    * Whenever we [might] break compatibility of file formarts etc,
    * we bump this up.
    */
    kNvwalBinaryVersion = 1,
  };
  return kNvwalBinaryVersion;
}


/**************************************************************************
 *
 *  Writers
 *
 ***************************************************************************/
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
  uint64_t buffer_size = writer->parent_->config_.writer_buffer_size_;
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
  uint64_t buffer_size = writer->parent_->config_.writer_buffer_size_;
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
  struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + writer->active_frame_;
  if (frame->log_epoch_ == log_epoch) {
    /** The epoch exists. Most likely this case. */
  } else {
    /**
     * We must newly populate a frame for this epoch.
     * Release offsets before publisizing the frame (==store to epoch).
     */
    if (frame->log_epoch_ == kNvwalInvalidEpoch) {
      /** null active frame means we have no active frame! probably has been idle */
      assert(writer->active_frame_ == writer->oldest_frame_);
    } else {
      /** active frame is too old. we move on to next */
      writer->active_frame_ = wrap_writer_epoch_frame(writer->active_frame_ + 1U);
      /**
       * Now active_frame is surely ahead of oldest_frame.
       * If the assert below fires, this writer was issueing too new epochs,
       * violating the "upto + 2" contract.
       */
      assert(writer->active_frame_ != writer->oldest_frame_);
      frame = writer->epoch_frames_ + writer->active_frame_;
    }

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

/**
 * Invoked from flusher_main_loop to durably bump up paged_mds_epoch_.
 */
nvwal_error_t flusher_update_mpe(struct NvwalContext* wal, nvwal_epoch_t new_mpe) {
  assert(nvwal_is_epoch_equal_or_after(
    new_mpe,
    wal->nv_control_block_->flusher_progress_.paged_mds_epoch_));
  nvwal_atomic_store(&wal->nv_control_block_->flusher_progress_.paged_mds_epoch_, new_mpe);

  if (msync(
    &wal->nv_control_block_->flusher_progress_.paged_mds_epoch_,
    sizeof(wal->nv_control_block_->flusher_progress_.paged_mds_epoch_),
    MS_SYNC)) {
    assert(errno);
    return errno;
  }

  return 0;
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

  if (wal->flusher_current_epoch_head_dsid_ == 0) {
    const struct NvwalLogSegment* cur_segment = wal->segments_ + wal->cur_seg_idx_;
    wal->flusher_current_epoch_head_dsid_ = cur_segment->dsid_;
    assert(wal->flusher_current_epoch_head_dsid_);
    wal->flusher_current_epoch_head_offset_ = cur_segment->written_bytes_;
  }

  /*
   * We don't make things durable for each writer-traversal.
   * We rather do it after taking a look at all workers.
   * Otherwise it's too frequent.
   */
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

    /** Promptly react when obvious. but no need to be atomic read. */
    if ((*thread_state) == kNvwalThreadStateRunningAndRequestedStop) {
      break;
    }
  }

  if (is_stable_epoch && target_epoch != wal->durable_epoch_) {
    /* We wrote out all logs in this epoch! Now we can bump up DE */
    struct MdsEpochMetadata new_meta;
    new_meta.epoch_id_ = target_epoch;
    new_meta.from_seg_id_ = wal->flusher_current_epoch_head_dsid_;
    new_meta.from_offset_ = wal->flusher_current_epoch_head_offset_;
    const struct NvwalLogSegment* cur_segment = wal->segments_ + wal->cur_seg_idx_;
    new_meta.to_seg_id_ = cur_segment->dsid_;
    new_meta.to_off_ = cur_segment->written_bytes_;

    const nvwal_error_t mds_ret = mds_write_epoch(wal, &new_meta);
    if (mds_ret) {
      if (mds_ret == ENOBUFS) {
        /* This is an expected error saying that we should trigger paging */
        const nvwal_epoch_t new_paged_epoch = wal->durable_epoch_;
        /* TODO NVWAL_CHECK_ERROR(mds_evict_page(wal, new_paged_epoch)); */

        /* Also durably record that we paged MDS */
        NVWAL_CHECK_ERROR(flusher_update_mpe(wal, new_paged_epoch));

        /* Then try again. This time it should succeed */
        NVWAL_CHECK_ERROR(mds_write_epoch(wal, &new_meta));
      } else {
        return mds_ret;
      }
    }

    nvwal_atomic_store(&wal->nv_control_block_->flusher_progress_.durable_epoch_, target_epoch);
    nvwal_atomic_store(&wal->durable_epoch_, target_epoch);

    wal->flusher_current_epoch_head_dsid_ = cur_segment->dsid_;
    wal->flusher_current_epoch_head_offset_ = cur_segment->written_bytes_;
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
      || nvwal_is_epoch_equal_or_after(frame_epoch, target_epoch)) {
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
    || nvwal_is_epoch_after(frame_epoch, target_epoch)) {
    return 0;  /** It's too new. Or target_epoch logs don't exist. Skip. */
  }

  assert(target_epoch == frame_epoch);
  const uint64_t segment_size = wal->config_.segment_size_;
  const uint64_t writer_buffer_size = wal->config_.writer_buffer_size_;
  while (1) {  /** Until we write out all logs in this frame */
    const uint32_t segment_index = wal->cur_seg_idx_;
    struct NvwalLogSegment* cur_segment = wal->segments_ + segment_index;
    assert(cur_segment->nv_baseaddr_);

    /** We read the markers, then the data. Must prohibit reordering */
    const uint64_t head = nvwal_atomic_load_acquire(&frame->head_offset_);
    const uint64_t tail = nvwal_atomic_load_acquire(&frame->tail_offset_);

    const uint64_t distance = calculate_writer_offset_distance(writer, head, tail);
    if (distance == 0) {
      return 0;  /** no relevant logs here... yet */
    }

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
      memset(
        writer->epoch_frames_ + frame_index,
        0,
        sizeof(struct NvwalWriterEpochFrame));
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
  assert(cur_segment->nv_segment_index_ == wal->cur_seg_idx_);
  assert(cur_segment->dsid_ > 0);
  assert((cur_segment->dsid_ - 1U) % wal->segment_count_
    == cur_segment->nv_segment_index_);
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
  assert(new_segment->dsid_ > 0);
  assert((new_segment->dsid_ - 1U) % wal->segment_count_ == new_index);
  new_segment->dsid_ += wal->segment_count_;
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
  nvwal_construct_disk_segment_path(
    segment->parent_,
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

/** @brief get_epoch() tries to mmap cursor->current_epoch_. If it cannot
 * mmap the entire epoch into a contiguous mapping, cursor->fetch_complete
 * will be set to 0.
 */
nvwal_error_t get_epoch(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor,
  struct MdsEpochMetadata target_epoch_meta) {

  struct NvwalEpochMapMetadata* epoch_map = cursor->read_metadata_ + cursor->free_map_;
  memset(epoch_map, 0, sizeof(struct NvwalEpochMapMetadata));
  struct MdsEpochMetadata epoch_meta = target_epoch_meta;
  nvwal_error_t error_code = 0;
  char* mmap_addr = 0;
  uint8_t first_mmap = 1;
  

  /* Check if epoch is a valid epoch number */
  /* And that current_epoch_ matches epoch_meta.epoch_id_ */

  /* Is this a retry call because we didn't finish mmapping everything 
   * for cursor->current_epoch_? */
  if (cursor->fetch_complete_)
  {
    /* We successfully fetched all of cursor->current_epoch before.
     * This is our first time trying to fetch any data from
     * target_epoch_meta.epoch_id_.
     * Initialize our segment progress for this mapping */
    epoch_map->seg_id_start_ = epoch_meta.from_seg_id_;
    epoch_map->seg_start_offset_ = epoch_meta.from_offset_;
  } else
  {
    /* We already mmapped part of this epoch. The last segment
     * mapped is in the read_metadata[free_map - 1]
     */
    int prev_map = (cursor->free_map_ - 1 < 0) ? (kNvwalNumReadRegions - 1) : (cursor->free_map_ - 1);
    epoch_map->seg_id_start_ = cursor->read_metadata_[prev_map].seg_id_end_;
    epoch_map->seg_start_offset_ = cursor->read_metadata_[prev_map].seg_end_offset_;
     /* Do we need to clean up the previous mapping before 
     * mapping more of this epoch?
     * */
    /*
    if (NULL != reader->mmap_start_)
    {
      consumed_epoch(wal, epoch);
      reader->seg_id_start_ = reader->seg_id_end_;
    }
    */  
  }

  /* Now starting epoch_meta.epoch_id_, map as many epochs as we can
   * into a contiguous mapping until we MAP_FAILED or we reach the
   * last epoch requested */
  nvwal_epoch_t epoch_to_map = epoch_meta.epoch_id_;
  epoch_map->seg_id_end_ = epoch_map->seg_id_start_;
  epoch_map->seg_end_offset_ = 0;
  nvwal_dsid_t segment_id = epoch_map->seg_id_start_;

  do
  {
  /* Mmap an epoch */
  do 
  {
    /* Mmap a segment of epoch_to_map. */
    uint64_t offset = 0;
    uint64_t map_len = 0;
    if (segment_id == epoch_meta.from_seg_id_)
    {
      /* This is the first segment */
      if (segment_id == epoch_meta.to_seg_id_)
      {
        /* This is also the only segment. */
        map_len = epoch_meta.to_off_ - epoch_meta.from_offset_;
      } else
      {
        /* There are more segments to follow. Mmap to the end of the segment. */
        map_len = wal->config_.segment_size_ - epoch_meta.from_offset_;
      }

      offset = epoch_meta.from_offset_;
    } else if (segment_id < epoch_meta.to_seg_id_)
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
      segment_id,
      backing_path);
      /* Did it get cleaned between time of check to time of use? 
       * Need to catch a return value here. */
    
    } else
    {
      nvwal_concat_sequence_filename(
        wal_->config_.disk_root_,
        "nvwal_ds",
        segment_id,
        backing_path);
    }
#endif
    int fd = -1; /*= open();*/
    if (-1 == fd)
    {
      error_code = errno;
      return error_code;
    }

    if (first_mmap)
    {
      /* This is the first mmap attempt for the epoch_meta.epoch_id_,
       * i.e. the first epoch of this get_epoch() call.
       * Let the kernel pick where to start and save the beginning of the mmap. */
      char* buf = mmap(mmap_addr, map_len, PROT_READ, MAP_SHARED, fd, offset);
      close(fd);
      first_mmap = 0;
      if (MAP_FAILED == buf)
      {
        /* Pretty bad to fail on the first attempt while letting the kernel pick */
        error_code = errno;
        if (cursor->current_epoch_ == epoch_to_map)
        {
          cursor->fetch_complete_ = 0;
        } else
        {
          //we were prefetching some future epoch */
          cursor->prefetch_complete_ = 0;
        }
        cursor->free_map_++;
        if (cursor->free_map_ >= kNvwalNumReadRegions)
        {
          cursor->free_map_ = 0;
        }
        return error_code; /*something*/
      }
      /* We successfully mapped part of the target epoch. Update the cursor. */
      cursor->current_epoch_ = epoch_to_map;
      epoch_map->mmap_start_ = (nvwal_byte_t*)buf;
    } else
    {
      char* fixed_map = mmap(mmap_addr, map_len, PROT_READ, MAP_SHARED|MAP_FIXED, fd, 0);
      close(fd);
      if (MAP_FAILED == fixed_map)
      {
        error_code = errno;
        if (cursor->current_epoch_ == epoch_to_map)
        {
          cursor->fetch_complete_ = 0;
        } else
        {
          //we were prefetching some future epoch */
          cursor->prefetch_complete_ = 0;
        }
        cursor->free_map_++;
        if (cursor->free_map_ >= kNvwalNumReadRegions)
        {
          cursor->free_map_ = 0;
        }
        return error_code; /* retry */
      }
    }

    mmap_addr += map_len;

    epoch_map->mmap_len_ += map_len;
    epoch_map->seg_id_end_ = segment_id;
    epoch_map->seg_end_offset_ = map_len;
    segment_id++;
  
  } while (segment_id <= epoch_meta.to_seg_id_);
  // We finished fetching the first epoch/an epoch. Keep trying to extend this mapping.
    if (cursor->current_epoch_ == target_epoch_meta.epoch_id_)
    {
      cursor->fetch_complete_ = 1;
    } else
    {
      cursor->prefetch_complete_ = 1;
    }
    epoch_to_map++;
    //mds_read_epoch(wal->mds, epoch_to_map, &epoch_meta); //need to catch a return code
  } while (epoch_to_map <= cursor->end_epoch_);

  cursor->free_map_++;
  if (cursor->free_map_ >= kNvwalNumReadRegions)
  {
    cursor->free_map_ = 0;
  }

  return error_code; /* no error */
}
#if 0
nvwal_error_t consumed_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t const epoch)
{
  nvwal_error_t error_code = 0;
  struct NvwalReaderContext *reader = &wal->reader_;

  struct MdsEpochMetadata epoch_meta;
  //mds_read_epoch(wal->mds, epoch, &epoch_meta); //need to catch a return code

  /* Need guard against client claiming to have 
   * consumed an epoch which is not the mmapped epoch.
   * ReaderContext should have a mapping between mmap_start and an epoch
   * number which will bemore easily handled with prefetching machinery.
   */

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
    /* Is this the only epoch in this segment or do we need it for
     * subsequent epoch mapping? */
    /* Is another reader also using this segment */
    /* Atomically mark the segment as free or some quiesced state, if it's in NVDIMM */

    segment_id++;
  
  } while (segment_id <= segment_id_last);


  return error_code; /* no error */
}
#endif
nvwal_error_t nvwal_open_log_cursor(
  struct NvwalContext* wal,
  struct NvwalLogCursor* out,
  nvwal_epoch_t begin_epoch,
  nvwal_epoch_t end_epoch) {

  nvwal_error_t error_code = 0;

  memset(out, 0, sizeof(*out));

  out->current_epoch_ = kNvwalInvalidEpoch;
  out->fetch_complete_ = 1;
  out->data_ = NULL;
  out->data_len_ = 0;
  out->start_epoch_ = begin_epoch;
  out->end_epoch_ = end_epoch; 
  out->free_map_ = 0;
  out->current_map_ = 0;
  for (int i = 0; i < kNvwalNumReadRegions; i++)
  {
    out->read_metadata_[i].seg_id_start_ = kNvwalInvalidDsid; 
    out->read_metadata_[i].seg_id_end_ = kNvwalInvalidDsid;
    out->read_metadata_[i].seg_start_offset_ = 0;
    out->read_metadata_[i].seg_end_offset_ = 0;
    out->read_metadata_[i].mmap_start_ = NULL; 
    out->read_metadata_[i].mmap_len_ = 0;
  }

  return error_code;
}

nvwal_error_t nvwal_close_log_cursor(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
  
  nvwal_error_t error_code = 0;

  return error_code;

}

nvwal_error_t nvwal_cursor_next_epoch(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {

  nvwal_error_t error_code = 0;

  if (kNvwalInvalidEpoch == cursor->current_epoch_)
  {
    cursor->current_epoch_ = cursor->start_epoch_;
    cursor->fetch_complete_ = 0;
  }

  /* Lookup the epoch info from the MDS */
  struct MdsEpochMetadata epoch_meta;
  if (cursor->fetch_complete_)
  {
    //We can unmap cursor->current_epoch_ now
    //mds_read_epoch(wal->mds, cursor->current_epoch_ + 1, &epoch_meta); //need to catch a return code
  } else
  {
    //mds_read_epoch(wal->mds, cursor->current_epoch_, &epoch_meta); //need to catch a return code
    /* If we are calling again and we didn't complete the fetch of current_epoch_, we must
     * have consumed all of read_metadata[current_map]*/
    cursor->current_map_++;
    cursor->current_map_ = cursor->current_map_%kNvwalNumReadRegions;
  }
  /* Is at least part of the desired epoch already fetched? */
  struct NvwalEpochMapMetadata* epoch_map = cursor->read_metadata_ + cursor->current_map_;
  /* If we have part of the epoch in epoch_map, we have the following cases:
   * case 1: epoch_map contains the start of the epoch but not the end,
   * case 2: epoch map_contains that start and end of the epoch,
   * case 3: epoch_map contains the middle of the epoch (neither start nor end),
   * case 4: epoch_map contains the end of the epoch.
   */
  /*if map valid, look at read_metadata[current_map] and return if found */
  if (NULL != epoch_map->mmap_start_)
  {
    uint64_t logical_map_start = epoch_map->seg_id_start_*wal->config_.segment_size_ 
                                 + epoch_map->seg_start_offset_;
    uint64_t logical_map_end = epoch_map->seg_id_end_*wal->config_.segment_size_ 
                               + epoch_map->seg_end_offset_;
    uint64_t logical_epoch_start = epoch_meta.from_seg_id_*wal->config_.segment_size_ 
                                   + epoch_meta.from_offset_;
    uint64_t logical_epoch_end = epoch_meta.to_seg_id_*wal->config_.segment_size_ 
                                 + epoch_meta.to_off_;
    
    if (cursor->fetch_complete_)
    {
      /* We are looking for the beginning of epoch_meta.epoch_id_ */
      cursor->data_ = epoch_map->mmap_start_ + logical_epoch_start - logical_map_start;
      /* Is the end of the epoch in this mapping? */
      if (logical_epoch_end <= logical_map_end)
      {
        /* case 2 */
        cursor->data_len_ = logical_epoch_end - logical_epoch_start;
        cursor->fetch_complete_ = 1;
      } else
      {
        /* case 1 */
        cursor->data_len_ = logical_map_end - logical_epoch_start;
        cursor->fetch_complete_ = 0;
      }
    } else
    {
      cursor->data_ = epoch_map->mmap_start_;
      if (logical_epoch_end <= logical_map_end)
      {
        /* case 4 */
        cursor->data_len_ = logical_epoch_end - logical_map_start;
        cursor->fetch_complete_ = 1;
      } else
      {
        /* case 3 */
        cursor->data_len_ = logical_map_end;
        cursor->fetch_complete_ = 0;
      }
    }
    cursor->current_epoch_ = epoch_meta.epoch_id_;
  } else
  {
    /* else go fetch it (and possibly more). */
    error_code = get_epoch(wal, cursor, epoch_meta);
  }


  return error_code;

}

uint8_t nvwal_cursor_is_valid(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {

  if (NULL == cursor->data_)
    return 0;
  else
    return 1;
}

nvwal_byte_t* nvwal_cursor_get_data(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
    if (nvwal_cursor_is_valid(wal, cursor))
      return cursor->data_;
    else
      return NULL;
}

uint64_t nvwal_cursor_get_data_length(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
    if (nvwal_cursor_is_valid(wal, cursor))
      return cursor->data_len_;
    else
      return 0;
}

nvwal_epoch_t nvwal_cursor_get_current_epoch(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
    if (nvwal_cursor_is_valid(wal, cursor))
      return cursor->current_epoch_;
    else
      return kNvwalInvalidEpoch;
}



