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
#include <libpmem.h>
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
#include "nvwal_impl_pin.h"
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

nvwal_error_t nvwal_advance_stable_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t new_stable_epoch) {
  const nvwal_epoch_t durable_epoch = nvwal_atomic_load_acquire(&wal->durable_epoch_);
  if (nvwal_increment_epoch(durable_epoch) != new_stable_epoch) {
    return 0;
  }

  nvwal_epoch_t expected = durable_epoch;
  nvwal_atomic_compare_exchange_strong(
    &wal->stable_epoch_,
    &expected,
    new_stable_epoch);

  assert(nvwal_is_epoch_equal_or_after(wal->stable_epoch_, new_stable_epoch));
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
    } else {
      /** active frame is too old. we move on to next */
      writer->active_frame_ = wrap_writer_epoch_frame(writer->active_frame_ + 1U);
      frame = writer->epoch_frames_ + writer->active_frame_;
    }

    /*
     * By writing in the following order, flusher won't pick up this frame
     * too early. frame->log_epoch_ should be now an irrelevant epoch,
     * thus until the 3rd line this has no effect.
     */
    nvwal_atomic_store_release(&frame->head_offset_, writer->last_tail_offset_);
    nvwal_atomic_store_release(&frame->tail_offset_, writer->last_tail_offset_);
    nvwal_atomic_store_release(&frame->log_epoch_, log_epoch);
  }
}

nvwal_error_t nvwal_on_wal_write(
  struct NvwalWriterContext* writer,
  uint64_t bytes_written,
  nvwal_epoch_t log_epoch) {
#ifndef NDEBUG
  const nvwal_epoch_t se = nvwal_atomic_load(&writer->parent_->stable_epoch_);
  assert(nvwal_is_epoch_after(log_epoch, se));
  const nvwal_epoch_t beyond_horizon = nvwal_add_epoch(se, kNvwalEpochFrameCount);
  assert(nvwal_is_epoch_after(beyond_horizon, log_epoch));
#endif  // NDEBUG

  assure_writer_active_frame(writer, log_epoch);
  struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + writer->active_frame_;
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

nvwal_error_t nvwal_tag_epoch(
  struct NvwalWriterContext* writer,
  nvwal_epoch_t target_epoch,
  uint64_t metadata)
{
  struct NvwalContext* const wal = writer->parent_;

  /**
   * First, we need to figure out what is the frame of the writer
   * we should write epoch metadata at.
   */
  int frame_index;
  for (frame_index = 0; frame_index < kNvwalEpochFrameCount; ++frame_index) {
    struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + frame_index;
    nvwal_epoch_t frame_epoch = nvwal_atomic_load_acquire(&frame->log_epoch_);
    if (frame_epoch == target_epoch) {
      break;
    }
  }
  if (frame_index == kNvwalEpochFrameCount) {
    return 0;  /** No frame in target epoch. Probably an idle writer */
  }

  struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + frame_index;
  const nvwal_epoch_t frame_epoch = nvwal_atomic_load_acquire(&frame->log_epoch_);
  assert(target_epoch == frame_epoch);
 
  nvwal_atomic_store_release(&frame->user_metadata_, metadata);

  return 0;
}

uint8_t nvwal_has_enough_writer_space(
  struct NvwalWriterContext* writer) {
  /*
   * Check all frames (not many, don't worry!) to see
   * which regions could be being used.
   */
  const nvwal_epoch_t durable
    = nvwal_atomic_load_acquire(&writer->parent_->durable_epoch_);
  const nvwal_epoch_t beyond_horizon
    = nvwal_add_epoch(durable, kNvwalEpochFrameCount);

  uint64_t consumed_bytes = 0;
  for (int frame_index = 0;
        frame_index < kNvwalEpochFrameCount;
        ++frame_index) {
    struct NvwalWriterEpochFrame* frame
      = writer->epoch_frames_ + frame_index;
    const nvwal_epoch_t frame_epoch
      = nvwal_atomic_load_acquire(&frame->log_epoch_);
    if (frame_epoch == kNvwalInvalidEpoch) {
      /* Simply unused */
      continue;
    } else if (nvwal_is_epoch_equal_or_after(durable, frame_epoch)) {
      /* Everything in DE is completely durable. This frame is in no use */
      continue;
    } else if (nvwal_is_epoch_equal_or_after(frame_epoch, beyond_horizon)) {
      /* This is a remnant of wrap-around */
      continue;
    }
    consumed_bytes += calculate_writer_offset_distance(
      writer,
      frame->head_offset_,
      frame->tail_offset_);
  }

  return (consumed_bytes * 2ULL <= writer->parent_->config_.writer_buffer_size_);
}

/**************************************************************************
 *
 *  Flusher/Fsyncer
 *
 ***************************************************************************/

struct NvwalLogSegment* flusher_get_segment_from_dsid(
  struct NvwalContext* wal,
  nvwal_dsid_t dsid) {
  assert(dsid);
  uint32_t index = (dsid - 1U) % wal->segment_count_;
  return wal->segments_ + index;
}

struct NvwalLogSegment* flusher_get_cur_segment(
  struct NvwalContext* wal) {
  return flusher_get_segment_from_dsid(wal, wal->flusher_current_nv_segment_dsid_);
}

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
 * Invoked from flusher_conclude_stable_epoch to durably bump up CB's paged_mds_epoch_.
 */
nvwal_error_t flusher_update_mpe(struct NvwalContext* wal, nvwal_epoch_t new_mpe) {
  assert(nvwal_is_epoch_equal_or_after(
    new_mpe,
    wal->nv_control_block_->flusher_progress_.paged_mds_epoch_));
  /* No race in CB. Usual write */
  wal->nv_control_block_->flusher_progress_.paged_mds_epoch_ = new_mpe;

  /** But, it must be a durable write */
  pmem_persist(
    &wal->nv_control_block_->flusher_progress_.paged_mds_epoch_,
    sizeof(wal->nv_control_block_->flusher_progress_.paged_mds_epoch_));

  return 0;
}
/**
 * Invoked from flusher_conclude_stable_epoch to durably bump up CB's durable_epoch_.
 */
nvwal_error_t flusher_update_de(struct NvwalContext* wal, nvwal_epoch_t new_de) {
  assert(nvwal_is_epoch_equal_or_after(
    new_de,
    wal->nv_control_block_->flusher_progress_.durable_epoch_));
  /* Same as above */
  wal->nv_control_block_->flusher_progress_.durable_epoch_ = new_de;

  pmem_persist(
    &wal->nv_control_block_->flusher_progress_.durable_epoch_,
    sizeof(wal->nv_control_block_->flusher_progress_.durable_epoch_));

  return 0;
}

/**
 * Invoked from flusher_main_loop to advance durable_epoch to stable_epoch.
 */
nvwal_error_t flusher_conclude_stable_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t target_epoch) {
  /* We wrote out all logs in this epoch! Now we can bump up DE */
  struct MdsEpochMetadata new_meta;
  new_meta.epoch_id_ = target_epoch;
  new_meta.from_seg_id_ = wal->flusher_current_epoch_head_dsid_;
  new_meta.from_offset_ = wal->flusher_current_epoch_head_offset_;
  const struct NvwalLogSegment* cur_segment = flusher_get_cur_segment(wal);
  new_meta.to_seg_id_ = cur_segment->dsid_;
  new_meta.to_off_ = cur_segment->written_bytes_;
  new_meta.user_metadata_ = wal->flusher_current_epoch_user_metadata_;

  /*
   * Individual copies to NV-segments were just usual memcpy without drain/persist.
   * Rather than we invoke persist for individual copies, we persist all writes
   * in this epoch here. This dramatically reduces the number of persist calls.
   */
  nvwal_dsid_t disk_dsid = wal->nv_control_block_->fsyncer_progress_.last_synced_dsid_;
  for (nvwal_dsid_t dsid = new_meta.from_seg_id_;
       dsid <= new_meta.to_seg_id_;
      ++dsid) {
    if (dsid <= disk_dsid) {
      /* If it's already on disk, definitely persisted. */
      continue;
    }
    uint64_t from_offset = 0;
    if (dsid == new_meta.from_seg_id_) {
      from_offset = new_meta.from_offset_;
    }
    uint64_t to_offset = wal->config_.segment_size_;
    if (dsid == new_meta.to_seg_id_) {
      to_offset = new_meta.to_off_;
    }
    assert(from_offset <= to_offset);
    uint32_t segment_index = (dsid - 1U) % wal->segment_count_;
    assert(wal->segments_[segment_index].dsid_ == dsid);
    pmem_persist(
      wal->segments_[segment_index].nv_baseaddr_ + from_offset,
      to_offset - from_offset);
  }

  const nvwal_error_t mds_ret = mds_write_epoch(wal, &new_meta);
  if (mds_ret) {
    return mds_ret;
  }

  /*
    * We have two instances of durable_epoch_. MDS already 
    * durably wrote to CB's durable_epoch_. Here we 'announce' it to other threads
    * by writing to wal->durable_epoch_. No usual thread directly refer to
    * CB's durable_epoch_.
    */
  nvwal_atomic_store(&wal->durable_epoch_, target_epoch);

  wal->flusher_current_epoch_head_dsid_ = cur_segment->dsid_;
  wal->flusher_current_epoch_head_offset_ = cur_segment->written_bytes_;

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
    NVWAL_CHECK_ERROR(flusher_conclude_stable_epoch(wal, target_epoch));
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
   * Here we assume that we are writing out only the logs in target_epoch.
   * All logs before that were already written out.
   */
  int frame_index;
  for (frame_index = 0; frame_index < kNvwalEpochFrameCount; ++frame_index) {
    struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + frame_index;
    nvwal_epoch_t frame_epoch = nvwal_atomic_load_acquire(&frame->log_epoch_);
    if (frame_epoch == target_epoch) {
      break;
    }
  }
  if (frame_index == kNvwalEpochFrameCount) {
    return 0;  /** No frame in target epoch. Probably an idle writer */
  }

  struct NvwalWriterEpochFrame* frame = writer->epoch_frames_ + frame_index;
  const nvwal_epoch_t frame_epoch = nvwal_atomic_load_acquire(&frame->log_epoch_);
  assert(target_epoch == frame_epoch);

  /* 
   * Copy user tagged metadata only if this is a stable epoch.
   * In case multiple writers tag the same epoch, then last non-zero writer wins. 
   */
  if (is_stable_epoch) {
    uint64_t user_metadata = nvwal_atomic_load_acquire(&frame->user_metadata_);
    wal->flusher_current_epoch_user_metadata_ = 
        (user_metadata != 0) ? user_metadata: wal->flusher_current_epoch_user_metadata_;
  }

  const uint64_t segment_size = wal->config_.segment_size_;
  const uint64_t writer_buffer_size = wal->config_.writer_buffer_size_;
  while (1) {  /** Until we write out all logs in this frame */
    struct NvwalLogSegment* cur_segment = flusher_get_cur_segment(wal);
    assert(cur_segment->nv_baseaddr_);

    /** We read the markers, then the data. Must prohibit reordering */
    const uint64_t head = nvwal_atomic_load_acquire(&frame->head_offset_);
    const uint64_t tail = nvwal_atomic_load_acquire(&frame->tail_offset_);

    const uint64_t distance = calculate_writer_offset_distance(writer, head, tail);
    if (distance == 0) {
      return 0;  /** no relevant logs here... yet */
    }

    assert(cur_segment->written_bytes_ <= segment_size);
    const uint64_t writable_bytes = segment_size - cur_segment->written_bytes_;
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
    /** This frame might receive more logs. We just remember the new head */
    /** The store must be in order because nvwal_has_enough_writer_space() depends on it */
    nvwal_atomic_store_release(&frame->head_offset_, new_head);

    cur_segment->written_bytes_ += copied_bytes;
    if (cur_segment->written_bytes_ == segment_size) {
      /* The segment is full. Move on to next, and also let the fsyncer know */
      nvwal_error_t error_code = flusher_move_onto_next_nv_segment(wal);
      if (error_code) {
        return error_code;
      }
      continue;
    } else if (copied_bytes == distance) {
      break;
    }
  }

  return 0;
}


nvwal_error_t flusher_move_onto_next_nv_segment(
  struct NvwalContext* wal) {
  struct NvwalLogSegment* cur_segment = flusher_get_cur_segment(wal);
  assert(cur_segment->dsid_ > 0);
  assert((cur_segment->dsid_ - 1U) % wal->segment_count_
    == cur_segment->nv_segment_index_);
  assert(cur_segment->written_bytes_ == wal->config_.segment_size_);
  assert(cur_segment->fsync_requested_ == 0);
  assert(cur_segment->fsync_error_ == 0);
  assert(cur_segment->fsync_completed_ == 0);

  nvwal_atomic_store(&cur_segment->fsync_requested_, 1U);  /** Signal to fsyncer */

  /**
   * Now, we need to recycle next segment. this might involve a wait if
   * we haven't copied it to disk, or epoch-cursor is now reading from this segment.
   */
  const nvwal_dsid_t next_dsid = wal->flusher_current_nv_segment_dsid_ + 1U;
  struct NvwalLogSegment* new_segment = flusher_get_segment_from_dsid(wal, next_dsid);
  while (new_segment->fsync_requested_
    && !nvwal_atomic_load_acquire(&new_segment->fsync_completed_)) {
    /** Should be rare! not yet copied to disk */
    sched_yield();
    nvwal_error_t fsync_error = nvwal_atomic_load_acquire(&new_segment->fsync_error_);
    if (fsync_error) {
      /** This is critical. fsyncher for some reason failed. */
      return fsync_error;
    }
  }

  /** Wait while any epoch-cursor is now reading from this */
  nvwal_pin_flusher_unconditional_lock(&new_segment->nv_reader_pins_);

  /** Ok, let's recycle */
  assert(new_segment->dsid_ > 0);
  assert((new_segment->dsid_ - 1U) % wal->segment_count_
    == (next_dsid - 1U) % wal->segment_count_);
  new_segment->dsid_ = next_dsid;
  new_segment->written_bytes_ = 0;
  new_segment->fsync_completed_ = 0;
  new_segment->fsync_error_ = 0;
  new_segment->fsync_requested_ = 0;

  nvwal_pin_flusher_unlock(&new_segment->nv_reader_pins_);

  /** No need to be atomic. only flusher reads/writes it */
  wal->flusher_current_nv_segment_dsid_ = next_dsid;
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

    uint32_t cur_segment;
    const nvwal_dsid_t last_sync_dsid = wal->nv_control_block_->fsyncer_progress_.last_synced_dsid_;
    if (last_sync_dsid == kNvwalInvalidDsid) {
      cur_segment = 0; 
    } else {
      cur_segment = last_sync_dsid % wal->segment_count_;
    }
    
    struct NvwalLogSegment* segment = wal->segments_ + cur_segment;
    if (!segment->fsync_completed_
      && nvwal_atomic_load_acquire(&(segment->fsync_requested_))) {
      error_code = fsyncer_sync_one_segment_to_disk(wal->segments_ + cur_segment);
      if (error_code) {
        break;
      }
    }

    /** Promptly react when obvious. but no need to be atomic read. */
    if ((*thread_state) == kNvwalThreadStateRunningAndRequestedStop) {
      break;
    }

    if (error_code) {
      break;
    }
  }
  nvwal_impl_thread_state_stopped(thread_state);

  return error_code;
}


nvwal_error_t fsyncer_sync_one_segment_to_disk(struct NvwalLogSegment* segment) {
  const nvwal_dsid_t dsid = nvwal_atomic_load_acquire(&segment->dsid_);
  assert(dsid);
  assert(!segment->fsync_completed_);
  nvwal_error_t ret = 0;
  segment->fsync_error_ = 0;
  char disk_path[kNvwalMaxPathLength];
  nvwal_construct_disk_segment_path(
    segment->parent_,
    dsid,
    disk_path);

  /*
   * Issue #15
   * This should work, but on our ProLiant box we had to
   * remove O_DIRECT. This just affects performance,
   * not correctness as we anyway do fsync.
   */
  int disk_fd = open(  /* nvwal_open_best_effort_o_direct( */
    disk_path,
    O_CREAT | O_RDWR,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
  assert(disk_fd);
  if (disk_fd == -1) {
    /** Probably permission issue? */
    ret = errno;
    goto error_return;
  }

  uint64_t total_written = 0;
  /** Be aware of the case where write() doesn't finish in one call */
  while (total_written < segment->parent_->config_.segment_size_) {
    uint64_t written = write(
      disk_fd,
      segment->nv_baseaddr_ + total_written,
      segment->parent_->config_.segment_size_ - total_written);
    if (written == -1) {
      /** Probably full disk? */
      ret = errno;
      goto error_return;
    }
    total_written += written;

    /** Is this fsyncher cancelled for some reason? */
    if (segment->parent_->fsyncer_thread_state_ == kNvwalThreadStateRunningAndRequestedStop) {
      ret = ETIMEDOUT;  /* Not sure this is appropriate.. */
      goto error_return;
    }
  }

  fsync(disk_fd);
  close(disk_fd);
  nvwal_open_and_fsync(segment->parent_->config_.disk_root_);

  /* Writing to fsync_completed must be after reading segment->dsid_ */
  nvwal_atomic_store(&(segment->fsync_completed_), 1U);

  /* Durably bump up CB's progress info */
  assert(dsid 
    > segment->parent_->nv_control_block_->fsyncer_progress_.last_synced_dsid_);
  segment->parent_->nv_control_block_->fsyncer_progress_.last_synced_dsid_ = dsid;
  pmem_persist(
    &segment->parent_->nv_control_block_->fsyncer_progress_.last_synced_dsid_,
    sizeof(segment->parent_->nv_control_block_->fsyncer_progress_.last_synced_dsid_));

  return 0;

error_return:
  if (disk_fd && disk_fd != -1) {
    close(disk_fd);
  }
  errno = ret;
  segment->fsync_error_ = ret;
  return ret;
}
