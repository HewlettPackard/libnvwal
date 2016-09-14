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
#include "nvwal_impl_cursor.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "nvwal_api.h"
#include "nvwal_atomics.h"
#include "nvwal_impl_pin.h"
#include "nvwal_mds.h"
#include "nvwal_mds_types.h"
#include "nvwal_util.h"

nvwal_error_t nvwal_open_log_cursor(
  struct NvwalContext* wal,
  nvwal_epoch_t begin_epoch,
  nvwal_epoch_t end_epoch,
  struct NvwalLogCursor* out) {
  memset(out, 0, sizeof(*out));
  out->wal_ = wal;
  out->start_epoch_ = begin_epoch;
  out->end_epoch_ = end_epoch;

  if (nvwal_is_epoch_equal_or_after(begin_epoch, end_epoch)) {
    nvwal_output_warning("Warning: Inclusive begin_epoch is at or after"
      " exclusive end_epoch. This might be a misuse of cursor. No results.\n");
    return 0;
  }

  nvwal_error_t error_code = cursor_next_initial(out);
  if (error_code) {
    /* Immediately close it in this case. */
    nvwal_close_log_cursor(wal, out);
    return error_code;
  }

  return 0;
}

nvwal_error_t cursor_next_initial(struct NvwalLogCursor* cursor) {
  assert(kNvwalInvalidEpoch == cursor->current_epoch_);
  assert(cursor->cur_segment_disk_fd_ == 0);
  assert(cursor->cur_segment_data_ == 0);
  /* First call to next_epoch after opening the cursor */
  cursor->current_epoch_ = cursor->start_epoch_;
  NVWAL_CHECK_ERROR(cursor_fetch_epoch_metadata(cursor, cursor->start_epoch_));

  if (cursor->fetched_epochs_count_ == 0) {
    return 0;
  }

  const struct NvwalCursorEpochMetadata* meta = cursor->fetched_epochs_;
  NVWAL_CHECK_ERROR(cursor_open_segment(cursor, meta->start_dsid_));
  cursor->cur_offset_ = meta->start_offset_;
  if (meta->last_dsid_ != meta->start_dsid_) {
    cursor->cur_len_ = cursor->wal_->config_.segment_size_ - cursor->cur_offset_;
  } else {
    assert(meta->end_offset_ >= cursor->cur_offset_);
    cursor->cur_len_ = meta->end_offset_ - cursor->cur_offset_;
  }

  return 0;
}

nvwal_error_t cursor_open_segment(
  struct NvwalLogCursor* cursor,
  nvwal_dsid_t dsid) {
  assert(dsid != kNvwalInvalidDsid);
  struct NvwalContext* wal = cursor->wal_;

  NVWAL_CHECK_ERROR(cursor_close_cur_segment(cursor));

  /* retry loop for a (rare) change-while-lock case */
  while (1) {
    const nvwal_dsid_t synced_dsid =
      nvwal_atomic_load(&wal->nv_control_block_->fsyncer_progress_.last_synced_dsid_);
    if (synced_dsid != kNvwalInvalidDsid && dsid >= synced_dsid) {
      /* The segment is on disk! */
      char path[kNvwalMaxPathLength];
      nvwal_construct_disk_segment_path(wal, dsid, path);
      cursor->cur_segment_disk_fd_ = open(path, O_RDONLY, 0);
      assert(cursor->cur_segment_disk_fd_);
      if (cursor->cur_segment_disk_fd_ == -1) {
        assert(errno);
        return errno;
      }

      cursor->cur_segment_data_ = mmap(
        0,
        wal->config_.segment_size_,
        PROT_READ,
        MAP_SHARED,
        cursor->cur_segment_disk_fd_,
        0);
      if (cursor->cur_segment_data_ == MAP_FAILED) {
        assert(errno);
        return errno;
      }
    } else {
      /* The segment is still on NV */
      const uint32_t nv_segment_index = (dsid - 1U) % wal->segment_count_;
      struct NvwalLogSegment* nv_segment = wal->segments_ + nv_segment_index;

      /* Pin it. This might involve a wait */
      nvwal_pin_read_unconditional_lock(&nv_segment->nv_reader_pins_);

      const nvwal_dsid_t synced_dsid_after =
        nvwal_atomic_load(&wal->nv_control_block_->fsyncer_progress_.last_synced_dsid_);
      if (synced_dsid != synced_dsid_after) {
        /* Something has been synced while waiting. Are we affected? */
        if (nv_segment->dsid_ != dsid) {
          nvwal_pin_read_unlock(&nv_segment->nv_reader_pins_);
          continue;  /* retry */
        }
      }

      assert(nv_segment->dsid_ == dsid);
      cursor->cur_segment_data_ = nv_segment->nv_baseaddr_;
      cursor->cur_segment_from_nv_segment_ = 1;
    }

    break;
  }

  cursor->cur_segment_id_ = dsid;
  return 0;
}


nvwal_error_t cursor_close_cur_segment(
  struct NvwalLogCursor* cursor) {
  nvwal_error_t last_seen_error = 0;
  if (cursor->cur_segment_from_nv_segment_) {
    /* The segment is on NV. We just need to unpin it */
    assert(cursor->cur_segment_disk_fd_ == 0);
    cursor->cur_segment_data_ = 0;
    cursor->cur_segment_disk_fd_ = 0;
    /* unpin it */
    const uint32_t nv_segment_index
      = (cursor->cur_segment_id_ - 1U) % cursor->wal_->segment_count_;
    struct NvwalLogSegment* nv_segment = cursor->wal_->segments_ + nv_segment_index;
    nvwal_pin_read_unlock(&nv_segment->nv_reader_pins_);
    cursor->cur_segment_from_nv_segment_ = 0;
  } else {
    if (cursor->cur_segment_data_ && cursor->cur_segment_data_ != MAP_FAILED) {
      if (munmap(
          cursor->cur_segment_data_,
          cursor->wal_->config_.segment_size_) == -1) {
        last_seen_error = nvwal_stock_error_code(last_seen_error, errno);
      }
    }
    cursor->cur_segment_data_ = 0;

    if (cursor->cur_segment_disk_fd_ && cursor->cur_segment_disk_fd_ != -1) {
      if (close(cursor->cur_segment_disk_fd_) == -1) {
        last_seen_error = nvwal_stock_error_code(last_seen_error, errno);
      }
    }
    cursor->cur_segment_disk_fd_ = 0;
  }

  cursor->cur_segment_id_ = kNvwalInvalidDsid;
  return last_seen_error;
}

nvwal_error_t cursor_fetch_epoch_metadata(
  struct NvwalLogCursor* cursor,
  nvwal_epoch_t from_epoch) {
  assert(from_epoch != kNvwalInvalidEpoch);

  nvwal_epoch_t to_epoch = nvwal_add_epoch(from_epoch, kNvwalCursorEpochPrefetches);
  if (nvwal_is_epoch_after(to_epoch, cursor->end_epoch_)) {
    to_epoch = cursor->end_epoch_;
  }
  if (nvwal_is_epoch_after(to_epoch, cursor->wal_->durable_epoch_)) {
    to_epoch = nvwal_increment_epoch(cursor->wal_->durable_epoch_);
  }

  cursor->fetched_epochs_current_ = 0;
  cursor->fetched_epochs_count_ = 0;
  if (to_epoch == from_epoch) {
    return 0;
  }

  assert(nvwal_is_epoch_after(to_epoch, from_epoch));
  struct MdsEpochIterator mds_iterator;
  NVWAL_CHECK_ERROR(mds_epoch_iterator_init(
    cursor->wal_,
    from_epoch,
    to_epoch - 1U,  /* TODO FIXME this is a tentative code until MDS employs exclusive end */
    &mds_iterator));
  nvwal_epoch_t cur_epoch = from_epoch;  /* Mostly for sanity check */
  for (int i = 0; i < kNvwalCursorEpochPrefetches; ++i) {
    assert(!mds_epoch_iterator_done(&mds_iterator));

    assert(mds_iterator.epoch_metadata_->epoch_id_ == cur_epoch);
    cursor->fetched_epochs_[i].epoch_ = cur_epoch;
    cursor->fetched_epochs_[i].start_dsid_ = mds_iterator.epoch_metadata_->from_seg_id_;
    cursor->fetched_epochs_[i].last_dsid_ = mds_iterator.epoch_metadata_->to_seg_id_;
    cursor->fetched_epochs_[i].start_offset_ = mds_iterator.epoch_metadata_->from_offset_;
    cursor->fetched_epochs_[i].end_offset_ = mds_iterator.epoch_metadata_->to_off_;

    cur_epoch = nvwal_increment_epoch(cur_epoch);
    ++cursor->fetched_epochs_count_;
    mds_epoch_iterator_next(&mds_iterator);

    if (nvwal_is_epoch_equal_or_after(cur_epoch, to_epoch)) {
      break;
    }
  }
  assert(cur_epoch == to_epoch);
  assert(cursor->fetched_epochs_count_ <= kNvwalCursorEpochPrefetches);
  NVWAL_CHECK_ERROR(mds_epoch_iterator_destroy(&mds_iterator));
  return 0;
}


nvwal_error_t nvwal_close_log_cursor(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
  nvwal_error_t last_seen_error = cursor_close_cur_segment(cursor);
  memset(cursor, 0, sizeof(*cursor));
  return last_seen_error;
}

nvwal_error_t nvwal_cursor_next(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
  assert(nvwal_cursor_is_valid(wal, cursor));
  assert(cursor->fetched_epochs_current_ < cursor->fetched_epochs_count_);
  /*
   * Cases in order of likelihood.
   * 1) Completed the epoch. Moving on to next epoch within the segment.
   *   1a) Fetched epochs contain next epoch.
   *   1b) Need to fetch next epochs.
   * 2) The epoch has remaining data. Moving on to next segment.
   */
  const struct NvwalCursorEpochMetadata* meta
    = cursor->fetched_epochs_ + cursor->fetched_epochs_current_;
  assert(meta->epoch_ == cursor->current_epoch_);
  if (meta->last_dsid_ == cursor->cur_segment_id_) {
    /* Case 1) Move on to next epoch! */
    assert(meta->end_offset_ == cursor->cur_offset_ + cursor->cur_len_);
    cursor->current_epoch_ = nvwal_increment_epoch(cursor->current_epoch_);
    ++cursor->fetched_epochs_current_;

    if (cursor->fetched_epochs_current_ < cursor->fetched_epochs_count_) {
      /* Case 1a) Cheapest case, good! */
    } else {
      /* Case 1b) */
      NVWAL_CHECK_ERROR(cursor_fetch_epoch_metadata(cursor, cursor->current_epoch_));
      assert(cursor->fetched_epochs_current_ == 0);
      if (cursor->fetched_epochs_count_ == 0) {
        /* No more epochs. We've hit the end */
        NVWAL_CHECK_ERROR(cursor_close_cur_segment(cursor));
        return 0;
      }
    }

    meta = cursor->fetched_epochs_ + cursor->fetched_epochs_current_;
    assert(meta->epoch_ == cursor->current_epoch_);
    cursor->cur_offset_ = meta->start_offset_;
  } else {
    /* Case 2) This involves close/open of the segment file */
    NVWAL_CHECK_ERROR(cursor_open_segment(cursor, cursor->cur_segment_id_ + 1U));
    cursor->cur_offset_ = 0;
  }

  if (meta->last_dsid_ != cursor->cur_segment_id_) {
    cursor->cur_len_ = cursor->wal_->config_.segment_size_ - cursor->cur_offset_;
  } else {
    cursor->cur_len_ = meta->end_offset_ - cursor->cur_offset_;
  }

  return 0;
}
