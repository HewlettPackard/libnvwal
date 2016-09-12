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
#include "nvwal_mds.h"
#include "nvwal_util.h"
#include "nvwal_mds_types.h"

nvwal_error_t cursor_get_epoch(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor,
  const struct MdsEpochMetadata* target_epoch_meta) {

  struct NvwalEpochMapMetadata* epoch_map = cursor->read_metadata_ + cursor->free_map_;
  memset(epoch_map, 0, sizeof(struct NvwalEpochMapMetadata));
  struct MdsEpochMetadata epoch_meta = *target_epoch_meta;
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
    if (wal->config_.segment_size_ == cursor->read_metadata_[prev_map].seg_end_offset_)
    {
      /* we mapped to the end of the segment last time. start at beginning of the next one */
      epoch_map->seg_id_start_++;
      epoch_map->seg_start_offset_ = 0;
     /* While it won't happen that we get a map fail in the middle of a segment while mapping an epoch,
      * we can get a map fail inside of mapping a segment because multiple epochs are inside the segment.
      */
    }
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
  epoch_map->seg_end_offset_ = epoch_map->seg_start_offset_;
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
    char backing_path[kNvwalMaxPathLength];
    if (0) /*FIXME*/
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
        wal->config_.disk_root_,
        "nvwal_ds",
        segment_id,
        backing_path);
    }

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
        cursor->current_map_ = cursor->free_map_;
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
        if (cursor->current_epoch_ == target_epoch_meta->epoch_id_)
        {
          cursor->fetch_complete_ = 0;
        } else
        {
          //we were prefetching some future epoch */
          cursor->prefetch_complete_ = 0;
        }
        cursor->current_map_ = cursor->free_map_;
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
    if (epoch_map->seg_id_end_ == segment_id)
    {
      /* we just mapped more of the same segment */
      epoch_map->seg_end_offset_ += map_len;
    } else
    {
      epoch_map->seg_end_offset_ = map_len;
    }
    epoch_map->seg_id_end_ = segment_id;
    segment_id++;

  } while (segment_id <= epoch_meta.to_seg_id_);
  // We finished fetching the first epoch/an epoch. Keep trying to extend this mapping.
    if (cursor->current_epoch_ == target_epoch_meta->epoch_id_)
    {
      cursor->fetch_complete_ = 1;
    } else
    {
      cursor->prefetch_complete_ = 1;
    }
    epoch_to_map++;
    //mds_read_epoch(wal->mds, epoch_to_map, &epoch_meta); //need to catch a return code
    segment_id = epoch_meta.from_seg_id_;
  } while (epoch_to_map <= cursor->end_epoch_);

  cursor->current_map_ = cursor->free_map_;
  cursor->free_map_++;
  if (cursor->free_map_ >= kNvwalNumReadRegions)
  {
    cursor->free_map_ = 0;
  }

  return error_code; /* no error */
}

nvwal_error_t consumed_map(
  struct NvwalLogCursor* cursor,
  struct NvwalEpochMapMetadata* epoch_map)
{
  nvwal_error_t error_code = 0;

  if (NULL == epoch_map->mmap_start_)
  {
    /* This isn't an active mapping... */
   return error_code; /* FIXME */
  }

  munmap(epoch_map->mmap_start_, epoch_map->mmap_len_);
  epoch_map->mmap_start_ = NULL;
  epoch_map->mmap_len_ = 0;


  /* Don't forget to unpin the segments that are in nvdimm */
  nvwal_dsid_t segment_id = epoch_map->seg_id_start_;

  do
  {

    /* Is it on NVDIMM or disk? */
    /* Is this the only epoch in this segment or do we need it for
     * subsequent epoch mapping? */
    /* Is another reader also using this segment */
    /* Atomically mark the segment as free or some quiesced state, if it's in NVDIMM */

    segment_id++;

  } while (segment_id <= epoch_map->seg_id_end_);


  return error_code; /* no error */
}

/* sub routine of nvwal_open_log_cursor */
nvwal_error_t cursor_next_initial(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor);

nvwal_error_t nvwal_open_log_cursor(
  struct NvwalContext* wal,
  nvwal_epoch_t begin_epoch,
  nvwal_epoch_t end_epoch,
  struct NvwalLogCursor* out) {
  memset(out, 0, sizeof(*out));

  out->current_epoch_ = kNvwalInvalidEpoch;
  out->fetch_complete_ = 1;
  out->data_ = NULL;
  out->data_len_ = 0;
  out->start_epoch_ = begin_epoch;
  out->end_epoch_ = end_epoch;
  out->free_map_ = 0;
  out->current_map_ = 0;
  for (int i = 0; i < kNvwalNumReadRegions; i++) {
    out->read_metadata_[i].seg_id_start_ = kNvwalInvalidDsid;
    out->read_metadata_[i].seg_id_end_ = kNvwalInvalidDsid;
    out->read_metadata_[i].seg_start_offset_ = 0;
    out->read_metadata_[i].seg_end_offset_ = 0;
    out->read_metadata_[i].mmap_start_ = NULL;
    out->read_metadata_[i].mmap_len_ = 0;
  }

  nvwal_error_t error_code = cursor_next_initial(wal, out);
  if (error_code) {
    /* Immediately close it in this case. */
    nvwal_close_log_cursor(wal, out);
    return error_code;
  }

  return 0;
}

nvwal_error_t nvwal_close_log_cursor(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
  for (int i = 0; i < kNvwalNumReadRegions; i++) {
    consumed_map(cursor, &(cursor->read_metadata_[i]));
  }

  memset(cursor, 0, sizeof(*cursor));
  return 0;
}

/* @brief Looks for the desired epoch (epoch_meta) in the rest of the mmapped region.
 * Updates the current_epoch, data_, and data_len_ in cursor, if found.
 */
nvwal_error_t get_prefetched_epoch(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor,
  struct NvwalEpochMapMetadata* epoch_map,
  struct MdsEpochMetadata* epoch_meta)
{

  /* If we have part of the epoch in epoch_map, we have the following cases:
   * case 1: epoch_map contains the start of the epoch but not the end,
   * case 2: epoch map_contains that start and end of the epoch,
   * case 3: epoch_map contains the middle of the epoch (neither start nor end),
   * case 4: epoch_map contains the end of the epoch.
   */
  /*if map valid, look at read_metadata[current_map] and return if found */
  if (NULL == epoch_map->mmap_start_)
  {
    return 1;
  }
    nvwal_error_t error_code = 0;
    uint64_t logical_map_start = epoch_map->seg_id_start_*wal->config_.segment_size_
                                 + epoch_map->seg_start_offset_;
    uint64_t logical_map_end = epoch_map->seg_id_end_*wal->config_.segment_size_
                               + epoch_map->seg_end_offset_;
    uint64_t logical_epoch_start = epoch_meta->from_seg_id_*wal->config_.segment_size_
                                   + epoch_meta->from_offset_;
    uint64_t logical_epoch_end = epoch_meta->to_seg_id_*wal->config_.segment_size_
                                 + epoch_meta->to_off_;

    if (cursor->fetch_complete_)
    {
      /* We are looking for the beginning of epoch_meta.epoch_id_ (current_epoch+1)*/
      if (logical_epoch_start >= logical_map_end)
      {
        /* It's beyond the end of the this mapping.
         * We are done with this mapping so clean it up here. */
        consumed_map(cursor, epoch_map);
        return 1;
      }

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
    cursor->current_epoch_ = epoch_meta->epoch_id_;
    return error_code;
}

nvwal_error_t cursor_next_initial(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
  assert(kNvwalInvalidEpoch == cursor->current_epoch_);
  /* First call to next_epoch after opening the cursor */
  cursor->current_epoch_ = cursor->start_epoch_;
  //mds_read_epoch(wal->mds, cursor->current_epoch_, &epoch_meta); //need to catch a return code
  struct MdsEpochMetadata epoch_meta;
  NVWAL_CHECK_ERROR(cursor_get_epoch(wal, cursor, &epoch_meta));
  cursor->data_ = cursor->read_metadata_[cursor->current_map_].mmap_start_;
  cursor->data_len_ = cursor->read_metadata_[cursor->current_map_].mmap_len_;
  return 0;
}

nvwal_error_t nvwal_cursor_next(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {

  nvwal_error_t error_code = 0;

  struct MdsEpochMetadata epoch_meta;
  assert(kNvwalInvalidEpoch != cursor->current_epoch_);

  /* Lookup the epoch info from the MDS */
  if (cursor->fetch_complete_) {
    //are we at the end of the desired epoch range? if fetch_complete_, then this is a mistaken call.
    //We can unmap cursor->current_epoch_ now or just unmap the entire mapping later.
    //mds_read_epoch(wal->mds, cursor->current_epoch_ + 1, &epoch_meta); //need to catch a return code
  } else {
    //mds_read_epoch(wal->mds, cursor->current_epoch_, &epoch_meta); //need to catch a return code
    /* If we are calling again and we didn't complete the fetch of current_epoch_, we must
     * have consumed all of read_metadata[current_map]*/
    goto fetch_more;
  }

  /* Is at least part of the desired epoch already fetched? */
  struct NvwalEpochMapMetadata* epoch_map = cursor->read_metadata_ + cursor->current_map_;
  error_code = get_prefetched_epoch(wal, cursor, epoch_map, &epoch_meta);
  if (0 == error_code)
  {
    return error_code;
  }

fetch_more:
  {
    /* else go fetch it (and possibly more). */
    /* don't unmap our current map here. we need to see how much progress we made
     * on the epoch we only partially mapped. */
    error_code = cursor_get_epoch(wal, cursor, &epoch_meta);
    cursor->data_ = cursor->read_metadata_[cursor->current_map_].mmap_start_;
    cursor->data_len_ = cursor->read_metadata_[cursor->current_map_].mmap_len_;
  }

  return error_code;

}
