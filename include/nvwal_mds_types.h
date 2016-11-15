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

#ifndef NVWAL_MDS_TYPES_H_
#define NVWAL_MDS_TYPES_H_
/**
 * @file mds.h
 * Provides typedefs/enums/structs used in mds (meta-data store).
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include "nvwal_fwd.h"
#include "nvwal_types.h"


/**
 * @brief Represents metadata associated with an epoch stored in the metadata store. 
 * 
 * @note This POD must equal to the NV-DIMM failure-atomic unit size, which
 * is a single cache line.
 */
struct MdsEpochMetadata {
  union {
    struct {
      nvwal_epoch_t epoch_id_;        /**< epoch identifier */
      nvwal_dsid_t  from_seg_id_;
      uint32_t      from_offset_; 
      nvwal_dsid_t  to_seg_id_;
      uint32_t      to_off_;
      uint64_t      user_metadata_0_; /**< user defined metadata word 0 */
      uint64_t      user_metadata_1_; /**< user defined metadata word 1 */
    };
    uint64_t        u64_[8];
  };
};

struct MdsEpochIteratorBuffer {
  struct MdsEpochMetadata epoch_metadata_[kNvwalMdsReadPrefetch];
  int num_entries_;
};

struct MdsEpochIterator {
  struct NvwalContext* wal_;
  nvwal_epoch_t cur_epoch_id_;  /**< Current epoch */
  nvwal_epoch_t begin_epoch_id_; /**< First epoch in the iterator range */
  nvwal_epoch_t end_epoch_id_; /**< Last epoch in the iterator range */
  struct MdsEpochIteratorBuffer buffer_; /**< Buffer holding prefetced entries */
  struct MdsEpochMetadata* epoch_metadata_; /**< Pointer to the current epoch */
};


/** @} */

#endif /* NVWAL_MDS_H */
