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

//FIXME: do we prefix all header files with nvwal_, regardless whether header is private or public?

#ifndef NVWAL_MDS_H_
#define NVWAL_MDS_H_
/**
 * @file mds.h
 * Provides typedefs/enums/structs used in mds (meta-data store).
 * @ingroup LIBNVWAL
 * @addtogroup LIBNVWAL
 * @{
 */

/*************************************************************************** 
 * Forward declarations for private types.
 ***************************************************************************/ 
struct BufferManager;


/*************************************************************************** 
 * Type declarations for public types.
 ***************************************************************************/ 


/**
 * @brief Represents a context of a meta-data store.
 */
struct MdsContext {
  /** File descriptor to the disk directory where we keep metadata page files */
  int    block_root_fd_;          
  struct BufferManager* bfmgr_;   /**< buffer manager */
};


/**
 * @brief Represents metadata associated with an epoch. 
 * 
 * @note This POD must equal to the NV-DIMM failure-atomic unit size, which
 * is a single cache line.
 */
struct EpochMetadata {
  union {
    struct {
      nvwal_epoch_t eid_;           /**< epoch identifier */
      uint64_t      from_seg_id_;
      uint32_t      from_offset_; 
      uint64_t      to_seg_id_;
      uint32_t      to_off_;
    };
    uint64_t        u64_[8];
  };
};

_Static_assert(sizeof(struct EpochMetadata) == 64, "Epoch metadata must match NV-DIMM failure-atomic unit size");


/*************************************************************************** 
 * Method declarations for public API.
 ***************************************************************************/ 


/**
 * @brief Initializes the metadata store.
 *
 * @param[in] config runtime configuration parameters
 * @param[out] mds metadata store context 
 */
nvwal_error_t mds_init(
  const struct NvwalConfig* config, 
  struct MdsContext* mds);


/**
 * @brief Uninitializes the metadata store.
 *
 * @param[in] mds metadata store context 
 */
nvwal_error_t mds_uninit(struct MdsContext* mds);


/** @} */

#endif /* NVWAL_MDS_H */
