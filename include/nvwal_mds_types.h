/* 
 * Copyright 2017 Hewlett Packard Enterprise Development LP
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met:
 * 
 *   1. Redistributions of source code must retain the above copyright 
 *      notice, this list of conditions and the following disclaimer.
 *
 *   2. Redistributions in binary form must reproduce the above copyright 
 *      notice, this list of conditions and the following disclaimer 
 *      in the documentation and/or other materials provided with the 
 *      distribution.
 *   
 *   3. Neither the name of the copyright holder nor the names of its 
 *      contributors may be used to endorse or promote products derived 
 *      from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
