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

#ifndef NVWAL_IMPL_MDS_H_
#define NVWAL_IMPL_MDS_H_
/**
 * @file nvwal_impl_mds.h
 * Function interface typedefs/enums/structs for internal use by the metadata 
 * store module implementation and tests.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include "nvwal_mds_types.h"

#ifdef __cplusplus
/* All interface functions must be extern-C to be used from C and C++ */
extern "C" {
#endif  /* __cplusplus */



/******************************************************************************
 * Declarations for private typedefs/enums/structs
 *****************************************************************************/

typedef uint64_t page_offset_t;
typedef mds_page_no_t page_no_t;
typedef mds_file_no_t file_no_t;
typedef uint64_t file_no_t;

/**
 * @brief Represents a page containing epoch metadata.
 */
struct Page {
  struct MdsEpochMetadata epochs_[0];
};

/******************************************************************************
 * Definitions for inline private functions
 *****************************************************************************/

/**
 * @brief Normalize epoch id for index arithmetic operations.
 * 
 * @details
 * As epoch 0 is an invalid epoch (kNvwalInvalidEpoch == 0), so epochs
 * start at 1. We therefore subtract 1 one simplify arithmetic operations.
 */
static inline nvwal_epoch_t normalize_epoch_id(nvwal_epoch_t epoch_id)
{
  static_assert(kNvwalInvalidEpoch == 0, "Invalid epoch expected to be 0 but is not.");
  return epoch_id - 1;
}

/**
 * @brief Returns the maximum number of epochs per page
 */
static inline int max_epochs_per_page(struct NvwalMdsContext* mds)
{ 
  return mds->wal_->config_.mds_page_size_ / sizeof(struct MdsEpochMetadata);
}

/**
 * @brief Returns the file number of the page file storing metadata for 
 * epoch \a epoch_id.
 * 
 * @details
 * To increase write parallelism to the disk, we maintain multiple page files
 * and stripe epoch pages evenly across page files.
 */
static inline file_no_t epoch_id_to_file_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  uint64_t page_offset = normalize_epoch_id(epoch_id) / max_epochs_per_page(mds);
  return page_offset % kNvwalMdsMaxPagefiles;
}

/**
 * @brief Return the page number of the page storing metadata for 
 * epoch \a epoch_id.
 */
static inline page_no_t epoch_id_to_page_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  assert(epoch_id != kNvwalInvalidEpoch);
  page_no_t page_no = normalize_epoch_id(epoch_id) / (max_epochs_per_page(mds) * kNvwalMdsMaxPagefiles);
  return page_no;
}

/**
 * @brief Return the record offset relative to the page 
 */
static inline page_offset_t epoch_id_to_page_offset(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  return normalize_epoch_id(epoch_id) % max_epochs_per_page(mds);
}

/**
 * @brief Return the byte offset relative to the file 
 */
static inline off_t epoch_id_to_file_offset(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  return normalize_epoch_id(epoch_id) * sizeof(struct MdsEpochMetadata);
}



/******************************************************************************
 * Interface for private functions
 *****************************************************************************/

/**
 * @brief Initializes the I/O subsystem of the meta-data store.
 * 
 * @param[in] mode Specifies whether we just restart or newly create
 * @param[in] wal nvwal instance context
 * @param[out] did_restart Indicates whether subsystem restarted successfully.
 * 
 * @details
 * Opens metadata page files. If the page files do not exist, it creates them. 
 */
nvwal_error_t mds_io_init(
  enum NvwalInitMode mode, 
  struct NvwalContext* wal, 
  int* did_restart);

/**
 * @brief Unitializes the I/O subsystem of the meta-data store.
 */
nvwal_error_t mds_io_uninit(struct NvwalContext* wal);

/**
 * @brief Opens a page file and provides a page-file descriptor for this file.
 */
nvwal_error_t mds_io_open_file(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no);

/**
 * @brief Creates a page file and provides a page-file descriptor for this file.
 */
nvwal_error_t mds_io_create_file(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no);

/**
 * @brief Closes a page file.
 * 
 * @details
 * Deallocates the memory associated with the page file descriptor.
 */
void mds_io_close_file(
  struct NvwalMdsIoContext* io,
  file_no_t file_no);

/**
 * @brief Returns the page-file descriptor to a given page file.
 */
struct NvwalMdsPageFile* mds_io_file(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no);

/**
 * @brief Atomically appends to a page file. 
 *
 * @details
 * Since a single append might require multiple write calls, the file 
 * system cannot guarantee that the whole append is atomic. However, 
 * with most sane journaled file systems, one can infer the amount of 
 * data written to the file based on the file size. So upon recovery, 
 * we can infer whether the last append was successful by checking 
 * whether the file size is multiple of page size.
 *  
 */
nvwal_error_t mds_io_append_page(
  struct NvwalMdsPageFile* file,
  const void* buf);

/**
 * @brief Initializes the buffer manager of the meta-data store.
 *
 * @param[in] mode Specifies whether we just restart or newly create
 * @param[in] wal nvwal instance context
 * @param[out] did_restart Indicates whether subsystem restarted successfully.
 * 
 * @details
 * As part of the initialization, the buffer manager remaps any NVRAM
 * buffers. However, the user is still responsible to assign NVRAM
 * buffers to the proper page file based on the recovery protocol
 * followed by the user. 
 */
nvwal_error_t mds_bufmgr_init(
  enum NvwalInitMode mode, 
  struct NvwalContext* wal,
  int* did_restart);


/**
 * @brief Unitializes the buffer manager.
 */
nvwal_error_t mds_bufmgr_uninit(
  struct NvwalContext* wal);

#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif /* NVWAL_IMPL_MDS_H_ */
