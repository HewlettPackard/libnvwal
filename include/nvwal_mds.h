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

#ifndef NVWAL_MDS_H_
#define NVWAL_MDS_H_
/**
 * @file nvwal_mds.h
 * Function interface to a minimal metadata store for internal use by the 
 * core nvwal library. 
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include "nvwal_fwd.h"
#include "nvwal_types.h"
#include "nvwal_mds_types.h"

#ifdef __cplusplus
/* All interface functions must be extern-C to be used from C and C++ */
extern "C" {
#endif  /* __cplusplus */

struct MdsEpochIterator;
struct MdsEpochMetadata;

/**
 * @brief Initializes the metadata store.
 *
 * @param[in] config runtime configuration parameters
 * @param[out] wal nvwal context 
 *
 * @notes
 * This function just does some basic initialization. It does not recover 
 * any durable state. Instead, the user has to call a separate recover 
 * function to recover such state.
 *
 */
nvwal_error_t mds_init(
  enum NvwalInitMode mode,
  struct NvwalContext* wal);

/**
 * @brief Uninitializes the metadata store.
 *
 * @param[in] wal WAL context instance.
 */
nvwal_error_t mds_uninit(struct NvwalContext* wal);

/**
 * @brief Writes epoch \a epoch_metadata.
 *
 * @param[in] wal WAL context instance.
 * @param[in] epoch_metadata Epoch metadata to write.
 * 
 * @details
 * When the function returns successfully, the epoch metadata is 
 * guaranteed to be durable.
 */
nvwal_error_t mds_write_epoch(
  struct NvwalContext* wal, 
  struct MdsEpochMetadata* epoch_metadata);

/**
 * @brief Returns identifier of latest durable epoch. 
 *
 * @param[in] wal WAL context instance.
 */
nvwal_epoch_t mds_latest_epoch(struct NvwalContext* wal);

/**
 * @brief Initializes an iterator instance to iterate over a given 
 * range of epoch metadata.
 *
 * @param[in] wal WAL context instance.
 * @param[in] begin_epoch_id Inclusive beginning of the epoch range to read.
 * @param[in] end_epoch_id Exclusive ending of the epoch range to read.
 * @param[out] iterator The iterator instance to initialize.
 * 
 * @details
 * The iterator member field epoch_metadata_ points to the current 
 * epoch metadata.
 */
nvwal_error_t mds_epoch_iterator_init(
  struct NvwalContext* wal, 
  nvwal_epoch_t begin_epoch_id, 
  nvwal_epoch_t end_epoch_id,
  struct MdsEpochIterator* iterator);

/**
 * @brief Advances iterator to the next epoch.
 *
 * @param[in] iterator The iterator instance to advance.
 */
void mds_epoch_iterator_next(struct MdsEpochIterator* iterator);

/**
 * @brief Checks if iterator is passed the end of the epoch range.
 * 
 * @param[in] iterator The iterator instance to check.
 */
int mds_epoch_iterator_done(struct MdsEpochIterator* iterator);

/**
 * @brief Destroys an iterator instance.
 *
 * @param[in] iterator The iterator instance to destroy.
 */
nvwal_error_t mds_epoch_iterator_destroy(struct MdsEpochIterator* iterator);

/**
 * @brief A convenience function to return a single epoch.
 * @pre epoch_id != kNvwalInvalidEpoch
 * @details
 * Just a thin wrapper over epoch iterator.
 * Not optimized for the performance, so use it where perforamnce is not an issue.
 */
nvwal_error_t mds_read_one_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t epoch_id,
  struct MdsEpochMetadata* out);

/**
 * @brief Truncates the metadata log so that the last epoch stored in the log 
 * matches given \a epoch.
 * 
 * @param[in] epoch Epoch to roll back to.
 */
nvwal_error_t mds_rollback_to_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t epoch);

#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif /* NVWAL_MDS_H */
