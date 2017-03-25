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

/**
 * Find the lowest epoch tagged with user-defined metadata for which 
 * the given predicate is true.
 */
nvwal_error_t mds_find_metadata_lower_bound(
  struct NvwalContext* wal,
  int user_metadata_id,
  struct NvwalPredicateClosure* predicate,
  struct MdsEpochMetadata* out);

/**
 * Find the greatest epoch tagged with user-defined metadata for which
 * the given predicate is true.
 */
nvwal_error_t mds_find_metadata_upper_bound(
  struct NvwalContext* wal,
  int user_metadata_id,
  struct NvwalPredicateClosure* predicate,
  struct MdsEpochMetadata* out);


#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif /* NVWAL_MDS_H */
