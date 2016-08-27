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
#ifndef NVWAL_API_H_
#define NVWAL_API_H_
/**
 * @file nvwal_api.h
 * Function interface to a simple WAL facility optimized for NVDIMM.
 * @ingroup LIBNVWAL
 * @addtogroup LIBNVWAL
 * @{
 */

#include "nvwal_fwd.h"
#include "nvwal_types.h"

#ifdef __cplusplus
/* All API functions must be extern-C to be used from C and C++ */
extern "C" {
#endif  /* __cplusplus */

/**
 * DESCRIBE ME.
 * @param[in] config DESCRIBE ME
 * @param[out] wal DESCRIBE ME
 */
nvwal_error_t nvwal_init(
  const struct NvwalConfig* config,
  struct NvwalContext* wal);

/**
 * @brief Releases all resources this WAL instance had.
 * @param[in,out] wal WAL context to release
 * @return Returns any non-zero error code we observed while releaseing.
 * @details
 * This method tries to release as much resource as possible even in
 * catastrophic circumstance. It thus continues even after observing
 * some error. The return value is thus the last error we observed.
 * In case there are many issues happening, it might not be the
 * root cause.
 */
nvwal_error_t nvwal_uninit(
  struct NvwalContext* wal);

/**
 * @brief This must be invoked by the client application after nvwal_init()
 * to be the log flusher thread.
 * @param[in] wal The flusher will be the only flusher on this WAL instance
 * @details
 * Each WAL instance has exactly one flusher that is responsible for
 * monitoring log activities of each log writer, writing them out to NVDIMM,
 * and letting the writers when they can reclaim their buffers.
 * In order to achieve maximum flexibility as a library, libnvwal lets
 * the client application to launch a flusher thread and invoke this function
 * themselves, rather than launching it ourselves.
 * This allows libnvwal to be agnostic to threading model and threading library
 * used in the client application.
 *
 * @attention The calling thread will \b block until nvwal_uninit() is invoked,
 * or returns an error for whatever reason.
 *
 * @note (To be implemented) In NvwalConfig, there will be
 * an option for libnvwal itself to
 * launch a thread and invoke this method. In that case, the client application
 * must make sure that the program is linked against pthread.
 */
nvwal_error_t nvwal_flusher_main(
  struct NvwalContext* wal);


/**
 * @brief This must be invoked by the client application after nvwal_init()
 * to be the log fsync thread.
 * @param[in] wal The fsyncer will be the only such kind on this WAL instance
 * @details
 * Each WAL instance has exactly one fsyncer that is responsible for
 * invoking fsyncs on segments in parallel to flusher so that flusher
 * can maximize its use of time.
 * For the same reason as flusher, libnvwal lets
 * the client application to launch a fsync thread and invoke this function
 * themselves, rather than launching it ourselves.
 *
 * @attention The calling thread will \b block until nvwal_uninit() is invoked,
 * or returns an error for whatever reason.
 *
 * @note (To be implemented) In NvwalConfig, there will be
 * an option for libnvwal itself to
 * launch a thread and invoke this method. In that case, the client application
 * must make sure that the program is linked against pthread.
 */
nvwal_error_t nvwal_fsync_main(
  struct NvwalContext* wal);

/**
 * @brief Notifies libnvwal of a region of written logs in the given writer's log buffer.
 * @param[in] writer the writer that represents the calling thread itself
 * @param[in] bytes_written number of bytes written since the previous call
 * @param[in] log_epoch epoch of all log entries in the given region.
 * The log_epoch must not be too new (SE + 2 or larger).
 * Also, the given epoch must be increasing. Once you give 100, you can't give 99 or 98,
 * except wrap-around case.
 * @invariant log_epoch > stable epoch (SE)  ; otherwise the client application violated
 * the contract on advancing SE.
 *
 */
nvwal_error_t nvwal_on_wal_write(
  struct NvwalWriterContext* writer,
  uint64_t bytes_written,
  nvwal_epoch_t log_epoch);

/**
 * @returns.Whether the writer has plenty of space left in the buffer.
 * When this returns false, the caller in client application should
 * wait until it becomes true. It is left the client application
 * whether to sleep, spin, or do something in the meantime.
 */
uint8_t nvwal_has_enough_writer_space(
  struct NvwalWriterContext* writer);

/**
 * DESCRIBE ME.
 */
nvwal_error_t nvwal_query_durable_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t* out);

/**
 * @returns whether left > right in wrap-around-aware fashion
 * @see see http://en.wikipedia.org/wiki/Serial_number_arithmetic
 * @details
 * Do NOT use straightforward "left > right". We must be wrap-around-aware.
 * Equality is fine.
 */
static inline uint8_t nvwal_is_epoch_after(nvwal_epoch_t left, nvwal_epoch_t right) {
  /** see http://en.wikipedia.org/wiki/Serial_number_arithmetic */
  uint64_t diff = left - right;
  return (diff != 0) && (diff < (1ULL << 63));
}
/** @see nvwal_is_epoch_after */
static inline uint8_t nvwal_is_epoch_equal_or_after(nvwal_epoch_t left, nvwal_epoch_t right) {
  return left == right || nvwal_is_epoch_after(left, right);
}

/**
 * @returns The epoch next to the given epoch.
 * @details
 * Do NOT use straightforward "++epoch". We must skip kNvwalInvalidEpoch on wrap-around.
 */
static inline nvwal_epoch_t nvwal_increment_epoch(nvwal_epoch_t epoch) {
  nvwal_epoch_t ret = epoch + 1ULL;
  if (ret == kNvwalInvalidEpoch) {
    return ret + 1ULL;
  } else {
    return ret;
  }
}

/**
 * @brief Initializes the reader context.
 */
nvwal_error_t nvwal_reader_init(
  struct NvwalReaderContext* reader); 

/**
 * @brief Uninitializes the reader context.
 */
nvwal_error_t nvwal_reader_uninit(
  struct NvwalReaderContext* reader); 

/**
 * @brief Fetches the requested epoch for the application
 * @param[in] epoch The requested epoch
 * @param[out] buf Pointer to epoch data
 * @param[out] len Length of buf
 *
 */
nvwal_error_t get_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t const epoch, 
  char ** buf, 
  uint64_t* len);

/**
 * @brief Notifies the reader that the epoch has been processed
 * and is no longer needed.
 * @param[in] epoch The requested epoch
 *
 */
nvwal_error_t consumed_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t const epoch);

#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif  /* NVWAL_API_H_ */
