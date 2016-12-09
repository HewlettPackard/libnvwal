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
 * @returns the version of this libnvwal binary.
 * Whenever we [might] break compatibility of file formarts etc,
 * we bump this up.
 */
uint64_t nvwal_get_version();

/**
 * @brief Initializes a WAL instance.
 * @param[in] config the configurations for this WAL instance.
 * This object must \b NOT be pointing to wal->config.
 * @param[in] mode Specifies whether we just restart or newly create, in a destructive fashion,
 * etc. If you give 0, it just restarts from an existring folder.
 * @param[out] out_wal The WAL instance to initialize.
 * @details
 * To use libnvwal, you must first call this API to initialize
 * your WAL instance.
 * Followings are the typical procedure to use libnvwal.
 * @code{.c}
 * struct NvwalConfig my_config;
 * ... Put configuration values onto my_config
 * struct NvwalContext my_wal;  // for ease of read. we recommend this to be on heap
 * if (nvwal_init(&my_config, &my_wal)) {
 *    ... some error happened! check errno
 * }
 * ... Launch a thread for flusher and fsyncer. see nvwal_fsync_main/nvwal_flusher_main.
 * struct NvwalWriterContext* my_writer = &my_wal.writers_[writer_index];
 * ... Write out logs for my_writer
 * nvwal_uninit(&my_wal)
 * @endcode
 * @return Error code if initialization fails. Iff the initialization fails,
 * you don't have to call nvwal_uninit().
 */
nvwal_error_t nvwal_init(
  const struct NvwalConfig* config,
  enum NvwalInitMode mode,
  struct NvwalContext* out_wal);

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
 * @attention Make sure you call nvwal_uninit() after nvwal_init().
 * Otherwise it cannot release the resources or do a clean shutup.
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
 *
 * Below is a typical code in C++11 (for easier read) to invoke this method.
 * Note, error-code check and a few other good-practices are omitted below
 * for easier read.
 * @code{.cpp}
 * struct NvwalContext* const wal = ...;
 * std::thread flusher([wal](){ nvwal_flusher_main(wal); }
 * std::thread fsyncer([wal](){ nvwal_fsync_main(wal); }
 * nvwal_wait_for_flusher_start(wal);
 * nvwal_wait_for_fsync_start(wal);
 * ...
 * @endcode
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
 * Wait until the flusher thread joins the context and starts working.
 * The client application can \e optionally invoke this method
 * after nvwal_init() and launching its own threads to call nvwal_flusher_main().
 * This method exists because occassionally the flusher thread might take long
 * time to be launched, giving some surprise. For example worker threads started
 * running but flusher hasn't been doing its job, or furthermore the main
 * thread exits before flusher thread even starts.
 * By invoking this method, the client application can make sure the flusher thread
 * has already started and joined.
 * Probably it's a good habit to call this, but not mandatory.
 */
void nvwal_wait_for_flusher_start(
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
/** Same as nvwal_wait_for_flusher_start() except it's for fsyncer */
void nvwal_wait_for_fsync_start(
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
 * @brief Assigns user defined metadata to a given epoch.
 * @param[in] writer the writer that represents the calling thread itself
 * @param[in] log_epoch epoch to assign metadata to
 * @param[in] user_metadata_0 user defined metadata word 0
 * @param[in] user_metadata_1 user defined metadata word 1
 * Same constraints and invariants apply as nvwal_on_wal_write.
 * Overwrites any previously assigned metadata, and assigned metadata 
 * becomes durable when epoch becomes durable.
 * When multiple writers concurrently assign metadata, then this constitutes
 * a data race and the outcome is undefined.
 */
nvwal_error_t nvwal_tag_epoch(
  struct NvwalWriterContext* writer,
  nvwal_epoch_t log_epoch,
  uint64_t user_metadata_0,
  uint64_t user_metadata_1);

/**
 * @returns.Whether the writer has plenty of space left in the buffer.
 * When this returns false, the caller in client application should
 * wait until it becomes true. It is left the client application
 * whether to sleep, spin, or do something in the meantime.
 */
uint8_t nvwal_has_enough_writer_space(
  struct NvwalWriterContext* writer);

/**
 * @returns Current Durable Epoch (DE) of this WAL instance.
 * @see nvwal_epoch_t
 */
nvwal_error_t nvwal_query_durable_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t* out);

/**
 * @brief Let libnvwal know that no new logs in current DE+1 will be
 * written any longer, which allows libnvwal to advance DE.
 * @param[in] new_stable_epoch SE, which should be current DE+1
 * @details
 * After calling this method, one can wait until
 * nvwal_query_durable_epoch() returns the stable epoch returned by
 * this method, which should happen shortly as far as libnvwal's flusher
 * is well catching up.
 * You can invoke this method from an arbitrary number of threads,
 * but there is no effect unless the given new_stable_epoch is exactly
 * current DE+1.
 */
nvwal_error_t nvwal_advance_stable_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t new_stable_epoch);

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
 * @returns epoch+operand in wrap-around-aware fashion.
 * @details
 * Do NOT use straightforward "epoch+=". We must skip kNvwalInvalidEpoch on wrap-around.
 * This method is equivalent to call nvwal_increment_epoch() operand times.
 */
static inline nvwal_epoch_t nvwal_add_epoch(nvwal_epoch_t epoch, uint64_t operand) {
  nvwal_epoch_t ret = epoch + operand;
  if (ret < epoch) {
    /* Wrapped around */
    return ret + 1ULL;
  } else {
    return ret;
  }
}

/**
 * @brief Opens a cursor to read durable logs and makes it ready to return
 * logs in the first epoch.
 * @param[in] wal WAL stream to read from
 * @param[in] being_epoch \b Inclusive beginning of the epochs to read
 * @param[in] end_epoch \b Exclusive ending of the epochs to read
 * @param[out] cursor Cursor object to initialize.
 * @return Any error on opening the cursor or reading the first epoch.
 * Iff this returns a non-zero error, the caller does not have to call
 * nvwal_close_log_cursor().
 * @details
 * To read durable logs, open and iterate over a cursor like the following.
 * Remember, you must have error handlings.
 * OEH (Omitted Error Handling) just for ease of read!
 * @code{.c}
 * struct NvwalLogCursor cursor;
 * nvwal_open_log_cursor(wal, 123, 234, &cursor);  .. OEH
 * while (nvwal_cursor_is_valid(wal, &cursor)) {
 *   nvwal_epoch_t epoch = nvwal_cursor_get_current_epoch(wal, &cursor);
 *   const nvwal_byte_t* data = nvwal_cursor_get_data(wal, &cursor);
 *   const uint64_t len = nvwal_cursor_get_data_length(wal, &cursor);
 *   ... (process the logs!)
 *   nvwal_cursor_next(wal, &cursor);  .. OEH
 * }
 * nvwal_close_log_cursor(wal, &cursor);  .. OEH
 * @endcode
 */
nvwal_error_t nvwal_open_log_cursor(
  struct NvwalContext* wal, 
  nvwal_epoch_t begin_epoch,
  nvwal_epoch_t end_epoch,
  struct NvwalLogCursor* cursor);

/**
 * Release all resources acquired for the log cursor.
 * This method is guaranteed to be idempotent.
 * You can call this method many times, but not from multiple threads.
 * @return Returns any non-zero error code we observed while releaseing.
 */
nvwal_error_t nvwal_close_log_cursor(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor);

/**
 * @brief Makes the next epoch accessible through the cursor.
 * @pre nvwal_cursor_is_valid(wal, cursor)
 * @details
 * After calling this method, the cursor may or may not be in
 * another epoch.
 * In the case that an epoch was broken into multiple mappings,
 * the cursor is updated to make the next mapping accessible,
 * but current_epoch is not updated.
 * The name of this method is thus \b not next_epoch but just next.
 */
nvwal_error_t nvwal_cursor_next(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor);

/** @return whether the cursor is ready to return log entries in some epoch */
static inline uint8_t nvwal_cursor_is_valid(
  struct NvwalContext* wal,
  struct NvwalLogCursor* cursor) {
  return (cursor->cur_segment_data_ != 0);
}

/**
 * @return VA-mapping for the cursor's current data
 * @pre nvwal_cursor_is_valid(wal, cursor)
 * @note Please call this method after \e each nvwal_cursor_next().
 * It might be the same virtual address, might be \b not.
 */
static inline const nvwal_byte_t* nvwal_cursor_get_data(
  struct NvwalContext* wal,
  const struct NvwalLogCursor* cursor) {
  return cursor->cur_segment_data_ + cursor->cur_offset_;
}

/**
 * @return byte length of the cursor's current data
 * @pre nvwal_cursor_is_valid(wal, cursor)
 */
static inline uint64_t nvwal_cursor_get_data_length(
  struct NvwalContext* wal,
  const struct NvwalLogCursor* cursor) {
  return cursor->cur_len_;
}

/**
 * @return epoch from which this cursor is now returning log entries
 * @pre nvwal_cursor_is_valid(wal, cursor)
 * @details
 * Our cursor (logically) returns log entries in only one epoch even when
 * it has only a few bytes. This method thus always has a single epoch to return.
 * Even in the aforementioned case, don't worry about iteration overhead;
 * we internally map a file covering multiple epochs, and entail almost
 * nothing to move on to next epoch in that case.
 */
static inline nvwal_epoch_t nvwal_cursor_get_current_epoch(
  struct NvwalContext* wal,
  const struct NvwalLogCursor* cursor) {
  return cursor->current_epoch_;
}

/**
 * @brief Find the lowest epoch tagged with user-defined metadata for which 
 * the given predicate is true.
 * @param[in] wal WAL stream to query
 * @param[in] user_metadata_id metadata identifier to query
 * @param[in] predicate 
 */
nvwal_epoch_t nvwal_query_epoch_lower_bound(
  struct NvwalContext* wal,
  int user_metadata_id,
  struct NvwalPredicateClosure* predicate);

/**
 * @brief Find the greatest epoch tagged with user-defined metadata for which 
 * the given predicate is true.
 * @param[in] wal WAL stream to query
 * @param[in] user_metadata_id metadata identifier to query
 * @param[in] predicate 
 */
nvwal_epoch_t nvwal_query_epoch_upper_bound(
  struct NvwalContext* wal,
  int user_metadata_id,
  struct NvwalPredicateClosure* predicate);

/**
 * @brief Return the user-defined metadata associated with a given epoch.
 * @param[in] wal WAL stream to query
 * @param[in] epoch the epoch to return metadata for
 * @param[out] user_metadata_0 the returned metadata word 0
 * @param[out] user_metadata_1 the returned metadata word 1
 */
nvwal_error_t nvwal_epoch_metadata(
  struct NvwalContext* wal,
  nvwal_epoch_t epoch,
  uint64_t* user_metadata_0,
  uint64_t* user_metadata_1);


#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif  /* NVWAL_API_H_ */
