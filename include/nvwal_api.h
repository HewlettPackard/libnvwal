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

/**
 * DESCRIBE ME.
 * @param[in] config DESCRIBE ME
 * @param[out] wal DESCRIBE ME
 */
nvwal_error_t nvwal_init(
  const struct nvwal_config* config,
  struct nvwal_context* wal);

/**
 * DESCRIBE ME.
 * @param[in] wal DESCRIBE ME
 */
nvwal_error_t nvwal_uninit(
  struct nvwal_context* wal);

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
 * @note (To be implemented) In nvwal_config, there will be
 * an option for libnvwal itself to
 * launch a thread and invoke this method. In that case, the client application
 * must make sure that the program is linked against pthread.
 */
nvwal_error_t nvwal_flusher_main(
  struct nvwal_context* wal);

/**
 * DESCRIBE ME.
 */
nvwal_error_t nvwal_on_wal_write(
  struct nvwal_writer_context* writer,
  const nvwal_byte_t* src,
  uint64_t size_to_write,
  nvwal_epoch_t current);

/**
 * DESCRIBE ME.
 */
nvwal_error_t nvwal_assure_writer_space(
  struct nvwal_writer_context* writer);
//  uint64_t size_to_write);  // no need for this param, right? or is it some kind of hint?

/** Is this needed? Or can we just read wal->durable? */
nvwal_error_t nvwal_query_durable_epoch(
  struct nvwal_context* wal,
  nvwal_epoch_t* out);

/** @} */

#endif  // NVWAL_API_H_
