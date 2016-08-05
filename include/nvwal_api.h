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
 */

#include "nvwal_fwd.h"
#include "nvwal_types.h"

/**
 * DESCRIBE ME.
 * @param[in] wal_config DESCRIBE ME
 * @param[out] wal_context DESCRIBE ME
 */
nvwal_error_t nvwal_init(
  const struct nvwal_config* wal_config,
  struct nvwal_context* wal_context);

/**
 * DESCRIBE ME.
 * @param[in] wal_context DESCRIBE ME
 */
nvwal_error_t nvwal_uninit(
  struct nvwal_context* wal_context);

/**
 * DESCRIBE ME.
 * @param[in] wal_context DESCRIBE ME
 * @param[in] write_buffer DESCRIBE ME
 * @param[out] wal_writer DESCRIBE ME
 */
nvwal_error_t nvwal_register_writer(
  struct nvwal_context* wal_context,
  struct nvwal_buffer_control_block* write_buffer,
  struct nvwal_writer_info* wal_writer);

/**
 * DESCRIBE ME.
 */
nvwal_error_t nvwal_on_wal_write(
  struct nvwal_context* wal_context,
  struct nvwal_writer_info* wal_writer,
  const void* src,
  uint64_t size_to_write,
  nvwal_epoch_t current);

/**
 * DESCRIBE ME.
 */
nvwal_error_t nvwal_assure_wal_space(
  struct nvwal_context* wal_context,
  struct nvwal_writer_info* wal_writer);
//  uint64_t size_to_write);  // no need for this param, right? or is it some kind of hint?

/** Is this needed? Or can we just read wal->durable? */
nvwal_error_t nvwal_query_durable_epoch(
  struct nvwal_context* wal_context);

#endif  // NVWAL_API_H_
