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
#ifndef NVWAL_IMPL_CURSOR_H_
#define NVWAL_IMPL_CURSOR_H_
/**
 * @file nvwal_impl_cursor.h
 * Internal functions for cursor objects in libnvwal.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include "nvwal_fwd.h"
#include "nvwal_types.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

struct MdsEpochMetadata;

/**
 * Intermal method to release cursor->cur_segment_data_ and
 * cursor->cur_segment_fd_.
 * Used in cursor-close and a few other places.
 */
nvwal_error_t cursor_close_cur_segment(struct NvwalLogCursor* cursor);

/**
 * Internal method to open the given segment as the current segment.
 */
nvwal_error_t cursor_open_segment(struct NvwalLogCursor* cursor, nvwal_dsid_t dsid);

/**
 * Intermal method to read epoch metadata from the given epoch into the cursor.
 */
nvwal_error_t cursor_fetch_epoch_metadata(
  struct NvwalLogCursor* cursor,
  nvwal_epoch_t from_epoch);


/* sub routine of nvwal_open_log_cursor */
nvwal_error_t cursor_next_initial(struct NvwalLogCursor* cursor);

#ifdef __cplusplus
}
#endif  /* __cplusplus */

/** @} */

#endif  /* NVWAL_IMPL_CURSOR_H_ */
