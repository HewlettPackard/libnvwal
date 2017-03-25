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
