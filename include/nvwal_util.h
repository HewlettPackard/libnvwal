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
#ifndef NVWAL_UTIL_H_
#define NVWAL_UTIL_H_
/**
 * @file nvwal_util.h
 * Internal and assorted macros/functions etc.
 * We should NOT have much here.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include <errno.h>
#include <stdio.h>

#include "nvwal_types.h"

inline nvwal_error_t nvwal_raise_einval(const char* message) {
  fprintf(stderr, message);
  errno = EINVAL;
  return EINVAL;
}

inline nvwal_error_t nvwal_raise_einval_llu(const char* message, uint64_t param) {
  fprintf(stderr, message, param);
  errno = EINVAL;
  return EINVAL;
}

/**
 * @brief A frequently occurring pattern in our code to construct a file name
 * from a common prefix and fixed-size hex string.
 * @param[in] folder Null-terminated folder path. Must be < kNvwalMaxFolderPathLength.
 * It should not be ending with /. But, probably it works even if not...
 * @param[in] file_prefix Null-terminated prefix filename. Must be within 16 characters.
 * @param[in] sequence Integer to be hex-ed and appended to the file name.
 * @param[out] out receive the constructed null-terminated filepath.
 * Must be at least kNvwalMaxPathLength
 */
void nvwal_concat_sequence_filename(
  const char* folder,
  const char* file_prefix,
  uint32_t sequence,
  char* out);

/**
 * @brief Equivalent to open(2) with O_DIRECT flag.
 * @details
 * The difference from open(2) is that it internally retries to open if
 * the filesystem refuses to receive O_DIRECT.
 * tmpfs (such as /tmp, /dev/shm) refuses to receive O_DIRECT, returning EINVAL (22).
 * In that case, let's retry without O_DIRECT flag. MySQL does similar thing, too.
 * If even the retry fails, it just returns what open(2) returned.
 * @return return value of open(2)
 */
int nvwal_open_best_effort_o_direct(
  const char* path,
  int oflag,
  int mode);

/**
 * A convenience method to do open(), fsync(), then close().
 * This is NOT recursive; we don't fsync the parent folder.
 * If you are calling this method for a file whose file size might be changing,
 * you must also call this for its parent folder.
 */
nvwal_error_t nvwal_open_and_fsync(const char* path);


/** @} */

#endif  /* NVWAL_UTIL_H_ */
