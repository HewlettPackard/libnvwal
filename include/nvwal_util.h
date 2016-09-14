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

#include "nvwal_types.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

nvwal_error_t nvwal_raise_einval(const char* message);

nvwal_error_t nvwal_raise_einval_llu(const char* message, uint64_t param);

nvwal_error_t nvwal_raise_einval_cstr(const char* message, const char* param);

/**
 * These are just warning, do not stop anything.
 * We need to determine whether we should output to files or stdout or stderr...
 * and whether/how we wipe them off completely based on build type, severity level etc..
 * The implementation will be changed. Nevertheless, we should
 * use these methods rather than printf/fprintf all over the place.
 */
void nvwal_output_warning(const char* message);
void nvwal_output_warning_llu(const char* message, uint64_t param);
void nvwal_output_warning_cstr(const char* message, const char* param);

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
 * We should use this method in all places that construct
 * a segment file on NVDIMM.
 */
void nvwal_construct_nv_segment_path(
  const struct NvwalContext* wal,
  uint32_t nv_segment_index,
  char* out_nv_path);

/**
 * We should use this method in all places that construct
 * a segment file on disk.
 */
void nvwal_construct_disk_segment_path(
  const struct NvwalContext* wal,
  nvwal_dsid_t dsid,
  char* out_nv_path);

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

/**
 * Another convenience method to do open(), syncfs(), then close().
 * This is useful when we need a durabiliry barrier after several file writes and
 * individual fsync is a bit wasteful.
 * For more details, check man syncfs.
 */
nvwal_error_t nvwal_open_and_syncfs(const char* path);

/**
 * Deletes all files/folders recursively under the given folder.
 * The folder itself is not removed.
 * @param[in] path Folder path. This method can't handle a folder whose path
 * or its descendants' path is kNvwalMaxPathLength or longer.
 */
nvwal_error_t nvwal_remove_all_under(const char* path);

/**
 * @returns Whether we coulf confirm that the given path represents a valid folder.
 * Returns 0 in all other cases, including the folder doesn't exist, any I/O error.
 */
uint8_t nvwal_is_valid_dir(const char* path);

/**
 * @returns Whether the given path represents a valid folder with at least one child.
 * Returns 0 in all other cases, including the folder doesn't exist, any I/O error.
 */
uint8_t nvwal_is_nonempty_dir(const char* path);

/**
 * Used to retain the last-observed error.
 */
static inline nvwal_error_t nvwal_stock_error_code(
  nvwal_error_t cur_code,
  nvwal_error_t new_code) {
  if (new_code) {
    return new_code;
  } else {
    return cur_code;
  }
}

/**
 * Circular-buffer-aware memcpy.
 */
void nvwal_circular_memcpy(
  nvwal_byte_t* dest,
  const nvwal_byte_t* circular_src_base,
  uint64_t circular_src_size,
  uint64_t circular_src_cur_offset,
  uint64_t bytes_to_copy);

/**
 * Min/max.
 * It's absurd to have such macro ourselves, but in pure C "whether/where min/max is"
 * is a stupidly complicated issue in some environment.
 * Rather we just have them here. Long live C++.
 */
#define NVWAL_MAX(a,b) (((a) > (b)) ? (a) : (b))
#define NVWAL_MIN(a,b) (((a) < (b)) ? (a) : (b))

#define NVWAL_CHECK_ERROR(X) { nvwal_error_t __x = X; if (__x) { return __x; } }

/** @} */

#ifdef __cplusplus
}
#endif  /* __cplusplus */

#endif  /* NVWAL_UTIL_H_ */
