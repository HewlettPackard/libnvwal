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
#include "nvwal_util.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include "nvwal_types.h"

nvwal_error_t nvwal_raise_einval(const char* message) {
  fprintf(stderr, message);
  errno = EINVAL;
  return EINVAL;
}

nvwal_error_t nvwal_raise_einval_llu(const char* message, uint64_t param) {
  fprintf(stderr, message, param);
  errno = EINVAL;
  return EINVAL;
}

nvwal_error_t nvwal_raise_einval_cstr(const char* message, const char* param) {
  fprintf(stderr, message, param);
  errno = EINVAL;
  return EINVAL;
}

void nvwal_output_warning(const char* message) {
  fprintf(stdout, message);
}
void nvwal_output_warning_llu(const char* message, uint64_t param) {
  fprintf(stdout, message, param);
}
void nvwal_output_warning_cstr(const char* message, const char* param) {
  fprintf(stdout, message, param);
}

void nvwal_concat_sequence_filename(
  const char* folder,
  const char* file_prefix,
  uint32_t sequence,
  char* out) {
  int folder_len, file_prefix_len, out_len, cpos;
  int8_t c;

  out[0] = '\0';

  folder_len = strnlen(folder, kNvwalMaxFolderPathLength);
  if (folder_len >= kNvwalMaxFolderPathLength) {
    errno = EINVAL;
    return;
  }
  file_prefix_len = strnlen(file_prefix, 16);
  if (file_prefix_len >= 16) {
    errno = EINVAL;
    return;
  }

  memcpy(out, folder, folder_len);
  out_len = folder_len;

  if (folder_len == 0) {
    /* No folder specified. Probably not a good practice, but we tolerate it */
  } else if (folder[folder_len - 1] != '/') {
    out[out_len] = '/';
    ++out_len;
  }

  memcpy(out + out_len, file_prefix, file_prefix_len);
  out_len += file_prefix_len;

  assert(out_len + 9 <= kNvwalMaxPathLength);
  for (cpos = 0; cpos < 8; ++cpos) {
    c = (sequence >> (cpos * 4)) & 0xF;
    if (c < 10) {
      out[out_len + 7 - cpos] = '0' + c;
    } else {
      out[out_len + 7 - cpos] = 'A' + (c - 10);
    }
  }
  out_len += 8;
  out[out_len] = '\0';
}

void nvwal_construct_nv_segment_path(
  const struct NvwalContext* wal,
  uint32_t nv_segment_index,
  char* out_nv_path) {
  assert(wal->config_.nv_root_len_ + 32U < kNvwalMaxPathLength);
  nvwal_concat_sequence_filename(
    wal->config_.nv_root_,
    "nv_segment_",
    nv_segment_index,
    out_nv_path);
}

void nvwal_construct_disk_segment_path(
  const struct NvwalContext* wal,
  nvwal_dsid_t dsid,
  char* out_nv_path) {
  assert(wal->config_.disk_root_len_ + 32U < kNvwalMaxPathLength);
  nvwal_concat_sequence_filename(
    wal->config_.disk_root_,
    "nvwal_ds_",
    dsid,
    out_nv_path);
}

int nvwal_open_best_effort_o_direct(
  const char* path,
  int oflag,
  int mode) {
  int fd;
  int oflag_direct, oflag_no_direct;

  oflag_direct = oflag | O_DIRECT;
  oflag_no_direct = oflag_direct ^ O_DIRECT;

  fd = open(path, oflag_direct, mode);
  if (fd == -1 && errno == EINVAL) {
    fd = open(path, oflag_no_direct, mode);
    if (fd != -1) {
      /** Okay, O_DIRECT was the cause. Just complain. go on. */
      fprintf(stderr,
        "nvwal_open_best_effort_o_direct() : O_DIRECT flag "
        " was rejected and automatically removed. This usually means you specified"
        " tmpfs, such as /tmp, /dev/shm. Such non-durable devices should be used only"
        " for testing and performance experiments."
        " Related URL: http://www.gossamer-threads.com/lists/linux/kernel/720702");
    }
    /** else the normal error flow below. */
  }
  return fd;
}

nvwal_error_t nvwal_open_and_fsync(const char* path) {
  int fd = open(path, 0, 0);
  if (fd == -1) {
    return errno;
  }

  assert(fd);

  nvwal_error_t ret = 0;
  if (fsync(fd)) {
    ret = errno;
  }

  close(fd);
  return ret;
}

nvwal_error_t nvwal_open_and_syncfs(const char* path) {
  int fd = open(path, 0, 0);
  if (fd == -1) {
    return errno;
  }

  assert(fd);

  nvwal_error_t ret = 0;
  if (syncfs(fd)) {
    ret = errno;
  }

  close(fd);
  return ret;
}

nvwal_error_t nvwal_remove_all_under(const char* path) {
  const uint32_t path_len = strnlen(path, kNvwalMaxPathLength);
  if (path_len >= kNvwalMaxPathLength) {
    /** For simplicity (laziness) we assume this below. */
    return nvwal_raise_einval("Error: nvwal_remove_all_under() encountered too long path");
  }

  DIR* dir = opendir(path);
  if (dir) {
    nvwal_error_t ret = 0;
    char child_path[kNvwalMaxPathLength];
    memcpy(child_path, path, path_len);
    child_path[path_len] = '/';
    while (1) {
      struct dirent* ent = readdir(dir);
      if (ent) {
        /** Ignore "." and ".." */
        if (strncmp(ent->d_name, ".", 1) == 0
          || strncmp(ent->d_name, "..", 2) == 0) {
          continue;
        }

        /** Recurse... */
        const uint32_t child_len
          = strnlen(ent->d_name, kNvwalMaxPathLength);
        if (path_len + 1 + child_len > kNvwalMaxPathLength) {
          return nvwal_raise_einval(
            "Error: nvwal_remove_all_under() encountered too long path");
        }
        memcpy(child_path + path_len + 1, ent->d_name, child_len);
        child_path[path_len + 1 + child_len] = '\0';
        ret = nvwal_remove_all_under(child_path);
        if (ret) {
          break;
        }
      } else {
        break;
      }
    }
    closedir(dir);
    return ret;
  } else {
    errno = ENOENT;
    return ENOENT;
  }
}


uint8_t nvwal_is_valid_dir(const char* path) {
  DIR* dir = opendir(path);
  if (dir) {
    closedir(dir);
    return 1U;
  } else {
    return 0U;
  }
}

uint8_t nvwal_is_nonempty_dir(const char* path) {
  DIR* dir = opendir(path);
  if (dir) {
    int found = 0;  /** Remember, there are "." and ".." */
    while (found <= 2) {
      if (readdir(dir)) {
        ++found;
      } else {
        break;
      }
    }
    closedir(dir);
    return (found > 2) ? 1U : 0U;
  } else {
    return 0U;
  }
}

void nvwal_circular_memcpy(
  nvwal_byte_t* dest,
  const nvwal_byte_t* circular_src_base,
  uint64_t circular_src_size,
  uint64_t circular_src_cur_offset,
  uint64_t bytes_to_copy) {
  assert(circular_src_size >= circular_src_cur_offset);
  assert(circular_src_size >= bytes_to_copy);
  if (circular_src_cur_offset + bytes_to_copy >= circular_src_size) {
    /** We need a wrap-around */
    uint64_t bytes = circular_src_size - circular_src_cur_offset;
    if (bytes) {
      memcpy(dest, circular_src_base + circular_src_cur_offset, bytes);
      dest += bytes;
      bytes_to_copy -= bytes;
    }
    circular_src_cur_offset = 0;
  }

  memcpy(dest, circular_src_base + circular_src_cur_offset, bytes_to_copy);
}

