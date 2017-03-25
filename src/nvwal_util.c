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
  sync();

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

        const uint32_t child_len
          = strnlen(ent->d_name, kNvwalMaxPathLength);
        if (path_len + 1 + child_len > kNvwalMaxPathLength) {
          return nvwal_raise_einval(
            "Error: nvwal_remove_all_under() encountered too long path");
        }
        memcpy(child_path + path_len + 1, ent->d_name, child_len);
        child_path[path_len + 1 + child_len] = '\0';
        if (ent->d_type == DT_DIR) {
          /** Recurse... */
          ret = nvwal_remove_all_under(child_path);
          if (ret) {
            break;
          }
        }

        if (remove(child_path)) {
          ret = errno;
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

void nvwal_circular_dest_memcpy(
    nvwal_byte_t* circular_dest_base,
    uint64_t circular_dest_size,
    uint64_t circular_dest_cur_offset,
    const nvwal_byte_t* src,
    uint64_t bytes_to_copy) 
{
    assert(circular_dest_size >= circular_dest_cur_offset);
    assert(circular_dest_size >= bytes_to_copy);
    if (circular_dest_cur_offset + bytes_to_copy >= circular_dest_size) {
      /** We need a wrap-around */
      uint64_t bytes = circular_dest_size - circular_dest_cur_offset;
      if (bytes) {
        memcpy(circular_dest_base + circular_dest_cur_offset, src, bytes);
        src += bytes;
        bytes_to_copy -= bytes;
      }
      circular_dest_cur_offset = 0;
    }

    memcpy(circular_dest_base + circular_dest_cur_offset, src, bytes_to_copy);
}

void hexdump(nvwal_byte_t* buf, uint64_t len)
{
    for (uint64_t i = 0; i < len; i+=16) {
        printf("%08lx ", i);
        for (uint64_t j=0; j < 16; j+=2) {
            printf("%02x %02x ", (short int) buf[i+j+1], (short int) buf[i+j]);
        }
        printf("\n");
    }
}
