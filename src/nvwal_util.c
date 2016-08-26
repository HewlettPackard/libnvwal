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

nvwal_error_t nvwal_stock_error_code(nvwal_error_t cur_code, nvwal_error_t new_code) {
  if (new_code) {
    return new_code;
  } else {
    return cur_code;
  }
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

  out[out_len] = '/';
  ++out_len;

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
  nvwal_error_t ret;
  int fd;

  fd = open(path, 0, 0);
  if (fd == -1) {
    return errno;
  }

  assert(fd);

  ret = fsync(fd);
  if (ret) {
    ret = errno;
    close(fd);
    return ret;
  }

  close(fd);
  return 0;
}

