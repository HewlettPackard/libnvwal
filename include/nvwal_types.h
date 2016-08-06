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
#ifndef NVWAL_TYPES_H_
#define NVWAL_TYPES_H_
/**
 * @file nvwal_types.h
 * Provides typedefs/enums/structs used in libnvwal.
 * @ingroup LIBNVWAL
 * @addtogroup LIBNVWAL
 * @{
 */

#include <stdbool.h>
#include <stdint.h>

#include "nvwal_fwd.h"

/** DESCRIBE ME */
typedef uint64_t  nvwal_epoch_t;
/** DESCRIBE ME */
typedef int32_t   nvwal_error_t;
/** DESCRIBE ME */
typedef int8_t    nvwal_byte_t;

/** DESCRIBE ME */
enum nvwal_seg_state_t {
  SEG_UNUSED = 0,
  SEG_ACTIVE,
  SEG_COMPLETE,
  SEG_SUBMITTED,
  SEG_SYNCING,
  SEG_SYNCED
};

/**
 * @brief Represents a context of \b one stream of write-ahead-log placed in
 * NVDIMM and secondary device.
 * @details
 * Each nvwal_context instance must be initialized by nvwal_init() and cleaned up by
 * nvwal_uninit().
 * Client programs that do distributed logging will instantiate
 * an arbitrary number of this context, one for each log stream.
 * There is no interection between two nvwal_context from libnvwal's standpoint.
 * It's the client program's responsibility to coordinate them.
 */
struct nvwal_context {
  nvwal_epoch_t durable;
  nvwal_epoch_t latest;

  char * nv_root;
  uint64_t nv_quota;
  int num_segments;

  struct nvwal_log_segment* segment;

  /** Path to log on block storage */
  char * log_root;
  int log_root_fd;

  /** 0 if append-only log */
  uint64_t max_log_size;

  /** Next on-disk sequence number, see log_segment.seq  */
  uint64_t log_sequence;

  /** mmapped address of current nv segment*/
  char* cur_region;

  /** Index into nv_region */
  uint64_t nv_offset;

  /** Index into segment[] */
  int cur_seg_idx;

  struct nvwal_writer_info* writer_list;
};

/** DESCRIBE ME */
struct nvwal_log_segment {
  nvwal_byte_t* nv_baseaddr;

  /** On-disk sequence number, identifies filename */
  uint64_t seq;
  int32_t nvram_fd;
  int32_t disk_fd;

  /** May be used for communication between flusher thread and fsync thread */
  nvwal_seg_state_t state;

  /** true if direntry of disk_fd is durable */
  bool dir_synced;
};

/**
 * These two structs, BufCB and WInfo (names subject to change) are used to
 * communicate between a writer thread and the flusher thread, keeping track of
 * the status of one writer's volatile buffer. The pointers follow this
 * logical ordering:
 *
 *     head <= durable <= copied <= complete <= tail
 *
 * These inequalities might not hold on the numeric values of the pointers
 * because the buffer is circular. Instead, we should first define the notion of
 * "circular size":
 *
 *    circular_size(x,y,size) (y >= x ? y - x : y + size - x)
 *
 * This gives the number of bytes between x and y in a circular buffer. If
 * x == y, the buffer is empty. If y < x, the buffer has wrapped around the end
 * point, and so we count the bytes from 0 to y and from x to size, which
 * gives y + size - x. This type of buffer can never actually hold "size" bytes
 * as there must always be one empty cell between the tail and the head.
 *
 * Then the logical pointer relationship a <= b can be tested as:
 *
 *    circular_size(head, a, size) <= circular_size(head, b, size)
 *
 */
struct nvwal_buffer_control_block {
  /** Updated by writer via nvwal_on_wal_write() and read by flusher thread.  */

  /** Where the writer will put new bytes - we don't currently read this */
  nvwal_byte_t* tail;

  /** Beginning of completely written bytes */
  nvwal_byte_t* head;

  /** End of completely written bytes */
  nvwal_byte_t* complete;

  /** max of epochs seen by on_wal_write() */
  nvwal_epoch_t latest_written;

  /** Size of (volatile) buffer. Do not change after registration! */
  uint64_t buffer_size;

  /** Buffer of buffer_size bytes, allocated by application */
  nvwal_byte_t* buffer;
};

/** DESCRIBE ME */
struct nvwal_writer_info {
  /** Updated by flusher, read by writer */
  struct nvwal_writer_info* next;
  struct nvwal_buffer_control_block* writer;

  /**
    * Everything up to this point is durable. It is safe for the application
    * to move the head up to this point.
    */
  nvwal_byte_t* flushed;

  /**
    * Everything up to this point has been copied by the flusher thread but
    * might not yet be durable
    */
  nvwal_byte_t* copied;

  /** Pending work is everything between copied and writer->complete */
};

/** Ignored. */
enum nvwal_config_flags {
    BG_FSYNC_THREAD = 1 << 0,
    MMAP_DISK_FILE  = 1 << 1,
    CIRCULAR_LOG    = 1 << 2
};

/** DESCRIBE ME */
struct nvwal_config {
  char * nv_root;
  int numa_domain;

  /** How big our nvram segments are */
  uint64_t nv_seg_size;
  uint64_t nv_quota;

  char * log_path;

  /** Assumed to be the same as nv_seg_size for now */
  uint64_t disk_seg_size;

  /**
    * How many on-disk log segments to create a time (empty files)
    * This reduces the number of times we have to fsync() the directory
    */
  int prealloc_file_count;

  int option_flags;
};

/** @} */

#endif  // NVWAL_TYPES_H_
