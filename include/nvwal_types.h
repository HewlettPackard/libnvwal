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

/**
 * @brief \b Epoch, a coarse-grained timestamp libnvwal is based on.
 * @details
 * Epoch is the most important concept in libnvwal.
 * Most feature in libnvwal is provided on top of epochs.
 *
 * @par What is Epoch
 * An Epoch represents a \b user-defined duration of time.
 * Depending on the type of client applications, it might be just one transaction,
 * 10s of milliseconds, or something else. Will talk about each application later.
 * In either case, it is coarse-grained, not like a nanosecond or RDTSC.
 * An epoch contains some number of log entries, ranging from 1 to millions or more.
 * Each log entry always belongs to exactly one epoch, which represents
 * \b when the log is written and becomes durable.
 *
 * @par Log ordering invariant
 * libnvwal always writes logs out to stable storage per epoch.
 * We durably write out logs of epoch E, then epoch E+1, etc.
 * When the user requests to read back logs,
 * we always guarantee that logs are ordered by epochs.
 * Logs in the same epoch might be not in real-time order, but they
 * do have some guarantee. See the corresponding API.
 *
 * @par Concept: Durable Epoch (DE)
 * \b Durable \b Epoch (\b DE) is the epoch upto which all logs in the epoch
 * are already durably written at least to NVDIMM. No log entries in DE
 * can be newly submit to libnvwal.
 * libnvwal provides an API, nvwal_query_durable_epoch(), to atomically
 * check the current value of DE of the system.
 *
 * @par Concept: Stable Epoch (SE)
 * \b Stable \b Epoch (\b SE) is the epoch upto which all logs in the epoch
 * are already submit to libnvwal.
 * No log entries in SE can be newly submit to libnvwal.
 * SE is equal to either DE or DE+1.
 * \li When libnvwal's flusher is well catching up, SE == DE in most of the time.
 * \li When workers are waiting for libnvwal's flusher, SE == DE+1.
 * As soon as the flusher completes writing, flushing, metadata-update, etc, it will
 * bump up DE for one, thus switching to another case.
 *
 * @par Concept: Next Epoch (NE)
 * \b Next \b Epoch (\b NE) is the epoch whose logs are ready
 * to be written out to durable storages.
 * NE is equal to either SE or SE+1.
 * NE is a hint for libnvwal to utilize storage bandwidth while waiting for flusher
 * and easily keep the log ordering invariant.
 * \li When NE == SE+1, libnvwal is allowed to go ahead and write out logs in NE
 * ahead of the flusher being asked to complete SE. All the logs in SE must be
 * already written out, whether durably or not.
 * \li When NE == SE, libnvwal just writes out logs in SE to complete the epoch.
 *
 * Even if there are logs in NE + 1, libnvwal does NOT write them out.
 * We can potentially provide the epoch-by-epoch guarantee without this restriction,
 * but things are much simpler this way. libnvwal handles up to 3 or 4 epochs always.
 *
 * @par Concept: Horizon Epoch (HE)
 * \b Horizon \b Epoch (\b HE) is a mostly conceptual epoch \b we \b don't \b care.
 * HE is an epoch larger than NE. libnvwal currently does not allow submitting
 * a log in this epoch. If a worker thread does it, the thread gets synchronously waited.
 *
 * @par Examples
 * \li DE = SE = NE : Idle. Probably this happens only at start up.
 * libnvwal is not allowed to write out anything, and everything seems already durable.
 * \li DE = SE < NE : Most common case. Flusher is well catching up and writing out
 * logs in NE well ahead, fully utilizing memory/storage bandwidth.
 * \li DE < SE = NE : Second most common case. The client application has requested
 * to bump up SE and then DE. Flusher is completing remaining writes in SE and then
 * will make sure the writes are durable as well as our internal metadata is
 * atomically written.
 * \li DE < SE < NE : A bit advanced case.
 * All log writes in SE must be already done, but the flusher might be still waiting
 * for the return of fsync, etc. In the meantime, we can go ahead and start writing logs in NE.
 * To make this case useful, the flusher must have a separate fsync-thread,
 * which we currently do not have. We might do this in future, though.
 *
 * @par Current XXX Epoch
 * The above epochs are globally meaningful concepts.
 * However, libnvwal involves various threads that must be as indepent as possible
 * for scalability. We can not and should not pause all of them whenever
 * we advance the global epochs.
 * Rather, each thread often maintains its own view of currently active epochs.
 * They sometime lag behind, or even have some holes (eg. no logs received for an epoch).
 * Current XXX Epoch is the oldest epoch XXX (some module/thread) is taking care of.
 * Probably it is same as the global DE. It might be occasionally DE - 1.
 * It's asynchronously and indepently maintained by XXX to catch up with the global epochs.
 * One very helpful guarantee here is that there is no chance XXX has to handle
 * DE-2 or DE+3 thanks to how we advance DE, and that HE is not allowed to submit.
 * Based on this guarantee, many modules in libnvwal have circular windows
 * of epochs with 5 or 6 slots. They do not need to maintain any information
 * older or newer than that.
 *
 * @par Wrap-around
 * We might run out of 2^64 epochs at some point.
 * Wrap-around is implemented as below.
 * \li We \b somehow guarantee that no two epochs
 *  in the system are more than 2^63 distant.
 * \li Thus, similarly to RFC 1982, a is after b \e iff
 * (b < a < b+2^63) or (a < b < a+2^63).
 * \li If the system is running for looong time (>2^63), we need
 * to provide a compaction tool to modify epoch values in
 * our metadata store. This piece is to be designed/implemented.
 * We will need it much much later.
 * @see nvwal_is_epoch_after()
 */
typedef uint64_t  nvwal_epoch_t;

/** DESCRIBE ME */
typedef int32_t   nvwal_error_t;
/** DESCRIBE ME */
typedef int8_t    nvwal_byte_t;

enum nvwal_constants {
  /**
   * Throughout this library, every file path must be represented within this length,
   * including null termination and serial path suffix.
   * In several places, we assume this to avoid dynamically-allocated strings and
   * simplify copying/allocation/deallocation.
   */
  kNvwalMaxPathLength = 256U,

  /**
   * Likewise, we statically assume each WAL instance has at most this number of
   * log writers assigned.
   * Thanks to these assumptions, all structs defined in this file are PODs.
   */
  kNvwalMaxWorkers = 64U,

  /**
   * @brief Largest number of log segments being actively written.
   * @details
   * The number of active log segments is calculated from nv_quota / kNvwalSegmentSize.
   * This constant defines the largest possible number for that.
   * If nv_quota demands more than this, nvwal_init() returns an error.
   */
  kNvwalMaxActiveSegments = 1024U,

  /**
   * DESCRIBE ME.
   * 32MB sounds like a good place to start?
   */
  kNvwalSegmentSize = 1ULL << 25,
};


/** DESCRIBE ME */
enum nvwal_seg_state_t {
  SEG_UNUSED = 0,
  SEG_ACTIVE,
  SEG_COMPLETE,
  SEG_SUBMITTED,
  SEG_SYNCING,
  SEG_SYNCED
};

/** Ignored. */
enum nvwal_config_flags {
    BG_FSYNC_THREAD = 1 << 0,
    MMAP_DISK_FILE  = 1 << 1,
    CIRCULAR_LOG    = 1 << 2
};

/**
 * DESCRIBE ME.
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing.
 */
struct nvwal_config {
  /**
   * Null-terminated string of folder to NVDIMM storage, under which
   * this WAL instance will write out log files at first.
   * If this string is not null-terminated, nvwal_init() will return an error.
   */
  char nv_root[kNvwalMaxPathLength];

  /**
   * Null-terminated string of folder to block storage, into which
   * this WAL instance will copy log files from NVDIMM storage.
   * If this string is not null-terminated, nvwal_init() will return an error.
   */
  char block_root[kNvwalMaxPathLength];

  /**
   * When this is a second run or later, give the definitely-durable epoch
   * as of starting.
   */
  nvwal_epoch_t resuming_epoch;

  uint32_t numa_domain;

  /**
   * Number of log writer threads on this WAL instance.
   * This value must be kNvwalMaxWorkers or less.
   * Otherwise, nvwal_init() will return an error.
   */
  uint32_t writer_count;

  /** How big our nvram segments are */
  uint64_t nv_seg_size;
  uint64_t nv_quota;

  /** Assumed to be the same as nv_seg_size for now */
  uint64_t block_seg_size;

  /** Size of (volatile) buffer for each writer-thread. */
  uint64_t writer_buffer_size;

  /**
    * How many on-disk log segments to create a time (empty files)
    * This reduces the number of times we have to fsync() the directory
    */
  uint32_t prealloc_file_count;

  uint64_t option_flags;

  /**
   * Buffer of writer_buffer_size bytes for each writer-thread,
   * allocated/deallocated by the client application.
   * The client application must provide it as of nvwal_init().
   * If any of writer_buffers[0] to writer_buffers[writer_count - 1]
   * is null, nvwal_init() will return an error.
   */
  nvwal_byte_t* writer_buffers[kNvwalMaxWorkers];
};

/**
 * These two structs, nvwal_buffer_control_block and nvwal_writer_context are used to
 * communicate between a writer thread and the flusher thread, keeping track of
 * the status of one writer's volatile buffer. The offsets follow this
 * logical ordering:
 *
 *     head <= durable <= copied <= complete <= tail
 *
 * These inequalities might not hold on the numeric values of the offsets
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
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing.
 */
struct nvwal_buffer_control_block {
  /** Updated by writer via nvwal_on_wal_write() and read by flusher thread.  */

  /** Where the writer will put new bytes - we don't currently read this */
  uint64_t tail;

  /** Beginning of completely written bytes */
  uint64_t head;

  /** End of completely written bytes */
  uint64_t complete;

  /** max of epochs seen by on_wal_write() */
  nvwal_epoch_t latest_written;
};

/**
 * DESCRIBE ME
 *
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing. All pointers in
 * this object just point to an existing buffer, in other words they
 * are just markers (TODO: does it have to be a pointer? how about offset).
 */
struct nvwal_writer_context {
  /** Back pointer to the parent WAL context. */
  struct nvwal_context* parent;

  /**
   * Sequence unique among the same parent WAL context, 0 means the first writer.
   * This is not unique among writers on different WAL contexts,
   * @invariant this == parent->writers + writer_seq_id
   */
  uint32_t writer_seq_id;

  struct nvwal_buffer_control_block cb;

  /**
    * Everything up to this point is durable. It is safe for the application
    * to move the head up to this point.
    */
  uint64_t flushed;

  /**
    * Everything up to this point has been copied by the flusher thread but
    * might not yet be durable
    */
  uint64_t copied;

  /** Shorthand for parent->config.writer_buffers[writer_seq_id] */
  nvwal_byte_t* buffer;

  /** Pending work is everything between copied and writer->complete */
};

/**
 * DESCRIBE ME
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing \b except \b file \b descriptors.
 */
struct nvwal_log_segment {
  nvwal_byte_t* nv_baseaddr;

  /** On-disk sequence number, identifies filename */
  uint64_t seq;
  int32_t nvram_fd;
  int32_t disk_fd;
  uint64_t disk_offset;

  /** May be used for communication between flusher thread and fsync thread */
  enum nvwal_seg_state_t state;

  /** true if direntry of disk_fd is durable */
  bool dir_synced;
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
 *
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing \b except \b file \b descriptors.
 * @note To be a POD, however, this object conservatively consumes a bit large memory.
 * We recommend allocating this object on heap rather than on stack. Although
 * it's very unlikely to have this long-living object on stack anyways (unit test?)..
 */
struct nvwal_context {
  nvwal_epoch_t durable;
  nvwal_epoch_t latest;

  /**
   * All static configurations given by the user on initializing this WAL instance.
   * Once constructed, this is/must-be const. Do not modify it yourself!
   */
  struct nvwal_config config;

  /**
   * DESCRIBE ME
   */
  uint32_t num_active_segments;

  /**
   * DESCRIBE ME
   */
  struct nvwal_log_segment active_segments[kNvwalMaxActiveSegments];

  int32_t log_root_fd;

  /** 0 if append-only log */
  uint64_t max_log_size;

  /** Next on-disk sequence number, see log_segment.seq  */
  uint64_t log_sequence;

  /** mmapped address of current nv segment*/
  nvwal_byte_t* cur_region;

  /** Index into nv_region */
  uint64_t nv_offset;

  /** Index into segment[] */
  uint32_t cur_seg_idx;

  struct nvwal_writer_context writers[kNvwalMaxWorkers];

  /**
   * Used to inform the flusher that nvwal_uninit() was invoked.
   */
  uint8_t flusher_stop_requested;
  /**
   * Set when the flusher thread started running.
   */
  uint8_t flusher_running;
};

/** @} */

#endif  /* NVWAL_TYPES_H_ */
