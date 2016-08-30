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
 * @par Examples of DE, SE, NE relationship
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
 * @par Translating epochs to user applications
 * Applications have varying mappings between their own notiion of timestamp
 * and libnvwal's epochs. We carefully designed our epochs so that all
 * existing database architectures can be mapped to epochs as below.
 *
 * @par Application 1 : Epoch-based databases
 * Epoch-based databases, such as FOEDUS/SILO etc, are mapped to libnvwal's
 * epochs without any hassle. Some architecture might not differentiate DE/SE/NE,
 * but all of them are based on epochs.
 * \li Call nvwal_advance_next_epoch() whenever the new epoch might come in to the system.
 * \li Call nvwal_advance_stable_epoch() whenever the client wants to advance its own epoch
 * and then call nvwal_query_durable_epoch() and sleep/spin on it.
 *
 * @par Application 2 : LSN-based databases
 * LSN based databases, such as MySQL/PostgreSQL etc, are mapped to libnvwal
 * by considering every transaction as an epoch.
 * \li LSN-based single-log-stream databases allocate only one writer in libnvwal.
 * Thus many things are simpler.
 * \li Whenever the application requests to make a commit log record of a transaction
 * durable, call nvwal_advance_stable_epoch(). The epoch and the LSN of the commit log
 * record have one-to-one monotonically increasing relationship.
 * \li We also allow binary-searching our epoch metadata array, assuming the above relationship.
 * For this purpose, we maintain one user-defined additional version tag in our metadata.
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
 * @see nvwal_increment_epoch()
 * @see kNvwalInvalidEpoch
 */
typedef uint64_t  nvwal_epoch_t;

/** DESCRIBE ME */
typedef int32_t   nvwal_error_t;
/** DESCRIBE ME */
typedef int8_t    nvwal_byte_t;

/**
 * @brief Unique identifier for a \b Durable (or disk-resident) \b Segment.
 * @details
 * DSID identifies a log segment on disk, uniquely within the given WAL instance.
 * DSID is not unique across different WAL instances, but they are completely separate.
 *
 * A valid DSID starts with 1 (0 means null) and grows by one whenever
 * we create a new segment file on disk.
 * Each log segment is initially just NVDIMM-resident, then copied to a
 * on-disk file named "nvwal_segment_xxxxxxxx" where xxxx is a hex string of
 * DSID (always 8 characters).
 *
 */
typedef uint64_t  nvwal_dsid_t;


enum NvwalConstants {
  /**
  * This value of epoch is reserved for null/invalid/etc.
  * Thus, when we increment an epoch, we must be careful.
  * @see nvwal_increment_epoch()
  */
  kNvwalInvalidEpoch = 0,

  /**
   * Represents null for nvwal_dsid_t.
   * @see nvwal_dsid_t
   */
  kNvwalInvalidDsid = 0,

  /**
   * Throughout this library, every file path must be represented within this length,
   * including null termination and serial path suffix.
   * In several places, we assume this to avoid dynamically-allocated strings and
   * simplify copying/allocation/deallocation.
   */
  kNvwalMaxPathLength = 256U,

  /**
   * nv_root_/disk_root_ must be within this length so that we can
   * append our own filenames under the folder.
   * eg nvwal_segment_xxxxxxxx.
   */
  kNvwalMaxFolderPathLength = kNvwalMaxPathLength - 32,

  /**
   * Likewise, we statically assume each WAL instance has at most this number of
   * log writers assigned.
   * Thanks to these assumptions, all structs defined in this file are PODs.
   */
  kNvwalMaxWorkers = 64U,

  /**
   * @brief Largest number of log segments being actively written.
   * @details
   * The number of active log segments is calculated from nv_quota / segment_size.
   * This constant defines the largest possible number for that.
   * If nv_quota demands more than this, nvwal_init() returns an error.
   */
  kNvwalMaxActiveSegments = 1024U,

  /**
   * Used as the segment size if the user hasn't speficied any.
   * 32MB sounds like a good place to start?
   */
  kNvwalDefaultSegmentSize = 1ULL << 25,

  /**
   * \li [oldest] : the oldest frame this writer \e might be using.
   * It is probably the global durable epoch, or occasionally older.
   * \li [oldest + 1, 2]: frames this writer \e might be using now.
   * \li [oldest + 3, 4]: guaranteed to be currently not used by this writer.
   * Thus, even when current_epoch_frame might be now bumped up for one, it is
   * safe to reset [oldest + 4]. This is why we have 5 frames.
   */
  kNvwalEpochFrameCount = 5,

  /**
   * Number of epochs to prefetch when calling get_next_epoch()
   */
  kNvwalPrefetchLength = 5,

  /**
   * @brief Default page size in bytes for meta-data store.
   * 
   */
  kNvwalMdsPageSize = 1ULL << 20,


  /**
   * @brief Largest number of page-files being actively written.
   */
  kNvwalMdsMaxActivePagefiles = 1U,

  /**
   * @brief Largest number of pages being buffered for reading.
   */
  kNvwalMdsMaxBufferPages = 1U,
};

/**
 * DESCRIBE ME.
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing.
 */
struct NvwalConfig {
  /**
   * Null-terminated string of folder to NVDIMM storage, under which
   * this WAL instance will write out log files at first.
   * If this string is not null-terminated, nvwal_init() will return an error.
   */
  char nv_root_[kNvwalMaxPathLength];

  /**
   * Null-terminated string of folder to block storage, into which
   * this WAL instance will copy log files from NVDIMM storage.
   * If this string is not null-terminated, nvwal_init() will return an error.
   */
  char disk_root_[kNvwalMaxPathLength];

  /**
   * strnlen(nv_root). Just an auxiliary variable. Automatically
   * set during initialization.
   */
  uint16_t nv_root_len_;

  /**
   * strnlen(disk_root). Just an auxiliary variable. Automatically
   * set during initialization.
   */
  uint16_t disk_root_len_;

  /**
   * When this is a second run or later, give the definitely-durable epoch
   * as of starting.
   */
  nvwal_epoch_t resuming_epoch_;

  /**
   * Number of log writer threads on this WAL instance.
   * This value must be kNvwalMaxWorkers or less.
   * Otherwise, nvwal_init() will return an error.
   */
  uint32_t writer_count_;

  /**
   * Byte size of each segment, either on disk or NVDIMM.
   * Must be a multiply of 512.
   * If this is 0 (not set), we automatically set kNvwalDefaultSegmentSize.
   */
  uint64_t segment_size_;

  uint64_t nv_quota_;

  /** Size of (volatile) buffer for each writer-thread. */
  uint64_t writer_buffer_size_;

  /**
   * Buffer of writer_buffer_size bytes for each writer-thread,
   * allocated/deallocated by the client application.
   * The client application must provide it as of nvwal_init().
   * If any of writer_buffers[0] to writer_buffers[writer_count - 1]
   * is null, nvwal_init() will return an error.
   */
  nvwal_byte_t* writer_buffers_[kNvwalMaxWorkers];

  /** 
   * Byte size of meta-data store page.
   * Must be a multiple of 512.
   * If this is 0 (not set), we automatically set kNvwalMdsPageSize.
   */
  uint64_t mds_page_size_;
};

/**
 * @brief Represents a region in a writer's private log buffer for one epoch
 * @details
 * NvwalWriterContext maintains a circular window of this object to
 * communicate with the flusher thread, keeping track of
 * the status of one writer's volatile buffer.
 *
 * This object represents the writer's log region in one epoch,
 * consisting of two offsets. These offsets might wrap around.
 * head==offset iff there is no log in the epoch.
 * To guarantee this, we make sure the buffer never becomes really full.
 *
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing.
 */
struct NvwalWriterEpochFrame {
  /**
   * Inclusive beginning offset in buffer marking where logs in this epoch start.
   * Always written by the writer itself only. Always read by the flusher only.
   */
  uint64_t head_offset_;

  /**
   * Exclusive ending offset in buffer marking where logs in this epoch end.
   * Always written by the writer itself only. Read by the flusher and the writer.
   */
  uint64_t tail_offset_;

  /**
   * The epoch this frame currently represents. As these frames are
   * reused in a circular fashion, it will be reset to kNvwalInvalidEpoch
   * when it is definitely not used, and then reused.
   * Loading/storing onto this variable must be careful on memory ordering.
   *
   * Always written by the writer itself only. Read by the flusher and the writer.
   */
  nvwal_epoch_t log_epoch_;
};

/**
 * DESCRIBE ME
 *
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing. All pointers in
 * this object just point to an existing buffer, in other words they
 * are just markers (TODO: does it have to be a pointer? how about offset).
 */
struct NvwalWriterContext {
  /** Back pointer to the parent WAL context. */
  struct NvwalContext* parent_;

  /**
   * Circular frames of this writer's offset marks.
   * @see kNvwalEpochFrameCount
   */
  struct NvwalWriterEpochFrame epoch_frames_[kNvwalEpochFrameCount];

  /**
   * Points to the oldest frame this writer is aware of.
   * This variable is written by the flusher only.
   * @invariant epoch_frames[oldest_frame].log_epoch != kNvwalInvalidEpoch.
   * To satisfy this invariant, we initialize all writers' first frame during initialization.
   */
  uint32_t oldest_frame_;

  /**
   * Points to the newest frame this writer is using, which is also the only frame
   * this writer is now putting logs to.
   * This variable is read/written by the writer only.
   * @invariant epoch_frames[active_frame].log_epoch != kNvwalInvalidEpoch.
   * To satisfy this invariant, we initialize all writers' first frame during initialization.
   */
  uint32_t active_frame_;

  /**
   * Sequence unique among the same parent WAL context, 0 means the first writer.
   * This is not unique among writers on different WAL contexts,
   * @invariant this == parent->writers + writer_seq_id
   */
  uint32_t writer_seq_id_;

  /**
   * This is read/written only by the writer itself, not from the flusher.
   * This is just same as the value of tail_offset in the last active frame.
   * We duplicate it here to ease maitaining the tail value, especially when we are
   * making a new frame.
   */
  uint64_t last_tail_offset_;

  /**
    * Everything up to this point has been copied by the flusher thread but
    * might not yet be durable.
    * Pending work is everything between copied and writer->complete.
    */
  uint64_t copied_offset_;

  /** Shorthand for parent->config.writer_buffers[writer_seq_id] */
  nvwal_byte_t* buffer_;
};

/**
 * @brief A log segment libnvwal is writing to, copying from, or reading from.
 * @details
 *
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing \b except \b file \b descriptors and \b mmap.
 */
struct NvwalLogSegment {
  /** Back pointer to the parent WAL context. */
  struct NvwalContext* parent_;

  /**
   * mmap-ed virtual address for this segment on NVDIMM.
   * Both MAP_FAILED and NULL mean an invalid VA.
   * When it is a valid VA, libnvwal is responsible to unmap it during uninit.
   */
  nvwal_byte_t* nv_baseaddr_;

  /**
   * When this segment is populated and then copied to disk,
   * this will be the ID of the disk-resident segment.
   * This is bumped up without race when the segment object is recycled for next use.
   */
  nvwal_dsid_t dsid_;

  /**
   * When this segment is populated and ready for copying to disk,
   * the flusher sets this variable to notify fsyncher.
   * fsyncher does nothing while this variable is 0.
   * Resets to 0 without race when the segment object is recycled for next use.
   */
  uint8_t fsync_requested_;

  /**
   * When this segment is durably copied to disk,
   * the fsyncer sets this variable to notify flusher.
   * Resets to 0 without race when the segment object is recycled for next use.
   */
  uint8_t fsync_completed_;

  /**
   * If fsyncer had any error while copying this segment to disk, the error code.
   */
  nvwal_error_t fsync_error_;

  /**
   * File descriptor on NVDIMM.
   * Both -1 and 0 mean an invalid descriptor.
   * When it is a valid FD, libnvwal is responsible to close it during uninit.
   */
  int64_t nv_fd_;
  /*
  we don't retain FD on disk. it's a local variable opened/used, then immediately
  closed by the fsyncer. simpler!
  int64_t disk_fd_;
  */
};

/**
 * @brief Represents a context of a meta-data-store I/O subsystem instance.
 */
struct NvwalMdsIoContext {
  /** Runtime configuration parameters */
  struct NvwalConfig config_;

  /** Active (open) page files */
  struct PageFile* active_files_[kNvwalMdsMaxActivePagefiles];

  /** Buffers */
  struct NvwalMdsBuffer* write_buffers_[kNvwalMdsMaxActivePagefiles];
};

/**
 * @brief Represents a context of a meta-data-store buffer-manager instance.
 */
struct NvwalMdsBufferManagerContext {
  /** Runtime configuration parameters */
  struct NvwalConfig config_;

  /** Buffers */
  struct NvwalMdsBuffer* write_buffers_[kNvwalMdsMaxActivePagefiles];
};

/**
 * @brief Represents a context of a meta-data store instance.
 */
struct NvwalMdsContext {
  /** Runtime configuration parameters */
  struct NvwalConfig config_;

  /** IO subsystem context */
  struct NvwalMdsIoContext io_;

  /** Buffer manager context */
  struct NvwalMdsBufferManagerContext bufmgr_;   

  /** Latest epoch */
  nvwal_epoch_t latest_epoch_;
};

/**
 * @brief Represents the context of the reading API for retrieving prior
 * epoch data. Must be initialized/uninitialized via nvwal_reader_init()
 * and nvwal_reader_uninit().
 */
struct NvwalReaderContext {
  struct NvwalContext* wal_;
  nvwal_epoch_t prev_epoch_; /* The epoch most recently requested and fetched */
  nvwal_epoch_t tail_epoch_; /* The largest epoch number we've prefetched */
  uint8_t fetch_complete_; /* Do we have to break the epoch into multiple mappings? */
  nvwal_dsid_t seg_id_; /* The last segment we tried to mmap */
  char *mmap_start_; /* Remember our mmap info for munmap later */
  uint64_t mmap_len_; /* We only remember info for one mapping. If the epoch needs
                      * multiple mappings, unmap the previous mapping before
                      * mapping the next chuck. */ 
};

/**
 * @brief Represents a context of \b one stream of write-ahead-log placed in
 * NVDIMM and secondary device.
 * @details
 * Each NvwalContext instance must be initialized by nvwal_init() and cleaned up by
 * nvwal_uninit().
 * Client programs that do distributed logging will instantiate
 * an arbitrary number of this context, one for each log stream.
 * There is no interection between two NvwalContext from libnvwal's standpoint.
 * It's the client program's responsibility to coordinate them.
 *
 * @note This object is a POD. It can be simply initialized by memzero,
 * copied by memcpy, and no need to free any thing \b except \b file \b descriptors.
 * @note To be a POD, however, this object conservatively consumes a bit large memory.
 * We recommend allocating this object on heap rather than on stack. Although
 * it's very unlikely to have this long-living object on stack anyways (unit test?)..
 */
struct NvwalContext {
  /**
   * DE of this WAL instance.
   * All logs in this epoch are durable at least on NVDIMM.
   */
  nvwal_epoch_t durable_epoch_;
  /**
   * SE of this WAL instance.
   * Writers won't submit logs in this epoch or earlier.
   * @invariant stable_ == durable_ || stable_ == durable_ + 1
   */
  nvwal_epoch_t stable_epoch_;
  /**
   * NE of this WAL instance.
   * logs in this epoch can be written to files.
   */
  nvwal_epoch_t next_epoch_;

  /**
   * All static configurations given by the user on initializing this WAL instance.
   * Once constructed, this is/must-be const. Do not modify it yourself!
   */
  struct NvwalConfig config_;

  /**
   * Maintains state of each log segment in this WAL instance.
   * Only up to log_segments_[segment_count_ - 1] are used.
   */
  struct NvwalLogSegment segments_[kNvwalMaxActiveSegments];

  /**
   * Number of segments this WAL instance uses.
   * Immutable once constructed. Do not modify it yourself!
   * @invariant segment_count_ <= kNvwalMaxActiveSegments
   */
  uint32_t segment_count_;

  /** Index into segment[] */
  uint32_t cur_seg_idx_;

  struct NvwalWriterContext writers_[kNvwalMaxWorkers];

  /**
   * Controls the state of flusher thread.
   * One of the values in NvwalThreadState.
   */
  uint8_t flusher_thread_state_;

  /**
   * Controls the state of fsyncer thread.
   * One of the values in NvwalThreadState.
   */
  uint8_t fsyncer_thread_state_;

  /**
   * Metadata store context 
   */
  struct NvwalMdsContext mds_;

  /**
   * Reader context
   */
  struct NvwalReaderContext reader_;
};

/** @} */

#endif  /* NVWAL_TYPES_H_ */
