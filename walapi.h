/**
 * walapi.h
 *
 * Interface to a simple WAL facility optimized for NVRAM
 *
 */

typedef unsigned long long epoch_t;
typedef unsigned char byte;
typedef int bool;

typedef enum {
    SEG_UNUSED = 0,
    SEG_ACTIVE,
    SEG_COMPLETE,
    SEG_SUBMITTED,
    SEG_SYNCING,
    SEG_SYNCED
} seg_state_t;

typedef struct log_segment {
    void * nv_baseaddr;

    /* On-disk sequence number, identifies filename */
    unsigned long long seq;
    int nvram_fd;
    int disk_fd;

    /* May be used for communication between flusher thread and fsync thread */
    seg_state_t state;

    /* true if direntry of disk_fd is durable */
    bool dir_synced;
} log_segment;

typedef struct wal_descriptor {

    epoch_t durable;
    epoch_t latest;

    char * nv_root;
    size_t nv_quota;
    int num_segments;

    log_segment * segment;

    /* Path to log on block storage */
    char * log_root;
    int log_root_fd;

    /* 0 if append-only log */
    size_t max_log_size;

    /* Next on-disk sequence number, see log_segment.seq  */
    size_t log_sequence;

    /* mmapped address of current nv segment*/
    byte * cur_region;

    /* Index into nv_region */
    size_t nv_offset;

    /* Index into segment[] */
    int cur_seg_idx;

    /* To protect writer list */
    pthread_mutex_t mutex;
    WInfo * writer_list;

} WD;


/*
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

typedef struct buffer_control_block {
    /* Updated by writer via on_wal_write() and read by flusher thread.  */

    /* Where the writer will put new bytes - we don't currently read this */
    byte * tail;

    /* Beginning of completely written bytes */
    byte * head;

    /* End of completely written bytes */
    byte * complete;

    /* max of epochs seen by on_wal_write() */
    epoch_t latest_written;

    /* Size of (volatile) buffer. Do not change after registration! */
    size_t buffer_size;

    /* Buffer of buffer_size bytes, allocated by application */
    byte * buffer;
} BufCB;

typedef struct writer_info {
    /* Updated by flusher, read by writer */
    struct writer_info * next;
    BufCB * writer;

    /* Everything up to this point is durable. It is safe for the application
     * to move the head up to this point. */
    byte * flushed;

    /* Everything up to this point has been copied by the flusher thread but
     * might not yet be durable */
    byte * copied;

    /* Pending work is everything between copied and writer->complete */
} WInfo;

/* Ignored. */
enum NVWALConfigFlags {
    BG_FSYNC_THREAD = 1 << 0,
    MMAP_DISK_FILE  = 1 << 1,
    CIRCULAR_LOG    = 1 << 2
};

typedef struct {
    char * nv_root;
    int numa_domain;

    /* How big our nvram segments are */
    size_t nv_seg_size;
    size_t nv_quota;

    char * log_path;

    /* Assumed to be the same as nv_seg_size for now */
    size_t disk_seg_size;

    /* How many on-disk log segments to create a time (empty files)
     * This reduces the number of times we have to fsync() the directory */
    int prealloc_file_count;

    int option_flags;

} NVWALConfig;

error_t initialize_nvwal(WD ** wal, NVWALConfig * config, bool resume);

error_t register_writer(WInfo **WD * wal, BufCB * write_buffer);

error_t on_wal_write(WD* wal,
                     WInfo * writer,
                     byte * src,
                     size_t size,
                     epoch_t current
                     );

error_t assure_wal_space(WD * wal, WInfo * writer, size_t size);

/* Is this needed? Or can we just read wal->durable? */
epoch_t query_durable_epoch(WD * wal);
