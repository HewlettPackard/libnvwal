
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "nvwal_api.h"
#include "nvwal_atomics.h"


nvwal_error_t nvwal_init(
  const struct nvwal_config* wal_config,
  struct nvwal_context* wal_context) {
  WD * wal;
  int nv_root_fd;
  int block_root_fd;
  unsigned long long i;
  struct stat file_stats;

  wal = malloc(sizeof *wal);
  memset(wal, 0, sizeof(*wal));

  pthread_mutex_init(&wal->mutex, 0);

  wal->log_root = strdup(log_path);
  if (circular_log) {
    fprintf(stderr, "Circular log not supported\n");
    exit(-1);
  } else {
    wal->max_log_size = 0;

    wal->log_descriptor = malloc(sizeof(int) * wal->num_segments);
    memset(wal->log_descriptor, -1, sizeof(int) * wal->num_segments);
  }

  wal->nv_root = strdup(nv_root);
  wal->nv_quota = nv_quota;
  wal->num_segments = nv_quota / NV_SEG_SIZE;

  if (nv_quota < 2*NV_SEG_SIZE) {
    fprintf(stderr,
            "Error: nv_quota less than 2*NV_SEG_SIZE (%lu)\n",
            2*NV_SEG_SIZE);
    exit(-1);
  }

  wal->nv_descriptor = malloc(sizeof(int) * wal->num_segments);
  memset(wal->nv_descriptor, -1, sizeof(int) * wal->num_segments);
  wal->nv_baseaddr = malloc(sizeof(void *) * wal->num_segments);
  memset(wal->nv_baseaddr, 0, sizeof(void *) * wal->num_segments);

  nv_root_fd = open(nv_root, O_DIRECTORY|O_RDONLY);
  ASSERT_FD_VALID(nv_root_fd);
  block_root_fd = open(secondary_root, O_DIRECTORY|O_RDONLY);
  ASSERT_FD_VALID(block_root_fd);

  /* Initialize all nv segments */

  for (i = 0; i < wal->num_segments; i++) {
    init_nvram_segment(wal, nv_root_fd, i);
  }

  /* Created the files, fsync parent directory */
  fsync(nv_root_fd);

  wal->cur_region = nv_baseaddr[0];

  /*
    * Set up some flusher thread stuff?
    */

  return wal;
}

nvwal_error_t nvwal_uninit(
  struct nvwal_context* wal_context) {
  return 0;
}

nvwal_error_t nvwal_register_writer(
  struct nvwal_context* wal_context,
  struct nvwal_buffer_control_block* write_buffer,
  struct nvwal_writer_info* wal_writer) {
  WInfo * new_writer;

  new_writer = malloc(sizeof(*new_writer));
  memset(new_writer, 0, sizeof(*new_writer));
  new_writer->writer = write_buffer;
  new_writer->flushed = write_buffer->head;
  new_writer->copied = write_buffer->head;

  pthread_mutex_lock(&wal->mutex);

  if (wal->writer_list == 0) {
      wal->writer_list = new_writer;
      new_writer->next = new_writer;
  } else {
      new_writer->next = wal->writer_list->next;
      wal->writer_list->next = new_writer;
  }

  pthread_mutex_unlock(&wal->mutex);
}

void process_one_writer(WD * wal, WInfo * winfo)
{
    byte * complete = cur_winfo->writer->complete;
    byte * copied = cur_winfo->copied;
    size_t size = cur_winfo->writer->buffer_size;
    byte * end = cur_winfo->writer->buffer + size;
    size_t len;
    int found_work = 0;

    // If they are not equal, there's work to do since complete
    // cannot be behind copied

    while(complete != copied) {
        found_work = 1;

        // Figure out how much we can copy linearly
        total_length = circular_size(copied, complete, size);
        nvseg_remaining = NV_SEG_SIZE - wal->nv_offset;
        writer_length = end - copied;

        len = MIN(MIN(total_length,nvseg_remaining),writer_length);

        pmem_memcpy_nodrain(wal->cur_region+nv_offset, copied, len);

        // Record some metadata here?

        // Update pointers
        copied += len;
        assert(copied <= end);
        if (copied == end) {
            // wrap around
            copied = cur_winfo->writer->buffer;
        }

        wal->nv_offset += len;

        assert(wal->nv_offset <= NV_SEG_SIZE);
        if (wal->nv_offset == NV_SEG_SIZE) {

            log_segment * cur_segment = wal->segment[wal->cur_seg_idx];
            int next_seg_idx = wal->cur_seg_idx + 1 % wal->num_segments;
            log_segment * next_segment = wal->segment[next_seg_idx];

            /* Transition current active segment to complete */
            assert(cur_segment->state == SEG_ACTIVE);
            cur_segment->state = SEG_COMPLETE;
            submit_write(cur_segment);

            /* Transition next segment to active */
            if (next_segment->state != SEG_UNUSED) {
                /* Should be at least submitted */
                assert(next_segment->state >= SEG_SUBMITTED);
                if (wal->flags & BG_FSYNC_THREAD) {
                    /* spin or sleep waiting for completion */
                } else {
                    sync_backing_file(wal, next_segment);
                }
                assert(next_segment->state == SEG_UNUSED);
            }

            assert(cur_segment->state >= SEG_SUBMITTED);

            /* Ok, we're done with the old cur_segment */
            cur_segment = next_segment;

            /* This isn't strictly necessary until we want to start
             * flushing out data, but might as well be done here. The
             * hard work can be done in batches, this function might
             * just pull an already-open descriptor out of a pool. */
            allocate_backing_file(cur_segment);

            wal->cur_seg_idx = next_seg_idx;
            wal->cur_region = cur_segment->nv_baseaddr;
            wal->nv_offset = 0;

            cur_segment->state = SEG_ACTIVE;

        }
    }

    return found_work;
}

void * flusher_thread_main(void * arg)
{
    WD * wal = arg;

    pthread_mutex_lock(&wal->mutex);

    if (wal->writer_list == 0) {
        fprintf(stderr, "No writers registered for flushing!\n");
        pthread_mutex_unlock(&wal->mutex);
        pthread_exit(0);
    }

    while(1) {

        /* Look for work */
        WInfo * cur_winfo = wal->writer_list;
        int found_work = 0;

        do {

            found_work = process_one_writer(wal, cur_winfo);

            cur_winfo = cur_winfo->next;

        } while (cur_winfo != wal->writer_list);

        /* Ensure writes are durable in NVM */
        pmem_drain();

        /* Some kind of metadata commit, we could use libpmemlog.
         * This needs to track which epochs are durable, what's on disk
         * etc. */

        //commit_metadata_updates(wal)

        // maybe wake sleeping writer threads?

        pthread_mutex_unlock(&wal->mutex);
        //Needs a fair lock instead so waiting writers can be added
        //or maybe even removed
        pthread_mutex_lock(&wal->mutex);
    }
    pthread_mutex_unlock(&wal->mutex);
    return 0;
}

void * fsync_thread_main(void * arg)
{
    WD * wal = arg;

    while(1) {
        int i;
        int found_work = 0;

        for(i = 0; i < wal->num_segments; i++) {
            if (wal->segments[i].state == SEG_SUBMITTED) {
                sync_backing_file(wal, &wal->segments[i]);
            }
        }
    }
}

nvwal_error_t nvwal_on_wal_write(
  struct nvwal_context* wal_context,
  struct nvwal_writer_info* wal_writer,
  const void* src,
  uint64_t size_to_write,
  nvwal_epoch_t current)

/*
 * Very simple version which does not allow gaps in the volatile log
 * and does not check possible invariant violations e.g. tail-catching
 */

{
    if (src != winfo->writer->complete) {
        fprintf(stderr, "Error: src %p complete %p\n",
                src,
                winfo->writer->complete);
        exit(-1);
    }

    /* Do something with current epoch and wal->latest
     * Is this where we might want to force a commit if epoch
     * is getting ahead of our durability horizon? */
    assert(winfo->writer->latest_written <= current);
    winfo->writer->latest_written = current;

    /* Bump complete pointer and wrap around if necessary */
    complete = complete + size;
    if (complete >= winfo->writer->buffer + winfo->writer->buffer_size) {
        complete = complete - winfo->writer->buffer_size;
    }

    /* Perhaps do something if we have a sleepy flusher thread */

    return 0;
}

nvwal_error_t nvwal_assure_wal_space(
  struct nvwal_context* wal_context,
  struct nvwal_writer_info* wal_writer) {
    /*
      check size > circular_size(tail, head, size). Note the reversal
      of the usual parameter ordering, since we're looking for *free*
      space in the log. We could also consider circular_size(tail, durable).

      if it's bad, we probably want to do something like:
      check
      busy wait
      check
      nanosleep
      check
      grab (ticket?) lock
      while !check
      cond_wait
      drop lock
    */
}

void init_nvram_segment(WD * wal, int root_fd, int i)
{

    log_segment * seg = &wal->segment[i];
    int fd;
    char filename[256];
    void * baseaddr;

    snprintf(filename, "nvwal-data-%lu", i);
    fd = openat(root_fd, filename, O_CREAT|O_RDWR);
    ASSERT_FD_VALID(fd);

    //posix_fallocate doesn't set errno, do it ourselves
    errno = posix_fallocate(fd, 0, NV_SEG_SIZE);
    ASSERT_NO_ERROR(err);
    err = ftruncate(fd, NV_SEG_SIZE);
    ASSERT_NO_ERROR(err);
    fsync(fd);

    /* First try for a hugetlb mapping */
    baseaddr = mmap(0,
                    NV_SEG_SIZE,
                    PROT_READ|PROT_WRITE,
                    MAP_SHARED|MAP_PREALLOCATE|MAP_HUGETLB
                    fd,
                    0);

    if (baseaddr == MAP_FAILED && errno == EINVAL) {

        /* If that didn't work, try for a non-hugetlb mapping */
        printf(stderr, "Failed hugetlb mapping\n");
        baseaddr = mmap(0,
                        NV_SEG_SIZE,
                        PROT_READ|PROT_WRITE,
                        MAP_SHARED|MAP_PREALLOC,
                        fd,
                        0);
    }
    /* If that didn't work then bail. */
    ASSERT_NO_ERROR(baseaddr == MAP_FAILED);

    /* Even with fallocate we don't trust the metadata to be stable
     * enough for userspace durability. Actually write some bytes! */

    memset(baseaddr, 0x42, NV_SEG_SIZE);
    msync(fd);

    seg->seq = INVALID_SEQNUM;
    seg->nvram_fd = fd;
    seg->disk_fd = -1;
    seg->nv_baseaddr = baseaddr;
    seg->state = SEG_UNUSED;
    seg->dir_synced = 0;
    seg->disk_offset = 0;

}

void submit_write(WD * wal, log_segment * seg)
{

    int bytes_written;

    assert(seg->state == SEG_COMPLETE);
    /* kick off write of old segment
     * This should be wrapped up in some function,
     * begin_segment_write or so. */

    bytes_written = write(seg->disk_fd,
                          seg->nv_baseaddr,
                          NV_SEG_SIZE);

    ASSERT_NO_ERROR(bytes_written != NV_SEG_SIZE);

    seg->state = SEG_SUBMITTED;
}


void sync_backing_file(WD * wal, log_segment * seg) {

    assert(seg->state >= SEG_SUBMITTED);

    /* kick off the fsync ourselves */
    seg->state = SEG_SYNCING;
    fsync(seg->disk_fd);

    /* check file's dirfsync status, should be
     * guaranteed in this model */
    assert(seg->dir_synced);
    seg->state = SEG_SYNCED;

    /* Update durable epoch marker? */

    //wal->durable = seg->latest - 2;

    /* Notify anyone waiting on durable epoch? */

    /* Clean up. */
    close(seg->disk_fd);
    seg->disk_fd = -1;
    seg->state = SEG_UNUSED;
    seg->seq = 0;
}

void allocate_backing_file(WD * wal, log_segment * seg)
{
    size_t our_sequence = wal->log_sequence++;
    int our_fd = -1;
    int i = 0;
    char filename[256];

    if (our_sequence % PREALLOC_FILE_COUNT == 0) {
        for(i = our_sequence; i < our_sequence + PREALLOC_FILE_COUNT; i++) {
            int fd;

            snprintf(filename, "log-segment-%lu", i);
            fd = openat(wal->log_root_fd,
                        filename,
                        O_CREAT|O_RDWR|O_TRUNC,
                        S_IRUSR|S_IWUSR);

            ASSERT_FD_VALID(fd);
            /* Now would be a good time to hint to the FS using
             * fallocate or whatever. */

            /* Clean up */

            /* We should really just stash these fds in a pool */
            close(fd);

        }

        /* Sync the directory, so that all these newly created (empty)
         * files are visible.
         * We may want to take care of this in the fsync thread instead
         * and set the dir_synced flag on the segment descriptor */
        fsync(wal->log_root_fd);

    }

    /* The file should already exist */
    snprintf(filename, "log-segment-%lu", our_sequence);
    our_fd = openat(wal->log_root_fd,
                    filename,
                    O_RDWR|O_TRUNC);

    ASSERT_FD_VALID(our_fd);

}
