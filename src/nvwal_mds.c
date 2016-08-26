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

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <libpmem.h>

#include "nvwal_mds.h"
#include "nvwal_types.h"
#include "nvwal_util.h"


#define ASSERT_FD_VALID(fd) assert(fd != -1)

#define MDS_NVRAM_BUFFER_FILE_PREFIX  "mds-nvram-buffer-"
#define MDS_PAGE_FILE_PREFIX          "mds-pagefile-"

/*
 * QUESTIONS:
 * -thread safety: does user ensure thread safety or the metadata store?
 * -buffering: 
     - one nvram buffer for writing (appending?), and one dram buffer for reading 
       - buffers are non overlapping
     - nvram buffer: 
        - used for writing but also supports reading 
        - as large as the segment
        - durable: mmaped to a nvram file 
     - dram buffer 
        - used for reading only
        - smaller than segment (prefetching size)
        - volatile: malloced or mmaped to anonymous memory

 *  - for the nvram buffer, do we need any durable header that is stored in nvram. 
 *  - we should probably avoid something that we need to update frequently to avoid 
 *    extra ordering. for example, we don't need to keep track of the head/tail if 
 *    we have a way to infer this from the metadata. if we start with a buffer that is
 *    always zero, then we can scan for valid log records based on a non-zero flag. 
 */

/*
  Code should be NUMA aware
 */


int strcat_s(char *dest, size_t destsz, const char* src)
{
  if (strlen(src) + strlen(dest) + 1 > destsz) {
    return 1;
  } 
  strcat(dest, src);
  return 0;
}

/******************************************************************************
 * Meta-data-store I/O helper methods
 *****************************************************************************/

typedef uint64_t page_no_t;
typedef uint64_t file_no_t;

/*/  
 * @brief Page-file descriptor structure.
 */
struct PageFile {
  file_no_t file_no_;
  int       refcnt_;
  int       fd_;
};


/**
 * @brief Returns page file descriptor of an active/open file.
 *
 * @details
 * If the file is not open/active then the function returns null.
 * 
 * @note The current implementation relies on simply scanning an 
 * array so it is obviously not efficient for a large number of 
 * active files. 
 */
static struct PageFile* mds_io_lookup_file_desc(
  struct NvwalMdsContext* mds, 
  file_no_t file_no)
{
  int i;
  struct PageFile* found_pf = NULL;

  for (i=0; i < kNvwalMdsMaxActivePagefiles; i++) {
    struct PageFile* pf = mds->active_files_[i];
    if (pf->file_no_ == file_no) {
      found_pf = pf;
      break;
    }
  }
  return found_pf;
}


/**
 * @brief Adds page file descriptor to table of active/open files.
 * 
 * @details
 * Assumes the caller has already checked that the page file descriptor
 * does not exist in the table.
 */
static nvwal_error_t mds_io_add_file_desc(
  struct NvwalMdsContext* mds,
  struct PageFile* file)
{
  int i;
  int empty_slot = -1;

  for (i=0; i < kNvwalMdsMaxActivePagefiles; i++) {
    struct PageFile* pf = mds->active_files_[i];
    if (!pf) {
      empty_slot = i;
      break;
    }
  }
  if (empty_slot < 0) {
    return nvwal_raise_einval("Error: no free space in table of active files\n");
  }
  mds->active_files_[empty_slot] = file;
  return 0; 
}


/**
 * @brief Removes page file descriptor from table of known active/open files.
 */
static void mds_io_remove_file_desc(
  struct NvwalMdsContext* mds,
  struct PageFile* file)
{
  int i;

  for (i=0; i < kNvwalMdsMaxActivePagefiles; i++) {
    struct PageFile* pf = mds->active_files_[i];
    if (pf->file_no_ == file->file_no_) {
      mds->active_files_[i] = NULL;
      break;
    }
  }
}


/**
 * @brief Allocates and initializes a page-file descriptor.
 * 
 * @details
 * Memory allocated for the returned descriptor can be freed using free().
 */
static struct PageFile* alloc_page_file_desc(file_no_t file_no, int fd)
{
  struct PageFile* file;

  if (!(file = malloc(sizeof(*file)))) {
    return NULL;
  }

  file->file_no_ = file_no;
  file->refcnt_ = 1;
  file->fd_ = fd;
  
  return file;
}


static nvwal_error_t mds_io_init(struct NvwalMdsContext* mds)
{
  int i;

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    mds->active_files_[i] = NULL;   
  }

  return 0;
}

/**
 * @brief Opens a page file and provides a page-file descriptor for this file.
 *  
 * FIXME: Use direct i/o
 */
static nvwal_error_t mds_io_open_file(
  struct NvwalMdsContext* mds, 
  file_no_t file_no,
  struct PageFile** file)
{
  nvwal_error_t ret;
  int fd = -1;
  char pathname[kNvwalMaxPathLength];
  
  if ((*file = mds_io_lookup_file_desc(mds, file_no))) {
    assert((*file)->file_no_ == file_no);
    assert((*file)->fd_ > -1);
    (*file)->refcnt_++;
    return 0;
  }

  nvwal_concat_sequence_filename(
    mds->config_.disk_root_,
    MDS_PAGE_FILE_PREFIX,
    file_no,
    pathname);
    
  fd = open(pathname, O_RDWR|O_APPEND);

  if (fd == -1) {
    /** Failed to open/create the file! */
    ret = errno;
    goto error_return;
  }

  *file = alloc_page_file_desc(file_no, fd);
  assert(*file);
  ret = mds_io_add_file_desc(mds, *file);

  return ret;
 
error_return:
  errno = ret;
  return ret;
}


/**
 * @brief Creates a page file and provides a page-file descriptor for this file.
 */
static nvwal_error_t mds_io_create_file(
  struct NvwalMdsContext* mds, 
  file_no_t file_no, 
  struct PageFile** file)
{
  nvwal_error_t ret;
  int fd = -1;
  char pathname[kNvwalMaxPathLength];

  nvwal_concat_sequence_filename(
    mds->config_.disk_root_,
    MDS_PAGE_FILE_PREFIX,
    file_no,
    pathname);
    
  fd = open(pathname,
            O_CREAT|O_RDWR|O_TRUNC|O_APPEND,
            S_IRUSR|S_IWUSR);

  if (fd == -1) {
    /** Failed to open/create the file! */
    ret = errno;
    goto error_return;
  }

  /* 
   * Sync the parent directory, so that the newly created (empty) file is visible.
   */
  ret = nvwal_open_and_fsync(mds->config_.disk_root_);
  if (ret) {
    goto error_return;
  }
 
  *file = alloc_page_file_desc(file_no, fd);
  assert(*file);
  ret = mds_io_add_file_desc(mds, *file);

  return ret;
 
error_return:
  errno = ret;
  return ret;
}

/**
 * @brief Closes a page file.
 *
 * @details
 * This function only closes the file if no other File descriptor exist that 
 * access the same file.
 */
static void mds_io_close_file(
  struct NvwalMdsContext* mds,
  struct PageFile* file)
{
  file->refcnt_--;
  if (file->refcnt_ > 0) {
    return;
  }
  close(file->fd_);
  mds_io_remove_file_desc(mds, file);
  free(file);  
}



/******************************************************************************
 * Meta-data-store buffer-manager helper methods
 *****************************************************************************/

/**
 * @brief Represents a buffer frame. 
 * 
 * @details 
 * The frame can be either persistent (mapped to NVRAM) or 
 * volatile (mapped to DRAM).
 */
struct NvwalMdsBuffer {
  struct PageFile* file_;
  page_no_t        page_no;
  int              nv_fd;
  void*            baseaddr;
  int              writable;
};



nvwal_error_t mds_bufmgr_init(
  const struct NvwalConfig* config,
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  bufmgr->buffer_ = NULL;
  return 0;
}


nvwal_error_t create_nvram_buffer_file(
  struct NvwalMdsContext* mds,
  int buffer_id)
{
  nvwal_error_t ret;
  int nv_fd;
  char pathname[256];

  nvwal_concat_sequence_filename(
    mds->config_.nv_root_,
    MDS_NVRAM_BUFFER_FILE_PREFIX,
    buffer_id,
    pathname);
    
  nv_fd = open(pathname,
            O_CREAT|O_RDWR|O_TRUNC,
            S_IRUSR|S_IWUSR);

  if (nv_fd == -1) {
    /** Failed to open/create the file! */
    ret = errno;
    goto error_return;
  }

  /** posix_fallocate doesn't set errno, do it ourselves */
  ret = posix_fallocate(nv_fd, 0, mds->config_.mds_page_size_);
  if (ret) {
    goto error_return;
  }

  fsync(nv_fd);
  close(nv_fd);

  /* 
   * Sync the parent directory, so that the newly created (empty) file is visible.
   */
  ret = nvwal_open_and_fsync(mds->config_.nv_root_);
  if (ret) {
    goto error_return;
  }

  return 0;

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t map_nvram_buffer_file(
  struct NvwalMdsContext* mds,
  int buffer_id,
  void** nv_baseaddr) 
{
  nvwal_error_t ret;
  int nv_fd;
  char pathname[256];

  nvwal_concat_sequence_filename(
    mds->config_.nv_root_,
    MDS_NVRAM_BUFFER_FILE_PREFIX,
    buffer_id,
    pathname);
    
  nv_fd = open(pathname, O_RDWR|O_APPEND);

  if (nv_fd == -1) {
    /** Failed to open/create the file! */
    ret = errno;
    goto error_return;
  }

  /*
   * Don't bother with (non-transparent) huge pages. Even libpmem doesn't try it.
   */
  *nv_baseaddr = mmap(0,
                  mds->config_.mds_page_size_,
                  PROT_READ | PROT_WRITE,
                  MAP_SHARED,
                  nv_fd,
                  0);

  if (*nv_baseaddr == MAP_FAILED) {
    ret = errno;
    goto error_return;
  }
  assert(*nv_baseaddr);

  /* We no longer need the file descriptor as we will be accessing the file 
   * through the memory mapping we just established.
   */
  close(nv_fd);

  return 0;

error_return:
  errno = ret;
  return ret;
}



/*
 *  should be able to reload last ble buffer and detect range of durable epoch records by scanning the buffer's backing file.
 */
nvwal_error_t mds_bufmgr_restore_nvram_buffers()
{
  /* TODO: implement me */
  return 0;
}

nvwal_error_t mds_bufmgr_alloc_buffer(struct NvwalMdsBufferManagerContext* bufmgr, file_no_t file_no, page_no_t page_no)
{
  /* TODO: implement me */
  return 0;
}

nvwal_error_t mds_bufmgr_alloc_nvram_buffer(struct NvwalMdsBufferManagerContext* bufmgr, struct NvwalMdsBuffer* buffer)
{
#if 0
  char pathname[1024];

  full_pathname(dir, "mds-nvram-buffer-", seq, pathname, 1024);
 
  strncpy(
  snprintf(filename, "mds-nvram-buffer-%lu", i);

  fd = openat(root_fd, filename, O_CREAT|O_RDWR);
  ASSERT_FD_VALID(fd);

#endif
  return 0;
}


void mds_bufmgr_read_page(struct NvwalMdsBufferManagerContext* bufmgr, page_no_t pgno, struct NvwalMdsBuffer** bf) 
{
  /* TODO: implement me */
}


void mds_bufmgr_alloc_page(struct NvwalMdsBufferManagerContext* bufmgr, struct NvwalMdsBuffer* bf) 
{
  /* TODO: implement me */
}



/******************************************************************************
 * Metadata store core methods
 *****************************************************************************/


nvwal_error_t mds_init(
  const struct NvwalConfig* config, 
  struct NvwalContext* wal) 
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);

  mds_io_init(mds);
  mds_bufmgr_init(config, bufmgr);
 
  return 0;
}


nvwal_error_t mds_uninit(struct NvwalContext* wal)
{
  /* TODO: implement me */
  return 0;
}


nvwal_error_t mds_recover(struct NvwalContext* wal) 
{
/*
 perform recovery:
   restore nvram buffers
   for each nvram buffer, find epoch id range and connect it to the appropriate page file.

   locate most latest durable epoch in nvram buffers
   find the latest disk metadata page
 
*/ 

  /* TODO: implement me */
  return 0;
}

/**
 * @brief Return the file number of the page file storing metadata for 
 * epoch \a epoch_id.
 * 
 * @note Currently, we support a single page file so we always return 0. 
 */
file_no_t epoch_id_to_file_no(struct NvwalMdsContext* wal, nvwal_epoch_t epoch_id)
{
  return 0;
}


/**
 * @brief Return the descriptor of the page file storing metadata for 
 * epoch \a epoch_id.
 * 
 * @details
 * If the page file is not active then open it 
 *
 * @note Currently, we support a single page file. 
 */
struct PageFile* epoch_id_to_file(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  file_no_t fno = epoch_id_to_file_no(mds, epoch_id);
  struct PageFile* pf = mds->active_files_[fno];
  if (!pf) {
    //open_file(mds, fno, &pf);
  }
  return NULL; 
}


/**
 * @brief Return the page number of the page storing metadata for 
 * epoch \a epoch_id.
 * 
 * @note Currently, we support a single page file.
 */
page_no_t epoch_id_to_page_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  return epoch_id / (mds->config_.mds_page_size_ / sizeof(struct MdsEpochMetadata));  
}


void mds_read_epoch(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id, struct MdsEpochMetadata* epoch_metadata)
{
  /* TODO: implement me */
}


/*
 * QUESTIONS:
 * - does user explicitly provide the epoch_id to write metadata for?
 * - is epoch_metadata opaque to the user or do we open up the struct?
 * - do we support random write or just append?
 */
nvwal_error_t mds_write_epoch(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id, struct MdsEpochMetadata* epoch_metadata)
{
  /*
    epoch_id -> <file, page_no>
    locate buffer frame holding  <file, page_no>
    if no buffer, then alloc buffer 
    now we have a buffer, write epoch record to nvram buffer, sync the nvram buffer. 
  */
  /* TODO: implement me */
  

  return 0;
}




void init_buffer() {
  /* TODO: implement me */
}


void sync_backing_file()
{
  /* TODO: implement me */
}


#if 0
void init_nvram_buffer(
  struct nvwal_context * wal,
  int root_fd,
  int i) {
  struct nvwal_log_segment * seg = &wal->segment[i];
  int fd;
  char filename[256];
  void * baseaddr;

  snprintf(filename, "mds-nvram-buffer-%lu", i);

  fd = openat(root_fd, filename, O_CREAT|O_RDWR);
  ASSERT_FD_VALID(fd);

  //posix_fallocate doesn't set errno, do it ourselves
  errno = posix_fallocate(fd, 0, kNvwalSegmentSize);
  ASSERT_NO_ERROR(err);
  err = ftruncate(fd, kNvwalSegmentSize);
  ASSERT_NO_ERROR(err);
  fsync(fd);

  /* First try for a hugetlb mapping */
  baseaddr = mmap(0,
                  kNvwalSegmentSize,
                  PROT_READ|PROT_WRITE,
                  MAP_SHARED|MAP_PREALLOC|MAP_HUGETLB
                  fd,
                  0);

  if (baseaddr == MAP_FAILED && errno == EINVAL) {
    /* If that didn't work, try for a non-hugetlb mapping */
    printf(stderr, "Failed hugetlb mapping\n");
    baseaddr = mmap(0,
                    kNvwalSegmentSize,
                    PROT_READ|PROT_WRITE,
                    MAP_SHARED|MAP_PREALLOC,
                    fd,
                    0);
  }
  /* If that didn't work then bail. */
  ASSERT_NO_ERROR(baseaddr == MAP_FAILED);

  /* Even with fallocate we don't trust the metadata to be stable
    * enough for userspace durability. Actually write some bytes! */

  memset(baseaddr, 0x42, kNvwalSegmentSize);
  msync(baseaddr, kNvwalSegmentSize, MS_SYNC);

  seg->seq = INVALID_SEQNUM;
  seg->nvram_fd = fd;
  seg->disk_fd = -1;
  seg->nv_baseaddr = baseaddr;
  seg->state = SEG_UNUSED;
  seg->dir_synced = 0;

  /* Is this necessary? I'm assuming this is offset into the disk-backed file.
   * If we have one disk file per nvram segment, we don't need this.
   * When we submit_write a segment, the FS should be tracking the file offset
   * for us anyway. What is this actually used for?
   */
  seg->disk_offset = 0;
}




#endif
