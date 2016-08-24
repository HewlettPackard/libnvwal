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
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <libpmem.h>

#include "nvwal_types.h"
#include "nvwal_mds.h"

#define ASSERT_FD_VALID assert

#define MDS_NVRAM_BUFFER_FILE_PREFIX  "mds-nvram-buffer-"
#define MDS_PAGE_FILE_PREFIX          "mds-pagefile-"

#define FILENAME_MAX_LEN              256
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
 *
 * METADATA-STORE BUFFER MANAGER 
 * 
 *****************************************************************************/

typedef uint64_t page_no_t;
typedef uint64_t file_no_t;


struct PageFile {
  int fd;
};


struct BufferManager {
  int root_fd_;  
};


/**
 * @brief Represents a buffer frame. 
 * 
 * @details 
 * The frame can be either persistent (mapped to NVRAM) or 
 * volatile (mapped to DRAM).
 */
struct Buffer {
  struct PageFile file_;
  page_no_t       page_no;
  int             nvram_fd;
  void*           base_addr;
  int             writable;
};



nvwal_error_t bfmgr_init() 
{
  /* TODO: implemement me */
  return 0;
}


/*
 *  should be able to reload last durable buffer and detect range of durable epoch records by scanning the buffer's backing file.
 */
nvwal_error_t bfmgr_recover()
{
  /* TODO: implemement me */
  return 0;
}

nvwal_error_t bfmgr_alloc_buffer(struct BufferManager* bfmgr, file_no_t file_no, page_no_t page_no)
{
  /* TODO: implemement me */
  return 0;
}

nvwal_error_t alloc_nvram_buffer(struct BufferManager* bfmgr, struct Buffer* buffer)
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


nvwal_error_t map_nvram_buffer(struct BufferManager* bfmgr, struct Buffer* buffer)
{
  /* TODO: implemement me */
#if 0
  void* baseaddr;
  int fd;

  baseaddr = mmap(0,
                  kNvwalMdsPageSize,
                  PROT_READ|PROT_WRITE,
                  MAP_SHARED|MAP_POPULATE,
                  fd,
                  0);
#endif  
  return 0;
}


void bfmgr_read_page(struct BufferManager* bfmgr, page_no_t pgno, struct Buffer** bf) 
{
  /* TODO: implemement me */
}


void bfmgr_write_page(struct BufferManager* bfmgr, struct Buffer* bf) 
{
  /* TODO: implemement me */
}





nvwal_error_t mds_init(
  const struct NvwalConfig* config, 
  struct MdsContext* mds) 
{
/*
 just do some barebones initialization 
 do not restore any nvram buffers. this is done as part of a separate recover function

 takes nv_root_fd folder where to store durable buffer and block_root_fd folder where to store disk pages
 or instead of file descriptors, it takes paths
 
  
 if no durable buffer, then allocate one
 */
  /* TODO: implemement me */
  return 0;
}


nvwal_error_t mds_uninit(struct MdsContext* mds)
{
  /* TODO: implemement me */
  return 0;
}


nvwal_error_t mds_recover(struct MdsContext* mds) 
{
/*
 perform recovery:
   restore nvram buffers
   locate most latest durable epoch in nvram buffers
   find the latest disk metadata page
 
*/ 

  /* TODO: implemement me */
  return 0;
}

void mds_read_epoch(struct MdsContext* mds, nvwal_epoch_t epoch_id, struct EpochMetadata* epoch_metadata)
{
  /* TODO: implemement me */
}


/*
 * QUESTIONS:
 * - does user explicitly provide the epoch_id to write metadata for?
 * - is epoch_metadata opaque to the user or do we open up the struct?
 * - do we support random write or just append?
 */
nvwal_error_t mds_write_epoch(struct MdsContext* mds, nvwal_epoch_t epoch_id, struct EpochMetadata* epoch_metadata)
{
  /*
    epoch_id -> <file, page_no>
    locate buffer frame holding  <file, page_no>
    if no buffer, then alloc buffer 
    now we have a buffer, write epoch record to nvram buffer, sync the nvram buffer. 
  */
  /* TODO: implemement me */
  return 0;
}


nvwal_error_t __mds_open_pagefile(struct MdsContext* mds, file_no_t file_no)
{
  int fd = -1;
  char filename[FILENAME_MAX_LEN];

  snprintf(filename, FILENAME_MAX_LEN, "%s%lu", MDS_PAGE_FILE_PREFIX, file_no);
  fd = openat(mds->block_root_fd_,
              filename,
              O_RDWR|O_APPEND);

  ASSERT_FD_VALID(fd);
  return 0;
}


nvwal_error_t __mds_create_pagefile(struct MdsContext* mds, file_no_t file_no)
{
  int fd = -1;
  char filename[FILENAME_MAX_LEN];

  snprintf(filename, FILENAME_MAX_LEN, "%s%lu", MDS_PAGE_FILE_PREFIX, file_no);
  fd = openat(mds->block_root_fd_,
              filename,
              O_CREAT|O_RDWR|O_TRUNC|O_APPEND,
              S_IRUSR|S_IWUSR);

  ASSERT_FD_VALID(fd);

  close(fd);

  /* 
   * Sync the directory, so that the newly created (empty) file is 
   * visible.
   */
  fsync(mds->block_root_fd_);
  
  return 0;
}


void init_buffer() {
  /* TODO: implemement me */
}


void sync_backing_file()
{
  /* TODO: implemement me */
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




void open_and_map_nvram_file(
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