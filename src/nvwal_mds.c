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

#include "nvwal_mds.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <libpmem.h>

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

 TODO:
 Buffer manager
  - upon initialization, the manager remaps any durable buffers.
    but it doesn't associate the buffers with the associated disk page.
    it's the responsibility of the user to assign each buffer to a disk page as this step requires recovery logic (detect whether the page exists).
    this can be done by inspecting the contents of the page and from the contents infer the disk page. for example, the page can include a page header
    with the fileno/pageno. or this information could be derived by the epoch ids as it is the case with epoch metadata.
    a trivial case is a buffer than contains just zeros. 
    in general the buffer manager should provide enough mechanism and interface to enable a client perform recovery.
  - we therefore provide an iterator interface that the user can use to iterate over all buffers and associate each buffer with a disk page when the user performs recovery  
 Recovery:
 - what if a durable buffer contains all zeros. we can reclaim this buffer.
   invariant: we zero a buffer only after we write the buffer out to the disk page file and sync the disk file. 
 */

/*
  TODO: Code should be NUMA aware
 */


/******************************************************************************
 * Declarations for private types and functions 
 *****************************************************************************/

typedef uint64_t page_no_t;
typedef uint64_t file_no_t;

/**  
 * @brief Page-file descriptor structure.
 */
struct PageFile {
  file_no_t file_no_;
  int       fd_;
};

/**
 * @brief Represents a descriptor of a buffer frame mapped on NVRAM. 
 */
struct NvwalMdsBuffer {
  struct PageFile* file_;
  page_no_t        page_no_;
  void*            baseaddr_;
};


/**
 * @brief Initializes the buffer manager of the meta-data store.
 *
 * @details
 * As part of the initialization, the buffer manager remaps any NVRAM
 * buffers. However, the user is still responsible to assign NVRAM
 * buffers to the proper page file based on the recovery protocol
 * followed by the user. 
 */
static nvwal_error_t mds_bufmgr_init(
  const struct NvwalConfig* config, 
  struct NvwalMdsBufferManagerContext* bufmgr);


int strcat_s(char *dest, size_t destsz, const char* src)
{
  if (strlen(src) + strlen(dest) + 1 > destsz) {
    return 1;
  } 
  strcat(dest, src);
  return 0;
}

/******************************************************************************
 * Meta-data store I/O subsystem functions
 *****************************************************************************/

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
  file->fd_ = fd;
  
  return file;
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

  return 0;
 
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

  return ret;
 
error_return:
  errno = ret;
  return ret;
}


/**
 * @brief Closes a page file.
 */
static void mds_io_close_file(
  struct NvwalMdsContext* mds,
  struct PageFile* file)
{
  close(file->fd_);
  free(file);  
}


/**
 * @brief Initializes the I/O subsystem of the meta-data store.
 * 
 * @details
 * Opens metadata page files. If the page files do not exist, it creates them. 
 */
static nvwal_error_t mds_io_init(struct NvwalMdsContext* mds)
{
  int i;
  nvwal_error_t ret;
  struct PageFile* pf;

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    ret = mds_io_open_file(mds, i, &pf);
    if (ret == EACCES) {
      /* page file does not exist; create it */
      ret = mds_io_create_file(mds, i, &pf);
      if (!ret) {
        goto error_return;
      }
    }
    mds->active_files_[i] = pf;
  }

  return 0;

error_return:
  errno = ret;
  return ret;
}


/**
 * @brief Unitializes the I/O subsystem of the meta-data store.
 */
static nvwal_error_t mds_io_uninit(struct NvwalMdsContext* mds)
{
  int i;

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    assert(mds->active_files_[i]);
    mds_io_close_file(mds, mds->active_files_[i]);   
    mds->active_files_[i] = NULL;
  }
  return 0;
}


static nvwal_error_t mds_io_append_page(
  struct PageFile* file,
  page_no_t page_no,
  const void* buf)
{
  /* TODO: implement me */
  return 0; 
}


/******************************************************************************
 * Meta-data-store buffer-manager helper methods
 *****************************************************************************/

static nvwal_error_t create_nvram_buffer_file(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id)
{
  nvwal_error_t ret;
  int nv_fd;
  char pathname[256];

  nvwal_concat_sequence_filename(
    bufmgr->config_.nv_root_,
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
  ret = posix_fallocate(nv_fd, 0, bufmgr->config_.mds_page_size_);
  if (ret) {
    goto error_return;
  }

  fsync(nv_fd);
  close(nv_fd);

  /* 
   * Sync the parent directory, so that the newly created (empty) file is visible.
   */
  ret = nvwal_open_and_fsync(bufmgr->config_.nv_root_);
  if (ret) {
    goto error_return;
  }

  return 0;

error_return:
  errno = ret;
  return ret;
}


static nvwal_error_t map_nvram_buffer_file(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id,
  void** nv_baseaddr) 
{
  nvwal_error_t ret;
  int nv_fd;
  char pathname[256];

  nvwal_concat_sequence_filename(
    bufmgr->config_.nv_root_,
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
                  bufmgr->config_.mds_page_size_,
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

nvwal_error_t unmap_nvram_buffer_file(
  struct NvwalMdsBufferManagerContext* bufmgr,
  void* nv_baseaddr)
{
  nvwal_error_t ret;

  ret = munmap(nv_baseaddr, bufmgr->config_.mds_page_size_);
  if (ret != 0) {
    ret = errno;
    goto error_return;   
  }
  return 0;

error_return:
  errno = ret;
  return ret;
}

static nvwal_error_t mds_bufmgr_init_nvram_buffer(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id, 
  struct NvwalMdsBuffer** buffer)
{
  nvwal_error_t ret;
  void* baseaddr;

  /* Attempt to map a buffer file and if it doesn't exist create 
   * it and map it */
  ret = map_nvram_buffer_file(bufmgr, buffer_id, &baseaddr);
  if (ret == EACCES) {
    ret = create_nvram_buffer_file(bufmgr, buffer_id);
    if (!ret) {
      goto error_return;
    }
    ret = map_nvram_buffer_file(bufmgr, buffer_id, &baseaddr);
  } 
  if (!ret) {
    goto error_return;
  }

  /* create buffer descriptor */
  *buffer = malloc(sizeof(**buffer));
  if (*buffer) {
    ret = ENOMEM;
    ret = unmap_nvram_buffer_file(bufmgr, baseaddr);
    goto error_return;
  }
  
  (*buffer)->file_ = NULL;
  (*buffer)->page_no_ = -1;
  (*buffer)->baseaddr_ = baseaddr;

  return 0;

error_return:
  errno = ret;
  return ret;
}




static nvwal_error_t mds_bufmgr_init_nvram_buffers(
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  nvwal_error_t ret;
  int i;
  struct NvwalMdsBuffer* buffer;

  for (i=0; i<kNvwalMdsMaxBufferPages; i++) {
    ret = mds_bufmgr_init_nvram_buffer(bufmgr, i, &buffer);
    if (!ret) {
      goto error_return;
    }
    bufmgr->buffers_[i] = buffer;
  }
  return 0;

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_bufmgr_init(
  const struct NvwalConfig* config, 
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  memset(bufmgr, 0, sizeof(*bufmgr));
  memcpy(&bufmgr->config_, config, sizeof(*config));

  mds_bufmgr_init_nvram_buffers(bufmgr);

  return 0;
}


nvwal_error_t mds_bufmgr_uninit(
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  nvwal_error_t ret;
  int i;
  struct NvwalMdsBuffer* buffer;

  for (i=0; i<kNvwalMdsMaxBufferPages; i++) {
    buffer = bufmgr->buffers_[i];
    ret = unmap_nvram_buffer_file(bufmgr, buffer->baseaddr_);
    if (!ret) {
      goto error_return;
    }
    free(buffer);
    bufmgr->buffers_[i] = NULL;
  }
  return 0;

error_return:
  errno = ret;
  return ret;
}


void  mds_bufmgr_lookup_page()
{
  /* TODO: implementn me */

}


nvwal_error_t mds_bufmgr_alloc_buffer(
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  /* TODO: implementn me */
  return 0;
}


nvwal_error_t mds_bufmgr_read_page(
  struct NvwalMdsBufferManagerContext* bufmgr, 
  struct PageFile* file, 
  page_no_t page_no, 
  struct NvwalMdsBuffer** buffer) 
{
  /* TODO: implement me */
  return 0;
}


nvwal_error_t mds_bufmgr_alloc_page(
  struct NvwalMdsBufferManagerContext* bufmgr, 
  struct PageFile* file, 
  page_no_t page_no, 
  struct NvwalMdsBuffer** buffer) 
{
  /* TODO: implement me */
  /*
     allocate a page in the file by doing truncate
   */
  return 0;
}



/******************************************************************************
 * Meta-data store core methods
 *****************************************************************************/


nvwal_error_t mds_init(
  const struct NvwalConfig* config, 
  struct NvwalContext* wal) 
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);

  memset(mds, 0, sizeof(*mds));
  memcpy(&mds->config_, config, sizeof(*config));

  mds_io_init(mds);
  mds_bufmgr_init(config, bufmgr);
 
  return 0;
}


nvwal_error_t mds_uninit(struct NvwalContext* wal)
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);

  mds_bufmgr_uninit(bufmgr);
  mds_io_uninit(mds);

  /* TODO: close active page files */
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
 * @brief Returns the maximum number of epochs per page
 */
static inline int max_epochs_per_page(struct NvwalMdsContext* mds)
{ 
  return mds->config_.mds_page_size_ / sizeof(struct MdsEpochMetadata);
}


/**
 * @brief Returns the file number of the page file storing metadata for 
 * epoch \a epoch_id.
 * 
 * @details
 * To increase write parallelism to the disk, we maintain multiple page files
 * and stripe epoch pages evenly across page files.
 */
file_no_t epoch_id_to_file_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  uint64_t page_offset = epoch_id / max_epochs_per_page(mds);
  return page_offset % kNvwalMdsMaxActivePagefiles;
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
/*
  nvwal_error_t ret;
  file_no_t fno = epoch_id_to_file_no(mds, epoch_id);
  struct PageFile* pf = mds_io_lookup_file_desc(mds, fno);
  if (!pf) {
    ret = mds_io_open_file(mds, fno, &pf);
    if (!ret) {
      pf = NULL;
    }
  }
  return pf; 
*/
}


/**
 * @brief Return the page number of the page storing metadata for 
 * epoch \a epoch_id.
 *
 * @note Currently, we support a single page file that grows infinitely. 
 */
page_no_t epoch_id_to_page_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  return epoch_id / ((mds->config_.mds_page_size_) / sizeof(struct MdsEpochMetadata));  
}


void mds_read_epoch(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id, struct MdsEpochMetadata* epoch_metadata)
{
  /* TODO: implement me */
}


nvwal_error_t mds_write_epoch(
  struct NvwalMdsContext* mds, 
  struct MdsEpochMetadata* epoch_metadata)
{
  nvwal_error_t ret;
  struct NvwalMdsBuffer* buffer;

  nvwal_epoch_t epoch_id = epoch_metadata->epoch_id_;

  struct PageFile* file = epoch_id_to_file(mds, epoch_id);
  page_no_t page_no = epoch_id_to_page_no(mds, epoch_id);

  ret = mds_bufmgr_read_page(&mds->bufmgr_, file, page_no, &buffer);
  if (ret != 0) {
    ret = mds_bufmgr_alloc_page(&mds->bufmgr_, file, page_no, &buffer);
    if (ret != 0) {
      goto error_return;
    }
  }
  /*
    TODO: now we have a buffer, write epoch record to nvram buffer, sync the nvram buffer. 
  */
  

  return 0;

error_return:
  errno = ret;
  return ret;
}
