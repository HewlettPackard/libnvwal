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

/* 
 * The type is defined in nvwal_mds_types.h but we do the assert check 
 * here to ensure header files can be compiled with older C compilers 
 */
static_assert(sizeof(struct MdsEpochMetadata) == 64, 
              "Epoch metadata must match NV-DIMM failure-atomic unit size");

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

typedef uint64_t page_offset_t;
typedef uint64_t page_no_t;
typedef uint64_t file_no_t;

/**  
 * @brief Represents a page-file descriptor structure.
 */
struct PageFile {
  file_no_t file_no_;
  int       fd_;
};

/**
 * @brief Represents a page containing epoch metadata.
 */
struct Page {
  struct MdsEpochMetadata epochs_[0];
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
 * @brief Opens a page file and provides a page-file descriptor for this file.
 */
static nvwal_error_t mds_io_open_file(
  struct NvwalMdsContext* mds, 
  file_no_t file_no,
  struct PageFile** file);


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

/*
 * FIXME: Use direct i/o
 */
nvwal_error_t mds_io_open_file(
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
 *
 * FIXME: Use direct i/o
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
 * 
 * @details
 * Deallocates the memory associated with the page file descriptor.
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


static inline struct PageFile* mds_io_file(
  struct NvwalMdsContext* mds, 
  file_no_t file_no)
{
  if (file_no > kNvwalMdsMaxActivePagefiles - 1) {
    return NULL;
  }
  return mds->active_files_[file_no];
}


static nvwal_error_t mds_io_write_page(
  struct PageFile* file,
  page_no_t page_no,
  const void* buf)
{
  assert(0 && "TODO: implement me -- do we need this API?");
  return 0; 
}


static nvwal_error_t mds_io_append_page(
  struct PageFile* file,
  const void* buf)
{
  nvwal_error_t ret;

  ret = write(file->fd_, buf, 1);
  if (ret != 0) {
    ret = errno;
    goto error_return;
  }
  return 0; 

error_return:
  errno = ret;
  return ret;
}



/******************************************************************************
 * Meta-data store buffer-manager subsystem functions
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
  (*buffer)->page_no_ = 0;
  (*buffer)->baseaddr_ = baseaddr;

  return 0;

error_return:
  errno = ret;
  return ret;
}


/**
 * @details 
 * We maintain a single nvram buffer per page file.
 */
static nvwal_error_t mds_bufmgr_init_nvram_buffers(
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  nvwal_error_t ret;
  int i;
  struct NvwalMdsBuffer* buffer;

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    ret = mds_bufmgr_init_nvram_buffer(bufmgr, i, &buffer);
    if (!ret) {
      goto error_return;
    }
    bufmgr->write_buffers_[i] = buffer;
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

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    buffer = bufmgr->write_buffers_[i];
    ret = unmap_nvram_buffer_file(bufmgr, buffer->baseaddr_);
    if (!ret) {
      goto error_return;
    }
    free(buffer);
    bufmgr->write_buffers_[i] = NULL;
  }
  return 0;

error_return:
  errno = ret;
  return ret;
}


static inline struct Page* mds_bufmgr_page(struct NvwalMdsBuffer* buffer)
{
  return (struct Page*) buffer->baseaddr_;
}


nvwal_error_t mds_bufmgr_alloc_buffer(
  struct NvwalMdsBufferManagerContext* bufmgr,
  struct NvwalMdsBuffer** buffer)
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


/**
 * @brief Allocates and buffers a page.
 * 
 * @details
 * As the buffer is durable, we simply allocate a durable buffer and 
 * lazily allocate the page in the page file by allocating the page 
 * when we finally evict it.
 */
nvwal_error_t mds_bufmgr_alloc_page(
  struct NvwalMdsBufferManagerContext* bufmgr, 
  struct PageFile* file, 
  page_no_t page_no, 
  struct NvwalMdsBuffer** buffer) 
{
  nvwal_error_t ret;
  struct NvwalMdsBuffer* buf = bufmgr->write_buffers_[file->file_no_];
  
  if (page_no == buf->page_no_) {
    /* do nothing: page is already allocated and buffered */
    ret = 0;
  } else if (page_no == buf->page_no_+1) {
    ret = mds_io_append_page(file, buf->baseaddr_);
    buf->page_no_ = page_no; 
    ret = 0;
  } else {
    assert(0 && "this shouldn't happen");
  }
  return ret;
}



/******************************************************************************
 * Meta-data store core methods
 *****************************************************************************/

/**
 * @brief Normalize epoch id for index arithmetic operations.
 * 
 * @details
 * As epoch 0 is an invalid epoch (kNvwalInvalidEpoch == 0), so epochs
 * start at 1. We therefore subtract 1 one simplify arithmetic operations.
 */
static inline nvwal_epoch_t normalize_epoch_id(nvwal_epoch_t epoch_id)
{
  static_assert(kNvwalInvalidEpoch == 0, "Invalid epoch expected to be 0 but is not.");
  return epoch_id - 1;
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
static inline file_no_t epoch_id_to_file_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  uint64_t page_offset = normalize_epoch_id(epoch_id) / max_epochs_per_page(mds);
  return page_offset % kNvwalMdsMaxActivePagefiles;
}


/**
 * @brief Return the page number of the page storing metadata for 
 * epoch \a epoch_id.
 */
static inline page_no_t epoch_id_to_page_no(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  assert(epoch_id != kNvwalInvalidEpoch);
  page_no_t page_no = normalize_epoch_id(epoch_id) / (max_epochs_per_page(mds) * kNvwalMdsMaxActivePagefiles);
  return page_no + 1;
}

/**
 * @brief Return the record offset relative to the page 
 */
static inline  epoch_id_to_page_offset(struct NvwalMdsContext* mds, nvwal_epoch_t epoch_id)
{
  return normalize_epoch_id(epoch_id) % max_epochs_per_page(mds);
}

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

  return 0;
}


/** 
 * @details
 * Epochs increase monotonically and we always write epochs in sequential order. 
 * Thus, we can infer the page number of the page by looking at the first epoch 
 * record.
 */   
static page_no_t mds_page_no(
  struct NvwalMdsContext* mds, 
  struct Page* page)
{
  page_no_t page_no;
  nvwal_epoch_t epoch_id = page->epochs_[0].epoch_id_;

  if (epoch_id == kNvwalInvalidEpoch) {
    page_no = 0; /* invalid page number */
  } else {
    page_no = epoch_id_to_page_no(mds, epoch_id);
  }

  return page_no;
}


static nvwal_epoch_t mds_latest_epoch_in_page(
  struct NvwalMdsContext* mds,
  struct Page* page)
{
  int i;
  nvwal_epoch_t latest_epoch = kNvwalInvalidEpoch;

  for (i=0; i<max_epochs_per_page(mds); i++) {
    nvwal_epoch_t epoch = page->epochs_[i].epoch_id_;
    /* epochs increase monotonically */
    if (epoch <= latest_epoch) {
      break;
    }
    latest_epoch = epoch;
  }
  return latest_epoch;
}


nvwal_error_t mds_recover(struct NvwalContext* wal) 
{
  int i;
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);
  nvwal_epoch_t latest_epoch;

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    struct PageFile* file = mds->active_files_[i];
    struct NvwalMdsBuffer* buffer = bufmgr->write_buffers_[i];
    struct Page* page = mds_bufmgr_page(buffer);

    latest_epoch = mds_latest_epoch_in_page(mds, page);
    if (latest_epoch > mds->latest_epoch_) {
      mds->latest_epoch_ = latest_epoch;
    }
    page_no_t page_no = mds_page_no(mds, page); 
    buffer->page_no_ = page_no;
    buffer->file_ = file;
  }
  
  return 0;
}


void mds_read_epoch(
  struct NvwalContext* wal, 
  nvwal_epoch_t epoch_id, 
  struct MdsEpochMetadata* epoch_metadata)
{
  /*
   TODO: 
    - we do optimistic reads 
    - if the epoch is found in a durable nvram buffer then it's possible that while we 
      read an epoch the buffered page gets evicted and recycled. 
      We address this case by reading the page number of the buffer, read the 
      epoch, and then re-read the page number to ensure that the buffered page has not 
      been recycled.
      This means that after we evict a page, before we recycle the buffer, we assign
      the page number of the page based on the new page number we need to buffer. 
      This serves as the linearization point with respect to readers. 
      The linearization point with respect to crashes is ...?
      We only infer page numbers from epochs stored in a page during recovery.
   */
  assert(0 && "Implement me");
}


nvwal_error_t mds_write_epoch(
  struct NvwalContext* wal, 
  struct MdsEpochMetadata* epoch_metadata)
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  nvwal_error_t ret;
  struct NvwalMdsBuffer* buffer;

  nvwal_epoch_t epoch_id = epoch_metadata->epoch_id_;

  file_no_t file_no = epoch_id_to_file_no(mds, epoch_id);
  page_no_t page_no = epoch_id_to_page_no(mds, epoch_id);
  struct PageFile* file = mds_io_file(mds, file_no);

  /* We are always guaranteed to have space in the page as we write epochs 
   * sequentially. An epoch that requires a new page will request the next 
   * page_no.
   */  
  /* TODO: do a sanity check of the above invariant: that the page has enough space */
  ret = mds_bufmgr_alloc_page(&mds->bufmgr_, file, page_no, &buffer);

  struct Page* page = mds_bufmgr_page(buffer);
  page_offset_t epoch_off = epoch_id_to_page_offset(mds, epoch_id);

  /*
    TODO: replace memcpy with memcpy persist
  */
  memcpy(&page->epochs_[epoch_off], epoch_metadata, sizeof(*epoch_metadata));

  return 0;

error_return:
  errno = ret;
  return ret;
}
