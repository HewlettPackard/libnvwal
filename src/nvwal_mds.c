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
#include <sys/stat.h>
#include <sys/types.h>

#include <libpmem.h>

#include "nvwal_atomics.h"
#include "nvwal_types.h"
#include "nvwal_util.h"

/* Inspired by Black Swan of Tchaikovsky */

/* 
 * The type is defined in nvwal_mds_types.h but we do the assert check 
 * here to ensure header files can be compiled with older C compilers 
 */
/* TODO: Define a constant for failure-atomic size or cacheline size */
/* TODO: or make this a runtime check based on hardware architecture */
static_assert(sizeof(struct MdsEpochMetadata) == 64, 
              "Epoch metadata must match NV-DIMM failure-atomic unit size");

#define ASSERT_FD_VALID(fd) assert(fd != -1)

#define MDS_NVRAM_BUFFER_FILE_PREFIX  "mds-nvram-buf-"
#define MDS_PAGE_FILE_PREFIX          "mds-pagefile-"

#include "nvwal_impl_mds.h"


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
static struct PageFile* alloc_page_file_desc(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no, 
  int fd)
{
  struct PageFile* file;

  if (!(file = malloc(sizeof(*file)))) {
    return NULL;
  }

  file->io_ = io;
  file->file_no_ = file_no;
  file->fd_ = fd;
  
  return file;
}


static nvwal_error_t mds_io_file_exists(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no)
{
  char pathname[kNvwalMaxPathLength];
  struct stat stbuf;
  
  nvwal_concat_sequence_filename(
    io->wal_->config_.disk_root_,
    MDS_PAGE_FILE_PREFIX,
    file_no,
    pathname);
    
  nvwal_error_t ret = stat(pathname, &stbuf);

  return ret == 0;
}


nvwal_error_t mds_io_open_file(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no,
  struct PageFile** file)
{
  nvwal_error_t ret;
  int fd = -1;
  char pathname[kNvwalMaxPathLength];
  
  nvwal_concat_sequence_filename(
    io->wal_->config_.disk_root_,
    MDS_PAGE_FILE_PREFIX,
    file_no,
    pathname);
    
  fd = open(pathname, O_RDWR|O_APPEND);

  if (fd == -1) {
    /** Failed to open/create the file! */
    ret = errno;
    goto error_return;
  }

  *file = alloc_page_file_desc(io, file_no, fd);
  assert(*file);

  return 0;
 
error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_io_create_file(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no, 
  struct PageFile** file)
{
  nvwal_error_t ret;
  int fd = -1;
  char pathname[kNvwalMaxPathLength];

  nvwal_concat_sequence_filename(
    io->wal_->config_.disk_root_,
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
  ret = nvwal_open_and_fsync(io->wal_->config_.disk_root_);
  if (ret) {
    goto error_return;
  }
 
  *file = alloc_page_file_desc(io, file_no, fd);
  assert(*file);

  return ret;
 
error_return:
  errno = ret;
  return ret;
}


void mds_io_close_file(
  struct NvwalMdsIoContext* io,
  struct PageFile* file)
{
  close(file->fd_);
  free(file);  
}


inline struct PageFile* mds_io_file(
  struct NvwalMdsIoContext* io, 
  file_no_t file_no)
{
  if (file_no > kNvwalMdsMaxActivePagefiles - 1) {
    return NULL;
  }
  return io->active_files_[file_no];
}


nvwal_error_t mds_io_pread(
  struct PageFile* file, 
  void* buf, 
  size_t count, 
  off_t offset)
{
  nvwal_error_t ret;


  /* we might need multiple preads */
  size_t total_read = 0;
  while (total_read < count) {
    ret = pread(file->fd_, (char*) buf + total_read, count - total_read, offset+total_read);
    if (ret < 0) {
      ret = errno;
      goto error_return;
    }
    total_read += ret;
  }
  
  return 0; 

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_io_append_page(
  struct PageFile* file,
  const void* buf)
{
  nvwal_error_t ret;
  size_t page_size = file->io_->wal_->config_.mds_page_size_;

  /* we might need multiple writes */
  size_t total_written = 0;
  while (total_written < page_size) {
    ret = write(file->fd_, (const char*) buf + total_written, page_size - total_written);
    if (ret < 0) {
      ret = errno;
      goto error_return;
    }
    total_written += ret;
  }
  fsync(file->fd_);
  
  return 0; 

error_return:
  errno = ret;
  return ret;
}


/**
 * @brief Ensure atomicity of last append.
 */
static nvwal_error_t mds_io_recovery_complete_append_page(struct PageFile* file)
{
  struct stat buf;

  nvwal_error_t ret = fstat(file->fd_, &buf);
  if (ret != 0) {
    ret = errno;
    goto error_return;
  }

  /**
   * If not multiple of page size, then we had a torn append due to a crash so
   * we truncate the last torned page.
   */
  size_t page_size = file->io_->wal_->config_.mds_page_size_;
  if (buf.st_size % page_size) {
    size_t complete_pages = buf.st_size / page_size;
    ret = ftruncate(file->fd_, complete_pages * page_size);
    if (ret != 0) {
      ret = errno;
      goto error_return;
    }
  }

  return 0;

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_io_init(
  enum NvwalInitMode mode,
  struct NvwalContext* wal,
  int* did_restart)
{
  nvwal_error_t ret;
  struct PageFile* pf;

  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsIoContext* io = &(mds->io_);
 
  memset(io, 0, sizeof(*io));

  io->wal_ = wal;

  *did_restart = 0;

  /* Check if there are any existing files */  
  int num_existing = 0;
  for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    int exists = mds_io_file_exists(io, i);
    num_existing += exists;
  }

  /* Attempt to restart from existing files */
  if ((mode == kNvwalInitRestart && 
       num_existing == kNvwalMdsMaxActivePagefiles) || 
      (mode == kNvwalInitCreateIfNotExists && 
       num_existing == kNvwalMdsMaxActivePagefiles))
  {
    for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
      ret = mds_io_open_file(io, i, &pf);
      if (ret != 0) {
        goto error_return;
      }
      io->active_files_[i] = pf;
      ret = mds_io_recovery_complete_append_page(pf);
      if (ret != 0) {
        goto error_return;
      }
    }
    *did_restart = 1;
    return 0;  
  }

  /* No existing files, attempt to create them */  
  if (mode == kNvwalInitCreateIfNotExists && 
      num_existing == 0)
  {
    for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
      ret = mds_io_create_file(io, i, &pf);
      if (ret != 0) {
        goto error_return;
      }
      io->active_files_[i] = pf;
    }
    return 0;  
  }

  ret = EIO;
  
error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_io_uninit(struct NvwalContext* wal)
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsIoContext* io = &(mds->io_);

  for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    if (io->active_files_[i]) {
      mds_io_close_file(io, io->active_files_[i]);   
      io->active_files_[i] = NULL;
    }
  }
  return 0;
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
    bufmgr->wal_->config_.nv_root_,
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
  ret = posix_fallocate(nv_fd, 0, bufmgr->wal_->config_.mds_page_size_);
  if (ret) {
    goto error_return;
  }

  fsync(nv_fd);
  close(nv_fd);

  /* 
   * Sync the parent directory, so that the newly created (empty) file is visible.
   */
  ret = nvwal_open_and_fsync(bufmgr->wal_->config_.nv_root_);
  if (ret) {
    goto error_return;
  }

  return 0;

error_return:
  errno = ret;
  return ret;
}

static int nvram_buffer_file_exists(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id)
{
  char pathname[kNvwalMaxPathLength];
  struct stat stbuf;

  nvwal_concat_sequence_filename(
    bufmgr->wal_->config_.nv_root_,
    MDS_NVRAM_BUFFER_FILE_PREFIX,
    buffer_id,
    pathname);
    
  nvwal_error_t ret = stat(pathname, &stbuf);
  return (ret == 0);
}


static nvwal_error_t map_nvram_buffer_file(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id,
  void** nv_baseaddr) 
{
  nvwal_error_t ret;
  int nv_fd;
  char pathname[kNvwalMaxPathLength];

  nvwal_concat_sequence_filename(
    bufmgr->wal_->config_.nv_root_,
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
                  bufmgr->wal_->config_.mds_page_size_,
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

static nvwal_error_t unmap_nvram_buffer_file(
  struct NvwalMdsBufferManagerContext* bufmgr,
  void* nv_baseaddr)
{
  nvwal_error_t ret;

  ret = munmap(nv_baseaddr, bufmgr->wal_->config_.mds_page_size_);
  if (ret != 0) {
    ret = errno;
    goto error_return;   
  }
  return 0;

error_return:
  errno = ret;
  return ret;
}


static nvwal_error_t mds_bufmgr_map_nvram_buffer(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id) 
{
  nvwal_error_t ret;
  void* baseaddr;

  ret = map_nvram_buffer_file(bufmgr, buffer_id, &baseaddr);
  if (ret != 0) {
    goto error_return;
  }

  struct NvwalMdsBuffer* buffer = &bufmgr->write_buffers_[buffer_id];
  buffer->file_ = NULL;
  buffer->page_no_ = 0;
  buffer->baseaddr_ = baseaddr;

  return 0;

error_return:
  errno = ret;
  return ret;
}


static nvwal_error_t mds_bufmgr_create_nvram_buffer(
  struct NvwalMdsBufferManagerContext* bufmgr,
  int buffer_id)
{
  nvwal_error_t ret;
  void* baseaddr;

  ret = create_nvram_buffer_file(bufmgr, buffer_id);
  if (ret != 0) {
    goto error_return;
  }
  ret = map_nvram_buffer_file(bufmgr, buffer_id, &baseaddr);
  if (ret != 0) {
    goto error_return;
  }

  struct NvwalMdsBuffer* buffer = &bufmgr->write_buffers_[buffer_id];
  buffer->file_ = NULL;
  buffer->page_no_ = 0;
  buffer->baseaddr_ = baseaddr;

  return 0;

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_bufmgr_init(
  enum NvwalInitMode mode,
  struct NvwalContext* wal,
  int* did_restart)
{
  nvwal_error_t ret;

  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);
 
  memset(bufmgr, 0, sizeof(*bufmgr));

  bufmgr->wal_ = wal;

  *did_restart = 0;

  /* Check if there are any existing buffers */  
  int num_existing = 0;
  for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    int exists = nvram_buffer_file_exists(bufmgr, i);
    num_existing += exists;
  }

  /* Attempt to restart from existing buffers */
  if ((mode == kNvwalInitRestart && 
       num_existing == kNvwalMdsMaxActivePagefiles) ||
      (mode == kNvwalInitCreateIfNotExists && 
       num_existing == kNvwalMdsMaxActivePagefiles))
  {
    for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
      ret = mds_bufmgr_map_nvram_buffer(bufmgr, i);
      if (ret != 0) {
        goto error_return;
      }
    }
    *did_restart = 1;
    return 0;  
  }

  /* No existing buffers, attempt to create them */  
  if (mode == kNvwalInitCreateIfNotExists && 
      num_existing == 0)
  {
    for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
      ret = mds_bufmgr_create_nvram_buffer(bufmgr, i);
      if (ret != 0) {
        goto error_return;
      }
    }
    return 0;  
  }

  ret = EIO;
  
error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_bufmgr_uninit(
  struct NvwalContext* wal)
{
  nvwal_error_t ret;
  struct NvwalMdsBuffer* buffer;
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);
 
  for (int i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    buffer = &bufmgr->write_buffers_[i];
    ret = unmap_nvram_buffer_file(bufmgr, buffer->baseaddr_);
    if (ret != 0) {
      goto error_return;
    }
    buffer->baseaddr_ = NULL;
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


/**
 * @brief Allocates and buffers a page.
 * 
 * @details
 * As the buffer is durable, we simply allocate a durable buffer that 
 * will hold the page. We lazily allocate the page in the page-file by 
 * allocating and writing the page when we finally evict it from the 
 * buffer.
 *
 * Linearization point with respect to readers: 
 * It's possible that while a concurrent reader finds and tries to read an 
 * epoch from a page buffered in a durable nvram buffer, we evict and recycle 
 * the buffered page.
 * To help readers detect this case, after we evict a page and before we 
 * recycle the buffer, we assign the page number of the buffer page based to 
 * the new buffered page. 
 * Since page numbers increase monotonically, a reader can detect a page 
 * recycle by first reading the page number of the buffer before reading the 
 * buffered epoch, then read the epoch, and finally re-read the page number 
 * to ensure that the buffered page has not been recycled.
 * 
 * Linearization point with respect to crashes:
 * We only recycle a buffered page after evicting and syncing the page on the 
 * page file.
 */
nvwal_error_t mds_bufmgr_alloc_page(
  struct NvwalMdsBufferManagerContext* bufmgr, 
  struct PageFile* file, 
  page_no_t page_no, 
  struct NvwalMdsBuffer** buffer) 
{
  nvwal_error_t ret;
  struct NvwalMdsBuffer* buf = &bufmgr->write_buffers_[file->file_no_];
  
  if (page_no == buf->page_no_) {
    /* do nothing: page is already allocated and buffered */
    *buffer = buf;
    ret = 0;
  } else if (page_no == buf->page_no_+1) {
    ret = mds_io_append_page(file, buf->baseaddr_);
    nvwal_atomic_store(&buf->page_no_, page_no);
    *buffer = buf;
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



nvwal_epoch_t min_epoch_id(nvwal_epoch_t a, nvwal_epoch_t b) 
{ 
  return (a < b) ? a : b; 
}


/**
 * @brief Performs recovery of the metadata store. 
 *
 * @param[in] wal nvwal context 
 * 
 * @details 
 * Restores epoch metadata to the latest consistent durable state.
 */
static nvwal_error_t mds_recover(struct NvwalContext* wal) 
{
  int i;
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);
  nvwal_epoch_t latest_epoch;

  for (i=0; i<kNvwalMdsMaxActivePagefiles; i++) {
    struct PageFile* file = mds->io_.active_files_[i];
    struct NvwalMdsBuffer* buffer = &bufmgr->write_buffers_[i];
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

nvwal_error_t mds_init(
  enum NvwalInitMode mode,
  struct NvwalContext* wal) 
{
  nvwal_error_t ret;
  nvwal_error_t ret2;

  struct NvwalMdsContext* mds = &(wal->mds_);

  memset(mds, 0, sizeof(*mds));

  mds->wal_ = wal;

  int io_did_restart;
  ret = mds_io_init(mode, wal, &io_did_restart);
  if (ret != 0) {
    goto error_return;
  }

  int bufmgr_did_restart;
  ret = mds_bufmgr_init(mode, wal, &bufmgr_did_restart);
  if (ret != 0) {
    goto error_io_uninit;
  }

  if (io_did_restart && bufmgr_did_restart) {
    ret = mds_recover(wal);
    if (ret != 0) {
      goto error_io_uninit;
    }
  }

  return 0;

error_io_uninit:
  ret2 = mds_io_uninit(wal);
  assert(ret2 == 0);
error_return:
  return ret;
}


nvwal_error_t mds_uninit(struct NvwalContext* wal)
{
  nvwal_error_t ret;

  ret = mds_bufmgr_uninit(wal);
  if (ret != 0) {
    goto error_return;
  }
  ret = mds_io_uninit(wal);
  if (ret != 0) {
    goto error_return;
  }

  return 0;

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_epoch_iterator_prefetch(
  struct MdsEpochIterator* iterator)
{
  struct NvwalContext* wal = iterator->wal_;
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);

  nvwal_epoch_t cur_epoch_id = iterator->cur_epoch_id_;
  file_no_t file_no = epoch_id_to_file_no(mds, cur_epoch_id);
  page_no_t page_no = epoch_id_to_page_no(mds, cur_epoch_id);
  struct PageFile* file = mds_io_file(&mds->io_, file_no);

  struct NvwalMdsBuffer* nvbuf = &bufmgr->write_buffers_[file->file_no_];
  page_no_t nvbuf_page_no = nvwal_atomic_load(&nvbuf->page_no_);

  /* Try reading from nvram buffer. */
  if (nvbuf_page_no == page_no) {
    /* 
     * Optimistically read from the nvram buffer.
     * Please review comments under mds_bufmgr_alloc_page to understand 
     * the linearization point. 
     */
    struct Page* page = mds_bufmgr_page(nvbuf);
    page_offset_t epoch_off = epoch_id_to_page_offset(mds, cur_epoch_id);
    memcpy(&iterator->buffer_.epoch_metadata_[0], &page->epochs_[epoch_off], sizeof(*iterator->epoch_metadata_));
    nvbuf_page_no = nvwal_atomic_load(&nvbuf->page_no_);
    /* Verify page didn't get evicted concurrently while reading */
    if (nvbuf_page_no == page_no) {
      iterator->buffer_.num_entries_ = 1;
      iterator->epoch_metadata_ = &iterator->buffer_.epoch_metadata_[0];
      return 0;
    }
  }

  /* 
   * Otherwise, try reading from the prefetch buffer or prefetch from 
   * the page file.
   */
  if (iterator->buffer_.num_entries_ > 0) {
    nvwal_epoch_t first_epoch_id = iterator->buffer_.epoch_metadata_[0].epoch_id_;
    nvwal_epoch_t last_epoch_id = iterator->buffer_.epoch_metadata_[iterator->buffer_.num_entries_-1].epoch_id_;
    if (cur_epoch_id >= first_epoch_id && cur_epoch_id <= last_epoch_id)
    {
      int idx = cur_epoch_id - first_epoch_id;
      iterator->epoch_metadata_ = &iterator->buffer_.epoch_metadata_[idx];
      return 0;
    }
  } 

  /* 
   * Prefetch from page file. 
   */

  /* We never prefetch past a page boundary to simplify implementation. */
  nvwal_epoch_t max_prefetchable_epoch_id = (page_no + 1) * max_epochs_per_page(mds);

  nvwal_epoch_t lower_epoch_id = cur_epoch_id;
  nvwal_epoch_t upper_epoch_id = 
    min_epoch_id(min_epoch_id(cur_epoch_id + kNvwalMdsReadPrefetch - 1, iterator->end_epoch_id_), 
                 max_prefetchable_epoch_id);
  
  int num_entries = upper_epoch_id - lower_epoch_id + 1;

  nvwal_error_t ret = 
    mds_io_pread(file, &iterator->buffer_.epoch_metadata_, 
      num_entries * sizeof(struct MdsEpochMetadata), epoch_id_to_file_offset(mds, lower_epoch_id));
  assert(ret == 0);
  iterator->buffer_.num_entries_ = num_entries;
  iterator->epoch_metadata_ = &iterator->buffer_.epoch_metadata_[0];

  return 0;
}


nvwal_error_t mds_epoch_iterator_init(
  struct NvwalContext* wal, 
  nvwal_epoch_t begin_epoch_id, 
  nvwal_epoch_t end_epoch_id,
  struct MdsEpochIterator* iterator)
{
  nvwal_error_t ret;

  if (end_epoch_id < begin_epoch_id) {
    ret = EINVAL;
    goto error_return;
  }

  iterator->wal_ = wal;
  iterator->begin_epoch_id_ = begin_epoch_id;
  iterator->end_epoch_id_ = end_epoch_id;
  iterator->cur_epoch_id_ = begin_epoch_id;
  memset(&iterator->buffer_, 0, sizeof(iterator->buffer_));

  mds_epoch_iterator_prefetch(iterator);

  return 0;  

error_return:
  errno = ret;
  return ret;
}


void mds_epoch_iterator_next(struct MdsEpochIterator* iterator)
{
  iterator->cur_epoch_id_++;
  if (iterator->cur_epoch_id_ <= iterator->end_epoch_id_) {
    mds_epoch_iterator_prefetch(iterator);
  }
}


int mds_epoch_iterator_done(struct MdsEpochIterator* iterator)
{
  return (iterator->cur_epoch_id_ > iterator->end_epoch_id_);
}


nvwal_error_t mds_epoch_iterator_destroy(struct MdsEpochIterator* iterator)
{
  memset(iterator, 0, sizeof(*iterator));
  return 0;
}


nvwal_epoch_t mds_latest_epoch(struct NvwalContext* wal)
{
  struct NvwalMdsContext* mds = &(wal->mds_);

  return nvwal_atomic_load(&mds->latest_epoch_);
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
  struct PageFile* file = mds_io_file(&mds->io_, file_no);

  /* 
   * We are always guaranteed to have space in the page as we write epochs 
   * sequentially. An epoch that requires a new page will request the next 
   * page_no.
   */  
  ret = mds_bufmgr_alloc_page(&mds->bufmgr_, file, page_no, &buffer);

  struct Page* page = mds_bufmgr_page(buffer);
  page_offset_t epoch_off = epoch_id_to_page_offset(mds, epoch_id);

  /*
    TODO: replace memcpy with memcpy persist
  */
  memcpy(&page->epochs_[epoch_off], epoch_metadata, sizeof(*epoch_metadata));

  nvwal_atomic_fetch_add(&mds->latest_epoch_, 1);

  return 0;

error_return:
  errno = ret;
  return ret;
}
