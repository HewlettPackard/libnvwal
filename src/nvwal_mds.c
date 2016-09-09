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

#define FS_SUPPORTS_ATOMIC_APPEND 0

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

/**
 * @brief Initializes and marks file descriptor as active.
 */
static void mds_io_activate_file(
  struct NvwalMdsIoContext* io,
  file_no_t file_no,
  int fd)
{
  struct NvwalMdsPageFile* file = mds_io_file(io, file_no);
  file->active_ = 1;
  file->io_ = io;
  file->file_no_ = file_no;
  file->fd_ = fd;
}

nvwal_error_t mds_io_open_file(
  struct NvwalMdsIoContext* io,
  file_no_t file_no)
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

  mds_io_activate_file(io, file_no, fd);

  return 0;

error_return:
  errno = ret;
  return ret;
}


nvwal_error_t mds_io_create_file(
  struct NvwalMdsIoContext* io,
  file_no_t file_no)
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

  mds_io_activate_file(io, file_no, fd);

  return ret;

error_return:
  errno = ret;
  return ret;
}


void mds_io_close_file(
  struct NvwalMdsIoContext* io,
  file_no_t file_no)
{
  struct NvwalMdsPageFile* file = mds_io_file(io, file_no);
  close(file->fd_);
  io->files_[file_no].active_ = 0;
}


inline struct NvwalMdsPageFile* mds_io_file(
  struct NvwalMdsIoContext* io,
  file_no_t file_no)
{
  if (file_no > kNvwalMdsMaxPagefiles - 1) {
    return NULL;
  }
  return &io->files_[file_no];
}


nvwal_error_t mds_io_pread(
  struct NvwalMdsPageFile* file,
  void* buf,
  size_t count,
  off_t offset)
{
  nvwal_error_t ret;


  /* we might need multiple preads */
  size_t total_read = 0;
  while (total_read < count) {
    ret = pread(file->fd_, (char*) buf + total_read, count - total_read, offset+total_read);
    //printf("mds_io_pread: %p %d %p %d\n", buf, total_read, (char*) buf + total_read, ret);
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
  struct NvwalMdsPageFile* file,
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
 * @brief Truncate and sync the file to a page multiple.
 */
static nvwal_error_t mds_io_truncate_file(
  struct NvwalMdsPageFile* file,
  off_t page_length)
{
  size_t page_size = file->io_->wal_->config_.mds_page_size_;
  off_t length = page_length * page_size;
  nvwal_error_t ret = ftruncate(file->fd_, length);
  if (ret != 0) {
    ret = errno;
    goto error_return;
  }

  ret = fsync(file->fd_);
  if (ret != 0) {
    ret = errno;
    goto error_return;
  }

  return 0;

error_return:
  errno = ret;
  return ret;

}

#if FS_SUPPORTS_ATOMIC_APPEND
/**
 * @brief Ensure atomicity of last append.
 */
static nvwal_error_t mds_io_recovery_complete_append_page(
  struct NvwalMdsIoContext* io,
  file_no_t file_no)
{
  struct stat buf;
  struct NvwalMdsPageFile* file = mds_io_file(io, file_no);

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
    ret = mds_io_truncate_file(file, complete_pages);
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
#endif

nvwal_error_t mds_io_num_pages(
  struct NvwalMdsPageFile* file,
  page_no_t* num_pages)
{
  struct stat buf;
  size_t page_size = file->io_->wal_->config_.mds_page_size_;

  nvwal_error_t ret = fstat(file->fd_, &buf);
  if (ret != 0) {
    ret = errno;
    goto error_return;
  }

  if (buf.st_size % page_size != 0) {
    return nvwal_raise_einval(
      "Error: file size is not a page multiple\n");
  }

  *num_pages = buf.st_size / page_size;
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

  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsIoContext* io = &(mds->io_);

  memset(io, 0, sizeof(*io));

  io->wal_ = wal;

  *did_restart = 0;

  /* Check if there are any existing files */
  int num_existing = 0;
  for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
    int exists = mds_io_file_exists(io, i);
    num_existing += exists;
  }

  /* Attempt to restart from existing files */
  if ((mode == kNvwalInitRestart &&
       num_existing == kNvwalMdsMaxPagefiles) ||
      (mode == kNvwalInitCreateIfNotExists &&
       num_existing == kNvwalMdsMaxPagefiles))
  {
    for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
      ret = mds_io_open_file(io, i);
      if (ret != 0) {
        goto error_return;
      }
#if FS_SUPPORTS_ATOMIC_APPEND
      ret = mds_io_recovery_complete_append_page(io, i);
      if (ret != 0) {
        goto error_return;
      }
#endif
    }
    *did_restart = 1;
    return 0;
  }

  /* No existing files, attempt to create them */
  if (mode == kNvwalInitCreateIfNotExists &&
      num_existing == 0)
  {
    for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
      ret = mds_io_create_file(io, i);
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


nvwal_error_t mds_io_uninit(struct NvwalContext* wal)
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsIoContext* io = &(mds->io_);

  for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
    if (io->files_[i].active_) {
      mds_io_close_file(io, i);
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
  for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
    int exists = nvram_buffer_file_exists(bufmgr, i);
    num_existing += exists;
  }

  /* Attempt to restart from existing buffers */
  if ((mode == kNvwalInitRestart &&
       num_existing == kNvwalMdsMaxPagefiles) ||
      (mode == kNvwalInitCreateIfNotExists &&
       num_existing == kNvwalMdsMaxPagefiles))
  {
    for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
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
    for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
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

  for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
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
 * @brief Buffers a page in a durable nvram buffer.
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
 *
 */
nvwal_error_t mds_bufmgr_alloc_page(
  struct NvwalMdsBufferManagerContext* bufmgr,
  struct NvwalMdsPageFile* file,
  page_no_t page_no,
  struct NvwalMdsBuffer** bufferp)
{
  nvwal_error_t ret;
  struct NvwalMdsBuffer* buffer = &bufmgr->write_buffers_[file->file_no_];

  if (page_no == kNvwalInvalidPage) {
    ret = EINVAL;
    goto error_return;
  }

  if (buffer->page_no_ == 0) {
    /* buffer is free: just use it */
    buffer->file_ = file;
    buffer->page_no_ = page_no;
  }

  if (page_no == buffer->page_no_) {
    /* do nothing: page is already allocated and buffered */
    buffer->dirty_ = 1;
    *bufferp = buffer;
  } else if (page_no == buffer->page_no_+1) {
    /* we can recycle buffer only if clean */
    if (buffer->dirty_ == 0) {
      nvwal_atomic_store(&buffer->page_no_, page_no);
      buffer->dirty_ = 1;
      *bufferp = buffer;
    } else {
      ret = ENOBUFS;
      goto error_return;
    }
  } else {
    assert(0 && "this shouldn't happen");
  }
  
  return 0;

error_return:
  return ret;
}


/**
 * @brief Reads page from a page file into a buffer.
 *
 * @details
 * This a destructive and non-atomic operation that discards the existing
 * contents of a buffer.
 *
 * This operation cannot be used concurrently with optimistic readers.
 */
nvwal_error_t mds_bufmgr_read_page(
  struct NvwalMdsBufferManagerContext* bufmgr,
  struct NvwalMdsPageFile* file,
  page_no_t page_no,
  struct NvwalMdsBuffer** bufferp)
{
  struct NvwalContext* wal = bufmgr->wal_;
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBuffer* buffer = &bufmgr->write_buffers_[file->file_no_];

  NVWAL_CHECK_ERROR(mds_io_pread(file, buffer->baseaddr_, max_epochs_per_page(mds) * sizeof(struct MdsEpochMetadata), page_no_to_file_offset(mds, page_no)));
  //TODO: call persist on buffer range

  nvwal_atomic_store(&buffer->page_no_, page_no);
  buffer->file_ = file;
  buffer->dirty_ = 1;
  *bufferp = buffer;
  return 0;
}


/**
 * @brief Write backs dirty and completely filled up bufferred pages.
 */
nvwal_error_t mds_bufmgr_writeback(
  struct NvwalMdsBufferManagerContext* bufmgr)
{
  for (int i=0; i<kNvwalMdsMaxPagefiles; i++) {
    struct NvwalMdsBuffer* buffer = &bufmgr->write_buffers_[i];
    if (buffer->dirty_) {
      NVWAL_CHECK_ERROR(mds_io_append_page(buffer->file_, buffer->baseaddr_));
      buffer->dirty_ = 0;
    }
  }
  return 0;
}


/******************************************************************************
 * Meta-data store core methods
 *****************************************************************************/


/*
 * @brief Returns latest durable epoch.
 *
 * @details
 * This is taken from the nvwal control block.
 */
static nvwal_epoch_t mds_latest_durable_epoch(struct NvwalMdsContext* mds)
{
  struct NvwalContext* wal = mds->wal_;
  return wal->nv_control_block_->flusher_progress_.durable_epoch_;
}


/*
 * @brief Sets latest durable epoch.
 */
static void mds_set_latest_durable_epoch(struct NvwalMdsContext* mds, nvwal_epoch_t val)
{
  struct NvwalContext* wal = mds->wal_;
  wal->nv_control_block_->flusher_progress_.durable_epoch_ = val;
}


/*
 * @brief Returns latest paged epoch to disk.
 *
 * @details
 * This is taken from the nvwal control block.
 */
static nvwal_epoch_t mds_latest_paged_epoch(struct NvwalMdsContext* mds)
{
  struct NvwalContext* wal = mds->wal_;
  return wal->nv_control_block_->flusher_progress_.paged_mds_epoch_;
}


/*
 * @brief Sets latest paged epoch.
 */
static void mds_set_latest_paged_epoch(struct NvwalMdsContext* mds, nvwal_epoch_t val)
{
  struct NvwalContext* wal = mds->wal_;
  wal->nv_control_block_->flusher_progress_.paged_mds_epoch_ = val;
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

  for (i=0; i<kNvwalMdsMaxPagefiles; i++) {
    struct NvwalMdsPageFile* file = mds_io_file(&mds->io_, i);
    struct NvwalMdsBuffer* buffer;

    nvwal_epoch_t latest_epoch = mds_latest_durable_epoch(mds);
    nvwal_epoch_t latest_paged_epoch = mds_latest_paged_epoch(mds);

    if (latest_epoch < latest_paged_epoch) {
      /* Complete outstanding rollback/truncation */
      mds_rollback_to_epoch(wal, latest_epoch);
    } else {
      /* Initialize buffer to latest page */
      page_no_t latest_epoch_page = epoch_id_to_page_no(mds, latest_epoch);
      if (latest_epoch_page != kNvwalInvalidPage) {
        mds_bufmgr_alloc_page(bufmgr, file, latest_epoch_page, &buffer);
      }
    }

    if (latest_epoch > mds->latest_epoch_) {
      mds->latest_epoch_ = latest_epoch;
    }
  }

  return 0;
}

/**
 * Simple standalone pre-screening checks/adjustments on the given config.
 * This is the first step in mds_init().
 */
static nvwal_error_t sanity_check_config(
  struct NvwalConfig* config,
  enum NvwalInitMode mode)
{
  if (config->mds_page_size_ % 512 != 0) {
    return nvwal_raise_einval(
      "Error: mds_page_size_ must be a multiple of 512\n");
  }
  if (config->mds_page_size_ == 0) {
    config->mds_page_size_ = kNvwalMdsPageSize;
  }

  return 0;
}


nvwal_error_t mds_init(
  enum NvwalInitMode mode,
  struct NvwalContext* wal)
{
  nvwal_error_t ret;
  nvwal_error_t ret2;

  struct NvwalConfig* config = &(wal->config_);
  NVWAL_CHECK_ERROR(sanity_check_config(config, mode));

  struct NvwalMdsContext* mds = &(wal->mds_);
  memset(mds, 0, sizeof(*mds));

  mds->wal_ = wal;

  int io_did_restart;
  NVWAL_CHECK_ERROR(mds_io_init(mode, wal, &io_did_restart));

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
  struct NvwalMdsPageFile* file = mds_io_file(&mds->io_, file_no);

  struct NvwalMdsBuffer* nvbuf = &bufmgr->write_buffers_[file->file_no_];
  page_no_t nvbuf_page_no = nvwal_atomic_load(&nvbuf->page_no_);

  /* Try reading from nvram buffer. */
  if (nvbuf_page_no == page_no && nvbuf_page_no != kNvwalInvalidPage)
  {
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
  nvwal_epoch_t max_prefetchable_epoch_id = page_no * max_epochs_per_page(mds);

  nvwal_epoch_t lower_epoch_id = cur_epoch_id;
  nvwal_epoch_t upper_epoch_id =
    NVWAL_MIN(NVWAL_MIN(cur_epoch_id + kNvwalMdsReadPrefetch - 1, iterator->end_epoch_id_),
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
  struct NvwalMdsBuffer* buffer;
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);
  nvwal_epoch_t epoch_id = epoch_metadata->epoch_id_;
  file_no_t file_no = epoch_id_to_file_no(mds, epoch_id);
  page_no_t page_no = epoch_id_to_page_no(mds, epoch_id);
  struct NvwalMdsPageFile* file = mds_io_file(&mds->io_, file_no);

  NVWAL_CHECK_ERROR(mds_bufmgr_alloc_page(bufmgr, file, page_no, &buffer));

  /*
   * If we reach here, then it's guaranteed that the buffered page has enough
   * space to hold the epoch.
   */
  struct Page* page = mds_bufmgr_page(buffer);
  page_offset_t epoch_off = epoch_id_to_page_offset(mds, epoch_id);

  //TODO: persist via libpmem
  memcpy(&page->epochs_[epoch_off], epoch_metadata, sizeof(*epoch_metadata));

  nvwal_atomic_fetch_add(&mds->latest_epoch_, 1);

  return 0;
}


nvwal_error_t mds_writeback(struct NvwalContext* wal)
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);

  return mds_bufmgr_writeback(bufmgr);
}

/**
 * @details
 * This operation cannot be used concurrently with optimistic readers
 * as it calls mds_bufmgr_read_page which cannot be used concurrently
 * with optimistic readers.
 */
nvwal_error_t mds_rollback_to_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t epoch)
{
  struct NvwalMdsContext* mds = &(wal->mds_);
  struct NvwalMdsBufferManagerContext* bufmgr = &(mds->bufmgr_);
  struct NvwalMdsBuffer* buffer;

  mds_set_latest_durable_epoch(mds, epoch);

  if (epoch < mds_latest_paged_epoch(mds)) {
    file_no_t file_no = epoch_id_to_file_no(mds, epoch);
    page_no_t page_no = epoch_id_to_page_no(mds, epoch);
    struct NvwalMdsPageFile* file = mds_io_file(&mds->io_, file_no);

    NVWAL_CHECK_ERROR(mds_bufmgr_read_page(bufmgr, file, page_no, &buffer));
    page_no_t new_latest_paged_page = page_no - 1;
    NVWAL_CHECK_ERROR(mds_io_truncate_file(file, new_latest_paged_page));
    mds_set_latest_paged_epoch(mds, max_epochs_per_page(mds)*new_latest_paged_page);
  }

  if (epoch < mds->latest_epoch_) {
    mds->latest_epoch_ = epoch;
  }

  return 0;
}

nvwal_error_t mds_read_one_epoch(
  struct NvwalContext* wal,
  nvwal_epoch_t epoch_id,
  struct MdsEpochMetadata* out) {
  assert(epoch_id != kNvwalInvalidEpoch);
  struct MdsEpochIterator mds_iterator;
  NVWAL_CHECK_ERROR(mds_epoch_iterator_init(
    wal,
    epoch_id,
    epoch_id,
    &mds_iterator));
  assert(!mds_epoch_iterator_done(&mds_iterator));
  assert(mds_iterator.epoch_metadata_->epoch_id_  == epoch_id);
  *out = *mds_iterator.epoch_metadata_;
  NVWAL_CHECK_ERROR(mds_epoch_iterator_destroy(&mds_iterator));
  return 0;
}
