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

#ifndef NVWAL_IMPL_MDS_H_
#define NVWAL_IMPL_MDS_H_
/**
 * @file nvwal_impl_mds.h
 * Function interface typedefs/enums/structs for internal use by the metadata 
 * store module implementation and tests.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */


/******************************************************************************
 * Declarations for private typedefs/enums/structs
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


/******************************************************************************
 * Interface for private functions
 *****************************************************************************/


/**
 * @brief Opens a page file and provides a page-file descriptor for this file.
 */
static nvwal_error_t mds_io_open_file(
  struct NvwalMdsContext* mds, 
  file_no_t file_no,
  struct PageFile** file);

/**
 * @brief Creates a page file and provides a page-file descriptor for this file.
 */
static nvwal_error_t mds_io_create_file(
  struct NvwalMdsContext* mds, 
  file_no_t file_no, 
  struct PageFile** file);

/**
 * @brief Closes a page file.
 * 
 * @details
 * Deallocates the memory associated with the page file descriptor.
 */
static void mds_io_close_file(
  struct NvwalMdsContext* mds,
  struct PageFile* file);



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



#endif /* NVWAL_IMPL_MDS_H_ */
