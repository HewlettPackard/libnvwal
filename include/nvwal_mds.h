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

#ifndef NVWAL_MDS_H_
#define NVWAL_MDS_H_
/**
 * @file mds.h
 * Function interface to a minimal metadata store for internal use by the 
 * core nvwal library. 
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include "nvwal_fwd.h"
#include "nvwal_types.h"
#include "nvwal_mds_types.h"


/**
 * @brief Initializes the metadata store.
 *
 * @param[in] config runtime configuration parameters
 * @param[out] wal nvwal context 
 *
 * @notes
 * This function just does some basic initialization. It does not restore 
 * any durable state. Instead, the user has to call a separate recover 
 * function to restore such state.
 *
 */
nvwal_error_t mds_init(
  const struct NvwalConfig* config, 
  struct NvwalContext* wal);


/**
 * @brief Uninitializes the metadata store.
 *
 * @param[in] wal nvwal context 
 */
nvwal_error_t mds_uninit(struct NvwalContext* wal);


/** @} */

#endif /* NVWAL_MDS_H */
