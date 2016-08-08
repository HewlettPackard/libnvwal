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
#ifndef NVWAL_UTIL_H_
#define NVWAL_UTIL_H_
/**
 * @file nvwal_util.h
 * Internal and assorted macros/functions etc.
 * We should NOT have much here.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include <errno.h>
#include <stdio.h>

#include "nvwal_types.h"

#define CHECK_FD_VALID(fd)                             \
    do {                                                \
        if(fd < 0) {                                    \
            perror( "Invalid fd in "                    \
                    __FUNCTION__ ":" __LINE__ " ");     \
            exit(-1);                                   \
        }                                               \
    } while(0)

#define CHECK_NO_ERROR(err)                                            \
    do {                                                                \
        if(err) {                                                       \
            perror( "Error in " __FUNCTION__ ":" __LINE__ " :");        \
            exit(-1);                                                   \
        }                                                               \
    } while(0)

#define POINTER_AHEAD(ptr1, ptr2, start, size)                          \
    (((start) <= (ptr1) ? (ptr1)-(start) : (ptr1)+(size)-(start)) >     \
     ((start) <= (ptr2) ? (ptr2)-(start) : (ptr2)+(size)-(start)))

#define CIRCULAR_SIZE(start, end, size)                         \
    ((start) <= (end) ? (end)-(start) : (end)+(size)-(start))


inline nvwal_error_t nvwal_raise_einval(const char* message) {
  fprintf(stderr, message);
  errno = EINVAL;
  return EINVAL;
}

inline nvwal_error_t nvwal_raise_einval_llu(const char* message, uint64_t param) {
  fprintf(stderr, message, param);
  errno = EINVAL;
  return EINVAL;
}

/** @} */

#endif  // NVWAL_UTIL_H_
