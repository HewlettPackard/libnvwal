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
 */

#include <errno.h>

/* 32MB sounds like a good place to start? */
#define NV_SEG_SIZE (1 << 25)

/* Number of empty log files to create at a time (minimize directory fsync) */
#define PREALLOC_FILE_COUNT 100

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

#endif  // NVWAL_UTIL_H_
