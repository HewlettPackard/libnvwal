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
#ifndef NVWAL_DEBUG_H_
#define NVWAL_DEBUG_H_
/**
 * @file nvwal_debug.h
 * Internal debug logging functions/macros.
 * @ingroup LIBNVWAL
 * @addtogroup LIBNVWAL
 * @{
 */

#include <stdio.h>

#include "nvwal_types.h"

#ifdef __cplusplus
/* All API functions must be extern-C to be used from C and C++ */
extern "C" {
#endif  /* __cplusplus */


struct NvwalDebugContext {
  /**
   * Copy log messages at or above this level.
   */
  enum NvwalDebugLevel minloglevel;

  /**
   * Whether to use full pathnames for the current input file passed 
   * to the debug logging macro.
   */
  int full_path;

  /**
   * Stream to log messages to.
   */
  FILE* stream;
};

/**
 * @brief Initialize debug logging subsystem.
 * 
 * @param[in] minloglevel Copy log messages at or above this level
 */
int nvwal_debug_init(enum NvwalDebugLevel minloglevel);

/**
 * @brief Log debug message.
 *
 * @details
 * Not to be used directly by the user. Use LOG macro instead.
 */
void nvwal_debug_printd(enum NvwalDebugLevel dlevel, char *file, int line, const char *strformat, ...);


/**
 * @brief Log debug message.
 * 
 * @param[in] debug_level Debug logging level
 * @param[in] format Log output according to format (similar to printf).
 */
#define LOG(debug_level, format, ...)                                            \
    nvwal_debug_printd(debug_level, __FILE__, __LINE__, format, ##__VA_ARGS__ )


/** @} */

#endif  /* NVWAL_DEBUG_H_ */
