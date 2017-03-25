/* 
 * Copyright 2017 Hewlett Packard Enterprise Development LP
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met:
 * 
 *   1. Redistributions of source code must retain the above copyright 
 *      notice, this list of conditions and the following disclaimer.
 *
 *   2. Redistributions in binary form must reproduce the above copyright 
 *      notice, this list of conditions and the following disclaimer 
 *      in the documentation and/or other materials provided with the 
 *      distribution.
 *   
 *   3. Neither the name of the copyright holder nor the names of its 
 *      contributors may be used to endorse or promote products derived 
 *      from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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

#ifdef NDEBUG

#define LOG(debug_level, format, ...) ((void)0)

#else

/**
 * @brief Log debug message.
 * 
 * @param[in] debug_level Debug logging level
 * @param[in] format Log output according to format (similar to printf).
 */
#define LOG(debug_level, format, ...)                                            \
    nvwal_debug_printd(debug_level, __FILE__, __LINE__, format, ##__VA_ARGS__ )

#endif

#ifdef __cplusplus
}
#endif

/** @} */

#endif  /* NVWAL_DEBUG_H_ */
