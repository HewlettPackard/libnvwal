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

#include <libgen.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "nvwal_debug.h"

const char* NvwalDebugLevelString[] = {
  "INFO",
  "WARNING",
  "ERROR",
  "FATAL"
};

static struct NvwalDebugContext nvwal_debug_context;

int nvwal_debug_init(enum NvwalDebugLevel minloglevel)
{
  nvwal_debug_context.minloglevel = minloglevel;
  nvwal_debug_context.full_path = 0;
  nvwal_debug_context.stream = stderr;

  return 0;
}

static inline int avail_space(int len, int maxbuflen)
{
  int l = maxbuflen - len - 1; /* -1 to account for the null character */
  if (l<0) { l = 0; }
  return l;
}

static inline void nvwal_debug_printd_impl(
  enum NvwalDebugLevel dlevel, 
  char *file, 
  int line, 
  const char *strformat, 
  va_list arglist) 
{
  const int maxbuflen = 1024;
	char buf[maxbuflen];
	int len = 0;

  if (dlevel < INFO || dlevel > FATAL ) { return; }
  if (dlevel < nvwal_debug_context.minloglevel) { return; }

  const char* loglevel_string = NvwalDebugLevelString[dlevel-1];

	if (loglevel_string) {
		len += snprintf(buf, avail_space(len, maxbuflen), "%s ", loglevel_string);
    if (len > avail_space(len, maxbuflen)) { goto overflow_error; }
	}
	if (file) {
    if (!nvwal_debug_context.full_path) {
      file = basename(file);
    }
		len += snprintf(&buf[len], avail_space(len, maxbuflen), "(%s, %d)", file, line); 
    if (len > avail_space(len, maxbuflen)) { goto overflow_error; }
	}
	if (file || loglevel_string) {
		len += snprintf(&buf[len], avail_space(len, maxbuflen), ": "); 
    if (len > avail_space(len, maxbuflen)) { goto overflow_error; }
	}
	vsnprintf(&buf[len], avail_space(len, maxbuflen), strformat, arglist);
	fprintf(nvwal_debug_context.stream, "%s\n", buf);

	if (dlevel == FATAL) {
		exit(1);
	}

  return;

overflow_error:
  return;
}

void nvwal_debug_printd(
  enum NvwalDebugLevel dlevel, 
  char *file, 
  int line, 
  const char *strformat, ...) 
{
  va_list arglist;

  va_start(arglist, strformat);
  nvwal_debug_printd_impl(dlevel, file, line, strformat, arglist);
  va_end(arglist);
}
