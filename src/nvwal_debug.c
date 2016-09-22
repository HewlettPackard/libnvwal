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
#ifndef NDEBUG
  va_list arglist;

  va_start(arglist, strformat);
  nvwal_debug_printd_impl(dlevel, file, line, strformat, arglist);
  va_end(arglist);
#endif
}
