# these are third party code, we don't care warning.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -w")

set(BUILD_TESTING OFF) # Disable testcases in these third party libraries

# We use gflags for command line argument parsing.
# boost::program_options provides equivalent functionalities, but it's not header-only!
# Rather, we statically link to gflags. So far, we are boost-free.
set(BUILD_SHARED_LIBS OFF) # we use gflags as static library only
set(BUILD_STATIC_LIBS ON)
add_subdirectory(gflags-2.1.1)

# For testing, we use Google-testing. It's active, provides more functionalities,
# and has no linking issues.
set(BUILD_SHARED_LIBS ON) # To reduce linking time, we now use shared-library version of gtest,
add_subdirectory(gtest-1.7.0)

add_subdirectory(glog-0.3.4)

set(BUILD_STATIC_LIBS ON) # this creates tinyxml2static library
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -w") # same as glog
add_subdirectory(tinyxml2-2.1.0)

# We use a variant of libbacktrace to get rich stacktrace information.
# This was retrieved from https://github.com/apitrace/apitrace
# which adds CMake script to the original libbacktrace in gcc.
# All files under libbacktrace folder are under BSD license of the original
# libbacktrace (see README), except cmake-related files under the following
# license (BSD).
## Copyright 2007-2012 VMware, Inc.
## Copyright 2011 Intel Corporation
## Copyright 2011 LunarG, Inc.
## Copyright 2011 Zack Rusin
## Copyright 2011-2012 Jose Fonseca
##
## All Rights Reserved.
##
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in
## all copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
## THE SOFTWARE.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -w")
add_subdirectory(libbacktrace)
# This is the latest version in https://github.com/gcc-mirror/gcc/tree/master/libbacktrace
# of 2014/10/02.
# I had to apply at least this patch:
#   https://gcc.gnu.org/ml/gcc-patches/2013-10/msg01445.html
# Without this, I got sigsegv in read_uint32() just like this:
#   https://groups.google.com/forum/#!topic/golang-nuts/hSEBs6Blo8U
# In case of other issues happening later, I applied the latest from gcc.
