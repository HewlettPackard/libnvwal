cmake_minimum_required(VERSION 2.6)
project(nvwal)

# FIRST, prohibit in-source build.
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/WarnInSrcBuild.cmake)

set(CMAKE_COLOR_MAKEFILE ON)
set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(NVWAL_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(NVWAL_BIN_ROOT ${CMAKE_CURRENT_BINARY_DIR})
set(NVWAL_TEST_REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/test-reports)
set(NVWAL_VALGRIND_REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/valgrind-reports)
make_directory(${NVWAL_TEST_REPORT_DIR})
make_directory(${NVWAL_VALGRIND_REPORT_DIR})
mark_as_advanced(NVWAL_SRC_ROOT NVWAL_BIN_ROOT
  NVWAL_TEST_REPORT_DIR NVWAL_VALGRIND_REPORT_DIR)

#############################################################################
#
#               Compiler Options  : BEGIN
#
#############################################################################
# To avoid misuse, the user must specify -DCMAKE_BUILD_TYPE=Debug/Release/etc.
# We observed so many people compiling without optimizations and then
# being surprised by performance or lack of debug symbols.
# If everyone is familiar with CMake, we wouldn't have to worry about this..
string(TOUPPER "${CMAKE_BUILD_TYPE}" UPPER_BUILD_TYPE)
if(UPPER_BUILD_TYPE STREQUAL "DEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_CMAKE_DEBUG_BUILD")
  MESSAGE(STATUS "[Build Type] Debug build specified.")
elseif(UPPER_BUILD_TYPE STREQUAL "RELEASE")
  MESSAGE(STATUS "[Build Type] Release build specified.")
elseif(UPPER_BUILD_TYPE STREQUAL "RELWITHDEBINFO")
  MESSAGE(STATUS "[Build Type] RelWithDebInfo build specified.")
elseif(UPPER_BUILD_TYPE STREQUAL "MINSIZEREL")
  MESSAGE(STATUS "[Build Type] MinSizeRel build specified.")
else()
  MESSAGE(FATAL_ERROR
    " [Build Type] No build type was specified. Please invoke cmake with a valid build type."
    " e.g., mkdir release; cd release; cmake ../ -DCMAKE_BUILD_TYPE=Release"
    " Debug/Release/RelWithDebInfo/MinSizeRel are supported.")
endif()

# We currently do not link to libnuma.
# The client program can optionally put NUMA affinity before calling libnvwal
# find_package(Numa REQUIRED)

# We also need pthread. Note that linking against libstdc with C++11 flag is not enough.
find_package(Threads REQUIRED)

# libpmem as a mandatory dependency
find_package(Pmem REQUIRED)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11 -pthread -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -fno-strict-aliasing -fno-pie -Wall")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fno-pie")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include ${PMEM_INCLUDE_DIR})

#############################################################################
#
#               Build Target Libraries  : BEGIN
#
#############################################################################
set(CMAKE_INCLUDE_CURRENT_DIR OFF)

enable_testing()
include(CTest)


set(WAL_SOURCE_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/nvwal_api.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/nvwal_impl_cursor.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/nvwal_impl_init.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/nvwal_mds.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/nvwal_util.c
  )

add_library(nvwal-static STATIC ${WAL_SOURCE_FILES})
add_library(nvwal-shared SHARED ${WAL_SOURCE_FILES})
target_link_libraries(nvwal-static ${CMAKE_THREAD_LIBS_INIT} ${PMEM_LIBRARY})
target_link_libraries(nvwal-shared ${CMAKE_THREAD_LIBS_INIT} ${PMEM_LIBRARY})

# Unit tests. If you give -DNO_NVWAL_TEST, we skip the following.
# This is useful for a production environment to quickly build libnvwal
# without additional dependencies for test.
# Other than those cases, we recommend building/running testcases always.
if(NO_NVWAL_TEST)
  message(STATUS "NO_NVWAL_TEST was specified all testcases and their dependencies skipped")
else(NO_NVWAL_TEST)
  add_subdirectory(test)
  add_subdirectory(third_party) # all of them are just for test
endif(NO_NVWAL_TEST)

#############################################################################
#
#               Doxygen: BEGIN
#
#############################################################################
# Doxygen commands (not mandatory)
find_package(Doxygen)
if(DOXYGEN_FOUND)
  set(DOXYGEN_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/include")
  set(DOXYGEN_LAYOUT "${CMAKE_CURRENT_SOURCE_DIR}/DoxygenLayout.xml")
  set(DOXYGEN_TARGETS "${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/src")

  set(DOXYGEN_MAINPAGE_MARKDOWN "README.md")

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  add_custom_target(dox
    ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen" VERBATIM)
  add_custom_target(browse-dox
    COMMAND google-chrome ${CMAKE_CURRENT_BINARY_DIR}/dox/html/index.html
    DEPENDS dox)
else(DOXYGEN_FOUND)
  add_custom_target(dox ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
  add_custom_target(browse-dox ${CMAKE_COMMAND} -E echo "Doxygen not found. No documents generated.")
endif(DOXYGEN_FOUND)
