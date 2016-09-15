# Example and Demonstration project for libnvwal.
# This folder is optionally and separately compiled,
# thus we don't hesitate using C++. We even allow C++11.
cmake_minimum_required(VERSION 2.6)
cmake_policy(SET CMP0003 NEW)
project(nvwal_example)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Only under test folder, we depend on boost.
# When you don't want to have these dependencies, skip the whole test folder (-DNO_NVWAL_TEST)
find_package(Boost 1.36.0 REQUIRED COMPONENTS filesystem system)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(SYSTEM
  ${Boost_INCLUDE_DIRS}
  ${NVWAL_BIN_ROOT}/third_party/gflags-2.1.1/include
  )

set(EXAMPLE_DEPENDENCIES
  nvwal-shared
  ${CMAKE_THREAD_LIBS_INIT}
  ${Boost_LIBRARIES}
  gflags-static)

add_executable(get_started ${CMAKE_CURRENT_SOURCE_DIR}/get_started.cpp)
target_link_libraries(get_started ${EXAMPLE_DEPENDENCIES})
