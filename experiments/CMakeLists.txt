# Unit test project for libnvwal.
# Under this folder, we don't hesitate using C++. We even allow C++11.
# We can also freely use gtest, glog, etc etc.
# Long live C++!
cmake_minimum_required(VERSION 2.6)
cmake_policy(SET CMP0003 NEW)
project(nvwal_experiments)


#############################################################################
#
#               Setup.
# You need to be familiar with cmake/gtest/valgrind to understand this part.
#
#############################################################################

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Only under test folder, we depend on boost.
# When you don't want to have these dependencies, skip the whole test folder (-DNO_NVWAL_TEST)
find_package(Boost 1.36.0 REQUIRED COMPONENTS filesystem system)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(SYSTEM
  ${Boost_INCLUDE_DIRS}
  ${NVWAL_SRC_ROOT}/third_party/libbacktrace
  ${NVWAL_SRC_ROOT}/third_party/gtest-1.7.0/include
  ${NVWAL_SRC_ROOT}/third_party/tinyxml2-2.1.0
  ${NVWAL_SRC_ROOT}/third_party/valgrind-3.9.0/include)



#############################################################################
#
#               Individual testcases.
#
#############################################################################


add_executable(test_nvwal_microbenchmark ${CMAKE_CURRENT_SOURCE_DIR}/test_nvwal_microbench.cpp)
target_link_libraries(test_nvwal_microbenchmark nvwal-shared)

