# Unit test project for libnvwal.
# Under this folder, we don't hesitate using C++. We even allow C++11.
# We can also freely use gtest, glog, etc etc.
# Long live C++!
cmake_minimum_required(VERSION 2.6)
cmake_policy(SET CMP0003 NEW)
project(nvwal_test)


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

add_library(nvwal_test_common SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/nvwal_test_common.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/nvwal_test_mds_common.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/nvwal_stacktrace.cpp)
target_link_libraries(nvwal_test_common tinyxml2static backtrace nvwal-shared ${CMAKE_THREAD_LIBS_INIT} ${Boost_LIBRARIES})

# This one adds individual tests as individual add_test entity, thus showing up nicely in
# results. You should prefer this one. However, you have to list individual tests.
# Automating this is possible, but tricky and I wouldn't rely on it.
#  param: individual_tests a _list_ variable to contain names of individual tests
# Related efforts:
#   http://baoilleach.blogspot.com/2013/06/using-ctest-with-multiple-tests-in.html
#   http://smspillaz.wordpress.com/2012/07/05/unit-test-autodiscovery-with-cmake-and-google-test/
function (add_nvwal_test_individual targetname individual_tests)
  add_nvwal_test_individual_without_valgrind("${targetname}" "${individual_tests}")
  add_nvwal_test_individual_valgrind("${targetname}" "${individual_tests}")
endfunction()

# Use this one when the testcase must NOT have a valgrind version (taking too long etc)
function (add_nvwal_test_individual_without_valgrind targetname individual_tests)
  add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${targetname}.cpp)
  target_link_libraries(${targetname} tinyxml2static backtrace nvwal-shared nvwal_test_common gtest_main ${CMAKE_THREAD_LIBS_INIT})

  foreach(individual_test ${individual_tests})
    # To pick up test report xml files from Jenkins,
    # Set "test-reports/*.xml" as the report file filter.
    add_test(
      NAME ${targetname}_${individual_test}
      COMMAND ${targetname}
        --gtest_filter=*.${individual_test}
        --gtest_output=xml:${NVWAL_TEST_REPORT_DIR}/${targetname}_${individual_test}.xml)
    if(${enable_valgrind})
      # also add valgrind version. This version takes long, so you might want to exclude
      # them from ctest by "ctest -E valgrind".
      # To pick up valgrind report xml files from Jenkins,
      # Set "valgrind-reports/*.xml" as the valgrind plugin filter.
      add_test(
        NAME valgrind_${targetname}_${individual_test}
        COMMAND
          ${MEMORYCHECK_COMMAND}
          --xml=yes --xml-file=${NVWAL_VALGRIND_REPORT_DIR}/${targetname}_${individual_test}.xml
          --error-exitcode=1 --leak-check=full --gen-suppressions=all
          # removed --show-leak-kinds=all, which is supported only by valgrind 3.9 and later
          --suppressions=${NVWAL_SRC_ROOT}/scripts/valgrind.supp
          ./${targetname}
          --gtest_filter=*.${individual_test}
          --gtest_output=xml:${NVWAL_TEST_REPORT_DIR}/valgrind_${targetname}_${individual_test}.xml
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    endif()
  endforeach(individual_test individual_tests)
endfunction()

function (add_nvwal_test_individual_valgrind targetname individual_tests)
  foreach(individual_test ${individual_tests})
    # also add valgrind version. This version takes long, so you might want to exclude
    # them from ctest by "ctest -E valgrind".
    # To pick up valgrind report xml files from Jenkins,
    # Set "valgrind-reports/*.xml" as the valgrind plugin filter.
    add_test(
      NAME valgrind_${targetname}_${individual_test}
      COMMAND
        ${MEMORYCHECK_COMMAND}
        --xml=yes --xml-file=${NVWAL_VALGRIND_REPORT_DIR}/${targetname}_${individual_test}.xml
        --error-exitcode=1 --leak-check=full --gen-suppressions=all
        # removed --show-leak-kinds=all, which is supported only by valgrind 3.9 and later
        --suppressions=${NVWAL_SRC_ROOT}/scripts/valgrind.supp
        ./${targetname}
        --gtest_filter=*.${individual_test}
        --gtest_output=xml:${NVWAL_TEST_REPORT_DIR}/valgrind_${targetname}_${individual_test}.xml
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  endforeach(individual_test individual_tests)
endfunction()


#############################################################################
#
#               Individual testcases.
# Just follow what other testcases do.
#
#############################################################################

add_nvwal_test_individual(test_nvwal_example "Test1;Test2")

add_nvwal_test_individual(test_nvwal_mds "Init;WriteEpochSingle;WriteEpochOneBatchSmall;WriteEpochOneBatchLarge;WriteEpochTwoBatches")

add_nvwal_test_individual(test_nvwal_mds_io "Init;AppendPage")

add_nvwal_test_individual(test_nvwal_util "CircularMemcpy;CircularMemcpyWrap;ConcatSequenceFilename;IsNonemptyDir")

add_nvwal_test_individual(test_nvwal_writer "OneEpoch;TwoEpochs")


