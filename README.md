# libnvwal : Write-Ahead-Logging library for Non-volatile DIMM

## Overview

Write-Ahead-Logging (WAL) is the central component in various software that require
atomicity and durability, such as data management systems (DBMS).
The durability and low-latency of non-volatile DIMM (NVDIMM) is a great fit for WAL.
This library, _libnvwal_, is a handy user-space library for such software to manage
WAL on NVDIMM with little effort.

The library offers the following benefits:
* Without libnvwal, individual applications must implement their own layering
logics on NVDIMM and Disk (HDD/SSD) to store WAL files larger than NVDIMM.
* This would result in either 1) significant code changes on WAL,
which is usually one of the most complex and critical modules,
or 2) inefficiency via filesystem-level abstraction, which cannot
exploit the unique access pattern of WAL.
* libnvwal solves the issue for a wide variety of DBMS.

## Dependencies

- Install additional packages

  Debian/Ubuntu

  ```
  $ sudo apt-get install build-essential cmake doxygen valgrind
  $ sudo apt-get install libdwarf-dev libunwind-devlibunwind-dev
  ```

  RedHat/CentOS/Fedora

  ```
  $ sudo yum groupinstall "Development Tools"  
  $ sudo yum install cmake doxygen valgrind valgrind-devel
  $ sudo yum install libdwarf libdwarf-devel libunwind libunwind-devel
  ```

- Install libpmem

  You can either install [pre-built packages]((https://github.com/pmem/nvml/releases)
  or build from source:

  ```
  $ sudo apt-get install autoconf pkg-config doxygen graphviz
  $ git clone https://github.com/pmem/nvml.git
  $ cd nvml
  $ make
  $ sudo make install
  ```

## Build

To compile this project, simply build it as a CMake project. For example:

  ```
  # Suppose you are at libnvwal source folder.
  # We prohibit in-source build, so you have to create a build folder and compile there.
  $ mkdir build
  $ cd build
  # You can also use Release/RelWithDebInfo just like usual CMake projects.
  $ cmake ../  -DCMAKE_BUILD_TYPE=Debug
  $ make
  ```

Or, import it to C++ IDE, such as kdevelop. Any IDEs that support CMake build should work.

## Test

libnvwal comes with several unittests we regularly run.
We recommend you to run them in your environment to check if
the compilation was successful.

Go to build folder, and:

  ```
  ctest
  ```

If everything goes fine, you would see an output like the following:

    [alice@dome build]$ ctest
    Test project /home/alice/projects/libnvwal/build
          Start  1: test_nvwal_cursor_NoLog
    1/70 Test  #1: test_nvwal_cursor_NoLog .................................   Passed    0.04 sec
          Start  2: test_nvwal_cursor_OneWriterOneEpoch
    2/70 Test  #2: test_nvwal_cursor_OneWriterOneEpoch .....................   Passed    0.04 sec
    .... <Snip>  .....
          Start 69: valgrind_test_nvwal_writer_TwoWritersSingleThread
    69/70 Test #69: valgrind_test_nvwal_writer_TwoWritersSingleThread .......   Passed    1.71 sec
          Start 70: valgrind_test_nvwal_writer_TwoWritersConcurrent
    70/70 Test #70: valgrind_test_nvwal_writer_TwoWritersConcurrent .........   Passed    2.02 sec

    100% tests passed, 0 tests failed out of 70

    Total Test time (real) =  42.13 sec

## Examples

Take a look at the _example_ folder for detailed examples and demonstrations.
Especially, start with get_started.cpp to see the basic interface of libnvwal.

## API Documents

Go to build folder, and type:

    make dox
    google-chrome dox/html/group__LIBNVWAL.html
