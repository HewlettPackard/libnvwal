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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <libpmem.h>

#include <gflags/gflags.h>

#include <boost/filesystem.hpp>

#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

#include "nvwal_api.h"
#include "nvwal_types.h"
#include "nvwal_util.h"

DEFINE_string(nvm_file, "",
  "NV-DIMM file name.");

DEFINE_uint64(nops, 1000000, 
  "Number of workload operations per worker.");

DEFINE_uint64(nrepeats, 1, 
  "Number of repetitions.");

DEFINE_uint64(data_size, 1U << 12, 
  "Number of bytes written per workload operation.");

DEFINE_string(bench, "memcpy", 
  "Benchmark: memcpy, memcpy_persist, pmem_memcpy_persist");



const int max_args = 11;
char const alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";


void generate_random_buffer(nvwal_byte_t *buf, size_t const len)
{
  for (unsigned int i = 0; i < len; i++)
  {
    buf[i] = alphabet[rand()% sizeof(alphabet)];
  }
} 


class Workload {
public:
  Workload(const char* bench_name, const char* nvm_file, uint64_t data_size, uint64_t nops);
  nvwal_error_t run(int nthreads);
  
  int init();
  int do_work(int tid);
  void memcpy(void *dest, const void *src, size_t n);
  void memcpy_persist(void *dest, const void *src, size_t n);
  void pmem_memcpy_persist(void *dest, const void *src, size_t n);

private:
  const char* nvm_file_;
  uint64_t data_size_;
  uint64_t nops_;

private:
  char* pmemaddr_;
  void (Workload::*memcpy_method_)(void *dest, const void *src, size_t n);
};


Workload::Workload(const char* bench_name, const char* nvm_file, uint64_t data_size, uint64_t nops)
  : nvm_file_(nvm_file),
    data_size_(data_size),
    nops_(nops)
{
  init();

  std::string bname(bench_name);
  if(bname == "memcpy") {
    memcpy_method_ = &Workload::memcpy;
  } else if (bname == "memcpy_persist") {
    memcpy_method_ = &Workload::memcpy_persist;
  } else if (bname == "pmem_memcpy_persist") {
    memcpy_method_ = &Workload::pmem_memcpy_persist;
  }
}

int Workload::init()
{
  int fd;

  /* create a pmem file */
  if ((fd = open(nvm_file_, O_CREAT|O_RDWR, 0666)) < 0) {
    perror("open");
    exit(1);
  }

  uint64_t pmem_len = nops_ * data_size_;

  /* allocate the pmem */
  if ((errno = posix_fallocate(fd, 0, pmem_len)) != 0) {
    perror("posix_fallocate");
    exit(1);
  }

  /* memory map it */
  if ((pmemaddr_ = (char*) mmap(0, pmem_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == NULL) {
    perror("mmap");
    exit(1);
  }
  close(fd);
}

void Workload::memcpy(void *dest, const void *src, size_t n)
{
  ::memcpy(dest, src, n);
}

void Workload::memcpy_persist(void *dest, const void *src, size_t n)
{
  ::memcpy(dest, src, n);
  pmem_persist(dest, n);
}

void Workload::pmem_memcpy_persist(void *dest, const void *src, size_t n)
{
  ::pmem_memcpy_persist(dest, src, n);
}

int Workload::do_work(int tid)
{
  nvwal_error_t rc;
  std::unique_ptr<char> buf_ptr(new char[data_size_]);
  char* buf = buf_ptr.get();
  generate_random_buffer((nvwal_byte_t*) buf, data_size_);

  auto start = std::chrono::steady_clock::now();

  uint64_t nrepeats = FLAGS_nrepeats;
  for (int r=0; r<nrepeats; r++) {
    for (int i=0; i<nops_; i++) {
      char* dst=&pmemaddr_[i*data_size_];
      (*this.*memcpy_method_)(dst, buf, data_size_);
    }
  }

  auto end = std::chrono::steady_clock::now();
  auto diff = end - start;

  uint64_t duration_usec = std::chrono::duration<double, std::ratio<1, 1000000>> (diff).count();

  std::cout << "workload_duration " << duration_usec << " us" << std::endl;
  std::cout << "throughput " << nrepeats*nops_* data_size_ / duration_usec<< " MB/s" << std::endl;
}

nvwal_error_t Workload::run(int nthreads)
{
    std::thread workers[nthreads];

    for (int i=0; i<nthreads; i++)
    {
      workers[i] = std::thread([this, i](){this->do_work(i);});
    }

    for (int i=0; i<nthreads; i++)
    {
      if (workers[i].joinable()) {workers[i].join();}
    }
}


int main(int argc, char *argv[])
{
  nvwal_error_t rc;

  gflags::SetUsageMessage(
    "A microbenchmark emulating a LSN-based log style workload for libnvwal.");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_nvm_file.empty()) {
    std::cerr << "NV-DIMM file not given." << std::endl;
    return 1;
  }

  Workload workload(FLAGS_bench.c_str(), FLAGS_nvm_file.c_str(), FLAGS_data_size, FLAGS_nops);
  workload.run(1);

  return 0;
}
