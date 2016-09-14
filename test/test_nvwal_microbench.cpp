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

#include <iostream>
#include <time.h>

#include "nvwal_api.h"
#include "nvwal_types.h"

#if 0
const int max_args = 9;

class NvwalMicrobenchmark
{

private:

  struct NvwalConfig config_;
  struct NvwalContext wal_;
  struct NvwalWriterContext * w_context_;
  struct timespec epoch_interval_;
  struct timespec write_interval_;
  uint64_t max_logrec_size_;
  nvwal_epoch_t current_global_epoch_;
  nvwal_epoch_t max_epoch_;
 
  int do_logging(struct NvwalWriterContext * writer)
  {
    nvwal_error_t rc;
    char buf[max_logrec_size];
    size_t bytes_written = 0;
    nvwal_epoch_t current_epoch;

    do
    {
      while (!nvwal_has_enough_writer_space(writer)) { /* spin */ }

     /* write some bytes into log buffer */
     /* randomly generated junk with random length? */     
     memcpy((char *)(writer->buffer_), buf, bytes_written);

     rc = nvwal_on_wal_write(writer, bytes_written, current_epoch_);

     /* need to catch up to global epoch? */

      /* exit condition? */
      if (current_global_epoch => max_epoch) { break; }

    } while(1);

  }


public:

  NvwalMicrobenchmark()
  {
    memset(&config_, 0, sizeof(config_));
    memset(&wal_, 0 sizeof(wal_));
    //max_epoch_ = XXX;
  }

  ~NvwalMicrobenchmark()
  {

  }

  inline set_nv_root(char * root_) { strcpy(&(config_.nv_root_), root_); }

  inline set_disk_root(char *root_) { strcpy(&(config_.disk_root_), root_); }

  inline set_writer_count(uint32_t num_writers_) { config_.writer_count_ = num_writers_; }

  inline set_segment_size(uint64_t seg_size_) { config_.segment_size_ = seg_size_; }

  inline set_nv_quota(uint64_t quota_) { config_.nv_quota_ = quota_; }

  inline set_writer_buffer_size(uint64_t buf_size_) { config_.writer_buffer_size_ = buf_size_; }

  inline set_mds_page_size(uint64_t pg_size_) { config_.mds_page_size_ = pg_size_; }

  inline set_epoch_interval(uint64_t nanosecs_) 
  {
    epoch_interval_.tv_sec = nanosecs_/1000000000;
    epoch_interval_.tv_nsec = nanosecs - epoch_interval_.tv_sec*1000000000;
  }

  int run_test()
  {
    int num_threads = config_.writer_count_ + 2;
    w_context_ = (struct NvwalWriterContext *)malloc(sizeof(struct NvwalWriterContext)*config_.writer_count_);
    std::thread workers[num_threads];

    for (int i = 0; i < config_writer_count_; i++)
    {
      config_.writer_buffers_[i] = (nvwal_byte_t *)malloc(config_.writer_buffer_size_);
    }

    if (nvwal_init(&config_, &wal_))
    {

    }

    

    workers[num_threads-2] = std::thread([this]{nvwal_flusher_main(&wal_)});
    workers[num_threads-1] = std::thread([this]{this->nvwal_fsync_main(&wal_)});
    nvwal_wait_for_flusher_start(&wal_);
    nvwal_wait_for_syncer_start(&wal_);

    for (int i = 0; i < config_.writer_count_)
    {
      workers[i] = std::thread([this, i](){this->do_logging(w_context_[i]);});
    }

    /* Everyone else is working now. Increment the epoch counter periodically. */
    do
    {
      nanosleep(epoch_interval_, NULL);
      nvwal_increment_epoch(current_global_epoch);
      nvwal_advance_stable_epoch(&wal_, current_global_epoch);
    } while (current_global_epoch < max_epoch);

    for (int i = 0; i < num_threads; i++)
    {
      if (workers[i].joinable()) {workers[i].join();}
    }

    nvwal_uninit(&wal_);

  }

};

void usage()
{
  printf("test_nvwal_microbench nv_root disk_root writer_count segment_size nv_quota writer_buffer_size mds_page_size epoch_interval sleep_interval max_epoch\n");
  printf("example: \n");
}

int main(int argc, char *argv[])
{
  if (max_args != argc)
  {
    usage();
    return -1;
  }

    //config_.nv_root_
    //config_.disk_root_
    //config_.writer_count_ = num_threads;
    //config_.segment_size_ //has default value
    //config_.nv_quota
    //config_.writer_buffer_size_
    //config_.writer_buffers_
    //config_.mds_page_size_) //has default value

  NvwalMicrobenchmark mb();

  mb.set_nv_root();
  mb.set_disk_root();
  mb.set_writer_count();
  if ((0 != atoi(argv[])) && (0 == atoi(argv[])%512))
  {
    mb.set_segment_size();
  }
  mb.set_nv_quota();
  mb.set_writer_buffer_size();
  if ((0 != atoi(argv[])) && (0 == atoi(argv[])%512))
  {
    mb.set_mds_page_size();
  }
  mb.set_epoch_interval();

  return 0;

}
#endif
