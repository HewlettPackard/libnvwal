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
#include <string.h>
#include <thread>
#include <assert.h>
#include "nvwal_api.h"
#include "nvwal_types.h"

namespace nvwaltest {
const int max_args = 11;
char const alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

class NvwalMicrobenchmark
{

private:

  struct NvwalConfig config_;
  struct NvwalContext wal_;
  struct timespec epoch_interval_;
  struct timespec write_interval_;
  uint64_t max_logrec_size_;
  nvwal_epoch_t current_stable_epoch_;
  nvwal_epoch_t max_epoch_;

  void circular_dest_memcpy(
    char* circular_dest_base,
    char* src,
    uint64_t circular_dest_size,
    uint64_t circular_dest_cur_offset,
    uint64_t bytes_to_copy) {
    assert(circular_dest_size >= circular_dest_cur_offset);
    assert(circular_dest_size >= bytes_to_copy);
    if (circular_dest_cur_offset + bytes_to_copy >= circular_dest_size) {
      /** We need a wrap-around */
      uint64_t bytes = circular_dest_size - circular_dest_cur_offset;
      if (bytes) {
        memcpy(circular_dest_base + circular_dest_cur_offset, src, bytes);
        src += bytes;
        bytes_to_copy -= bytes;
      }
      circular_dest_cur_offset = 0;
    }

    memcpy(circular_dest_base, src, bytes_to_copy);
  }

  void generate_random_buffer(char *buf, size_t const len)
  {
    for (unsigned int i = 0; i < len; i++)
    {
      buf[i] = alphabet[rand()% sizeof(alphabet)];
    }
  } 

  int do_logging(struct NvwalWriterContext * writer)
  {
    nvwal_error_t rc;
    char buf[max_logrec_size_];
    generate_random_buffer(buf, max_logrec_size_);

    size_t bytes_written = 0;
    nvwal_epoch_t current_epoch;
    writer->active_frame_ = 0;
    struct NvwalWriterEpochFrame * epoch_frame = &(writer->epoch_frames_[writer->active_frame_]);
    epoch_frame->head_offset_ = 0;
    epoch_frame->tail_offset_ = 0;
    rc = nvwal_query_durable_epoch(writer->parent_, &current_epoch);
    printf("Durable epoch: %lu\n", current_epoch);
    current_epoch = nvwal_increment_epoch(current_epoch);
    current_epoch = nvwal_increment_epoch(current_epoch);
    epoch_frame->log_epoch_ = current_epoch;

    do
    {

      do
      {
        /* stable epoch is right behind us so finish our use of this epoch */
        if (current_epoch == nvwal_increment_epoch(current_stable_epoch_))
        {
          rc = nvwal_on_wal_write(writer, bytes_written, current_epoch);
          current_epoch = nvwal_increment_epoch(current_epoch);
          bytes_written = 0;
          break; /* we are done with this epoch */
        } else if (current_epoch <= current_stable_epoch_)
        {
          /* we are lagging behind */
          current_epoch = nvwal_increment_epoch(current_stable_epoch_);
          current_epoch = nvwal_increment_epoch(current_epoch);
          break;
        }

        /* write some number of log records for this epoch */
        printf("Writer logging in epoch %lu\n", current_epoch);
        while (!nvwal_has_enough_writer_space(writer)) { /* spin */ }

        /* write some bytes into log buffer */
        /* randomly generated junk with random length? */   
        size_t bytes_to_write = 100;/* do randr() or something later */ 
        circular_dest_memcpy((char *)(writer->buffer_), buf, config_.writer_buffer_size_,
                               epoch_frame->tail_offset_, bytes_to_write);
        /* update last_tail_offset_ */
        epoch_frame->tail_offset_ += bytes_to_write;
        if (epoch_frame->tail_offset_ > config_.writer_buffer_size_)
        {
          epoch_frame->tail_offset_ %= config_.writer_buffer_size_;
        }
        writer->last_tail_offset_ = epoch_frame->tail_offset_;

        bytes_written += bytes_to_write; 
        nanosleep(&write_interval_, NULL);
      } while (1);

      /* done with this frame. set up a new one. */
      printf("Writer advancing frame, will log in epoch %lu\n", current_epoch);
      writer->active_frame_++;
      writer->active_frame_ %= kNvwalEpochFrameCount;
      epoch_frame = &(writer->epoch_frames_[writer->active_frame_]);
      epoch_frame->head_offset_ = writer->last_tail_offset_;
      epoch_frame->tail_offset_ = writer->last_tail_offset_;
      //current_epoch = nvwal_increment_epoch(current_epoch);
      epoch_frame->log_epoch_ = current_epoch;

      /* exit condition? */
      if (current_epoch > max_epoch_) { break; }


    } while(1);

  }


public:

  NvwalMicrobenchmark()
  {
    memset(&config_, 0, sizeof(config_));
    memset(&wal_, 0, sizeof(wal_));
  }

  ~NvwalMicrobenchmark()
  {

  }

  inline void set_nv_root(char * root_) { strcpy(config_.nv_root_, root_); }

  inline void set_disk_root(char *root_) { strcpy(config_.disk_root_, root_); }

  inline void set_writer_count(uint32_t num_writers_) { config_.writer_count_ = num_writers_; }

  inline void set_segment_size(uint64_t seg_size_) { config_.segment_size_ = seg_size_; }

  inline void set_nv_quota(uint64_t quota_) { config_.nv_quota_ = quota_; }

  inline void set_writer_buffer_size(uint64_t buf_size_) { config_.writer_buffer_size_ = buf_size_; }

  inline void set_mds_page_size(uint64_t pg_size_) { config_.mds_page_size_ = pg_size_; }

  inline void set_epoch_interval(uint64_t nanosecs_) 
  {
    epoch_interval_.tv_sec = nanosecs_/1000000000;
    epoch_interval_.tv_nsec = nanosecs_ - epoch_interval_.tv_sec*1000000000;
  }
  
  inline void set_write_interval(uint64_t nanosecs_) 
  {
    write_interval_.tv_sec = nanosecs_/1000000000;
    write_interval_.tv_nsec = nanosecs_ - epoch_interval_.tv_sec*1000000000;
  }

  inline void set_max_epoch(nvwal_epoch_t max_) { max_epoch_ = max_; }


  int run_test()
  {

    srand(time(NULL));

    int num_threads = config_.writer_count_ + 2;
    std::thread workers[num_threads];

    for (int i = 0; i < config_.writer_count_; i++)
    {
      config_.writer_buffers_[i] = (nvwal_byte_t *)malloc(config_.writer_buffer_size_);
    }

    if (nvwal_init(&config_, kNvwalInitCreateTruncate, &wal_))
    {

    }

    printf("Finished Nvwal Init\n"); 

    workers[num_threads-2] = std::thread([this]{nvwal_flusher_main(&wal_);});
    workers[num_threads-1] = std::thread([this]{nvwal_fsync_main(&wal_);});
    nvwal_wait_for_flusher_start(&wal_);
    nvwal_wait_for_fsync_start(&wal_);

    printf("Launched flusher and syncer threads\n");

    for (int i = 0; i < config_.writer_count_; i++)
    {
      workers[i] = std::thread([this, i](){this->do_logging(&(wal_.writers_[i]));});
    }

    /* Everyone else is working now. Increment the epoch counter periodically. */
    printf("Launched writer threads\n");
    do
    {
      nanosleep(&epoch_interval_, NULL);
      current_stable_epoch_ = nvwal_increment_epoch(current_stable_epoch_);
      nvwal_advance_stable_epoch(&wal_, current_stable_epoch_);
      printf("Main thread, stable epoch: %lu\n", current_stable_epoch_);
    } while (current_stable_epoch_ <= max_epoch_);

    printf("Joining writer threads\n");
    for (int i = 0; i < config_.writer_count_; i++)
    {
      if (workers[i].joinable()) {workers[i].join();}
    }

    nvwal_uninit(&wal_);

    if (workers[num_threads-1].joinable()) {workers[num_threads-1].join();}
    if (workers[num_threads-2].joinable()) {workers[num_threads-2].join();}

    for (int i = 0; i < config_.writer_count_; i++)
    {
      free(config_.writer_buffers_[i]);
    }
  }

};
}
void usage()
{
  printf("test_nvwal_microbench nv_root disk_root writer_count segment_size nv_quota writer_buffer_size mds_page_size epoch_interval write_interval max_epoch\n");
  printf("example: \n");
}

int main(int argc, char *argv[])
{
  if (nvwaltest::max_args != argc)
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

  nvwaltest::NvwalMicrobenchmark mb;

  mb.set_nv_root(argv[1]);
  mb.set_disk_root(argv[2]);
  mb.set_writer_count(atoi(argv[3]));
  if ((0 != atoi(argv[4])) && (0 == atoi(argv[4])%512))
  {
    mb.set_segment_size(atoi(argv[4]));
  } else
  {
    printf("Using default segment size\n");
  }
  mb.set_nv_quota(atoi(argv[5]));
  mb.set_writer_buffer_size(atoi(argv[6]));
  if ((0 != atoi(argv[7])) && (0 == atoi(argv[7])%512))
  {
    mb.set_mds_page_size(atoi(argv[7]));
  } else
  {
    printf("Using default MDS page size\n");
  }
  mb.set_epoch_interval(atoi(argv[8]));
  mb.set_write_interval(atoi(argv[9]));
  mb.set_max_epoch(atoi(argv[10]));

  mb.run_test();

  return 0;

}


