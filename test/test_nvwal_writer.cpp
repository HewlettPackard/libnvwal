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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#include "nvwal_api.h"

#include "nvwal_test_common.hpp"

/**
 * @file test_nvwal_writer.cpp
 * Test the writer piece separately.
 */

namespace nvwaltest {


TEST(NvwalWriterTest, OneEpoch) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = resource->wal_instance_.writers_ + 0;
  const uint32_t kBytes = 64;

  std::memset(buffer, 42, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  std::memset(buffer + kBytes, 24, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalWriterTest, TwoEpochs) {
  TestContext context(1);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = resource->wal_instance_.writers_ + 0;
  const uint32_t kBytes = 64;

  std::memset(buffer, 42, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 1));

  std::memset(buffer + kBytes, 24, kBytes);
  EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
  EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, 2));

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalWriterTest, ManyEpochsBufferWrapAround) {
  TestContext context(1, TestContext::kExtremelyTiny);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  const uint32_t buffer_size = wal->config_.writer_buffer_size_;
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = wal->writers_ + 0;
  const uint32_t kBytes = 128;
  const uint32_t kReps = 100;

  EXPECT_TRUE(buffer_size % kBytes == 0);  // This simplifies a bit
  for (int i = 0; i < kReps; ++i) {
    EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
    const uint32_t offset = (kReps * kBytes) % buffer_size;
    std::memset(buffer + offset, static_cast<nvwal_byte_t>(i), kBytes);
    EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, i + 1));
    EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, i + 1));
    EXPECT_EQ(0, context.wait_until_durable(wal, i + 1));
    if (i % 10 == 0) {
      std::cout << i << "/" << kReps << std::endl;
    }
  }

  EXPECT_EQ(0, context.uninit_all());
}

TEST(NvwalWriterTest, TooManyEpochsBufferWrapAround) {
  TestContext context(1, TestContext::kExtremelyTiny);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  const uint32_t buffer_size = wal->config_.writer_buffer_size_;
  auto* buffer = resource->writer_buffers_[0].get();
  auto* writer = wal->writers_ + 0;
  const uint32_t kBytes = 128;
  const uint32_t kReps = 100;

  EXPECT_TRUE(buffer_size % kBytes == 0);  // This simplifies a bit
  for (int i = 0; i < kReps; ++i) {
    EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
    const uint32_t offset = (kReps * kBytes) % buffer_size;
    std::memset(buffer + offset, static_cast<nvwal_byte_t>(i), kBytes);
    EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, i + 1));
    EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, i + 1));
    EXPECT_EQ(0, context.wait_until_durable(wal, i + 1, 0));
  }

  EXPECT_EQ(0, context.uninit_all());
}



TEST(NvwalWriterTest, TwoWritersSingleThread) {
  TestContext context(1, TestContext::kExtremelyTiny);
  EXPECT_EQ(0, context.init_all());

  auto* resource = context.get_resource(0);
  auto* wal = &resource->wal_instance_;
  const uint32_t buffer_size = wal->config_.writer_buffer_size_;
  const uint32_t kBytes = 128;
  const uint32_t kReps = 100;

  EXPECT_TRUE(buffer_size % kBytes == 0);  // This simplifies a bit
  for (int i = 0; i < kReps; ++i) {
    const uint32_t offset = (kReps * kBytes) % buffer_size;
    for (int j = 0; j < 2; ++j) {
      auto* buffer = resource->writer_buffers_[j].get();
      auto* writer = wal->writers_ + j;
      EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
      std::memset(buffer + offset, static_cast<nvwal_byte_t>(i), kBytes);
      EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, i + 1));
    }
    EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, i + 1));
    EXPECT_EQ(0, context.wait_until_durable(wal, i + 1));
    if (i % 10 == 0) {
      std::cout << i << "/" << kReps << std::endl;
    }
  }

  EXPECT_EQ(0, context.uninit_all());
}

struct ConcurrentTest {
  enum Constants {
    kReps = 100,
  };
  ConcurrentTest() :
    context_(1, TestContext::kExtremelyTiny),
    writer_count_(2U) {
    global_epoch_.store(0);
    written_in_this_epoch_.store(0);
  }
  void run() {
    EXPECT_EQ(0, context_.init_all());
    EXPECT_TRUE(writer_count_ <= context_.get_wal(0)->config_.writer_count_);
    launch_writers();

    auto* wal = context_.get_wal(0);
    for (int i = 0; i < kReps; ++i) {
      const nvwal_epoch_t next_epoch = i + 1U;

      // Let writers write a log in next_epoch
      written_in_this_epoch_.store(0);
      global_epoch_.store(next_epoch);
      while (written_in_this_epoch_.load(std::memory_order_acquire) < writer_count_) {
        std::this_thread::yield();
      }

      // Make next_epoch a durable epoch
      EXPECT_EQ(0, nvwal_advance_stable_epoch(wal, next_epoch));
      EXPECT_EQ(0, context_.wait_until_durable(wal, next_epoch));

      if (i % 10 == 0) {
        std::cout << i << "/" << kReps << std::endl;
      }
    }

    join_writers();
    EXPECT_EQ(0, context_.uninit_all());
  }

  void launch_writers() {
    for (int i = 0; i < writer_count_; ++i) {
      writer_threads_.emplace_back(&ConcurrentTest::writer_main, this, i);
    }
  }
  void join_writers() {
    for (auto& t : writer_threads_) {
      t.join();
    }
  }

  void writer_main(int me) {
    std::cout << "Writer-" << me << " started" << std::endl;
    auto* wal = context_.get_wal(0);
    const uint32_t buffer_size = wal->config_.writer_buffer_size_;
    auto* buffer = context_.get_resource(0)->writer_buffers_[me].get();
    auto* writer = wal->writers_ + me;
    const uint32_t kBytes = 128;
    EXPECT_TRUE(buffer_size % kBytes == 0);  // This simplifies a bit

    for (int i = 0; i < kReps; ++i) {
      const nvwal_epoch_t ep = i + 1U;
      while (global_epoch_.load(std::memory_order_acquire) != ep) {
        std::this_thread::yield();
      }

      EXPECT_EQ(1U, nvwal_has_enough_writer_space(writer));
      const uint32_t offset = (i * kBytes) % buffer_size;
      std::memset(buffer + offset, static_cast<nvwal_byte_t>(ep), kBytes);
      EXPECT_EQ(0, nvwal_on_wal_write(writer, kBytes, ep));

      written_in_this_epoch_.fetch_add(1);
    }
    std::cout << "Writer-" << me << " exit" << std::endl;
  }

  TestContext context_;
  const int writer_count_;

  std::atomic< nvwal_epoch_t > global_epoch_;
  std::vector< std::thread > writer_threads_;

  /**
   * How many writers completed writing a log in the current epoch,
   *
   * In a realistic situation, each writer catches up with global epoch
   * without this kind of central memory (otherwise what's the point of
   * "distributed" logging), using so-called "in-commit-epoch-guard".
   *
   * But, in this testcase we don't have to be that real.
   * Simply use this as a big/inefficient barrier for each epoch.
   */
  std::atomic< int > written_in_this_epoch_;
};

TEST(NvwalWriterTest, TwoWritersConcurrent) {
  ConcurrentTest test;
  test.run();
}

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalWriterTest);
