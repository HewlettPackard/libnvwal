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

#include "nvwal_impl_pin.h"
#include "nvwal_test_common.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

/**
 * @file test_nvwal_pin.cpp
 * Pin/unpin unit test.
 * The pin/unpin functions are designed to be independent
 * to help write this unittests.
 * It just receives an int pointer, so we don't need
 * any context object in these tests.
 */

namespace nvwaltest {

TEST(NvwalPinTest, NoContention) {
  int32_t count = 0;
  EXPECT_TRUE(nvwal_pin_read_try_lock(&count));
  EXPECT_EQ(1, count);

  EXPECT_FALSE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(1, count);

  nvwal_pin_read_unconditional_lock(&count);
  EXPECT_EQ(2, count);

  EXPECT_FALSE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(2, count);

  EXPECT_TRUE(nvwal_pin_read_try_lock(&count));
  EXPECT_EQ(3, count);

  EXPECT_FALSE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(3, count);

  nvwal_pin_read_unlock(&count);
  EXPECT_EQ(2, count);

  EXPECT_FALSE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(2, count);

  nvwal_pin_read_unlock(&count);
  EXPECT_EQ(1, count);

  EXPECT_FALSE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(1, count);

  nvwal_pin_read_unlock(&count);
  EXPECT_EQ(0, count);

  EXPECT_TRUE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(kNvwalPinExclusive, count);

  EXPECT_FALSE(nvwal_pin_read_try_lock(&count));
  EXPECT_EQ(kNvwalPinExclusive, count);

  EXPECT_FALSE(nvwal_pin_flusher_try_lock(&count));
  EXPECT_EQ(kNvwalPinExclusive, count);

  nvwal_pin_flusher_unlock(&count);
  EXPECT_EQ(0, count);
}

class ConcurrentPinTest {
 public:
  static void run(int flusher_count, int reader_count) {
    ConcurrentPinTest test(flusher_count, reader_count);
    test.launch_threads();
    test.started_.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    test.ended_.store(true);
    test.join_threads();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    EXPECT_EQ(0, test.pins_);
    EXPECT_EQ(0, test.flusher_lock_count_.load());
    EXPECT_EQ(0, test.reader_lock_count_.load());
  }

 private:
  ConcurrentPinTest(int flusher_count, int reader_count)
    : flusher_count_(flusher_count), reader_count_(reader_count), pins_(0) {
    flusher_lock_count_.store(0);
    reader_lock_count_.store(0);
    started_.store(false);
    ended_.store(false);
  }

  void launch_threads() {
    for (int i = 0; i < flusher_count_; ++i) {
      flusher_threads_.emplace_back(&ConcurrentPinTest::flusher_main, this, i);
    }
    for (int i = 0; i < reader_count_; ++i) {
      reader_threads_.emplace_back(&ConcurrentPinTest::reader_main, this, i);
    }
  }
  void join_threads() {
    for (auto& t : flusher_threads_) {
      t.join();
    }
    for (auto& t : reader_threads_) {
      t.join();
    }
  }

  void flusher_main(int me) {
    std::cout << "Flusher-" << me << " launched" << std::endl;
    while (!started_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    std::cout << "Flusher-" << me << " started" << std::endl;
    while (!ended_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
      if (nvwal_pin_flusher_try_lock(&pins_)) {
        flusher_lock_count_.fetch_add(1);
        EXPECT_EQ(1, flusher_lock_count_.load());
        EXPECT_EQ(0, reader_lock_count_.load());
        flusher_lock_count_.fetch_sub(1);
        nvwal_pin_flusher_unlock(&pins_);
      }

      nvwal_pin_flusher_unconditional_lock(&pins_);
      flusher_lock_count_.fetch_add(1);
      EXPECT_EQ(1, flusher_lock_count_.load());
      EXPECT_EQ(0, reader_lock_count_.load());
      flusher_lock_count_.fetch_sub(1);
      nvwal_pin_flusher_unlock(&pins_);
    }
    std::cout << "Flusher-" << me << " exit" << std::endl;
  }

  void reader_main(int me) {
    std::cout << "Reader-" << me << " launched" << std::endl;
    while (!started_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    std::cout << "Reader-" << me << " started" << std::endl;
    while (!ended_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
      if (nvwal_pin_read_try_lock(&pins_)) {
        reader_lock_count_.fetch_add(1);
        EXPECT_EQ(0, flusher_lock_count_.load());
        reader_lock_count_.fetch_sub(1);
        nvwal_pin_read_unlock(&pins_);
      }

      nvwal_pin_read_unconditional_lock(&pins_);
      reader_lock_count_.fetch_add(1);
      EXPECT_EQ(0, flusher_lock_count_.load());
      reader_lock_count_.fetch_sub(1);
      nvwal_pin_read_unlock(&pins_);
    }
    std::cout << "Reader-" << me << " exit" << std::endl;
  }

  const int flusher_count_;
  const int reader_count_;

  int32_t pins_;

  std::atomic<bool> started_;
  std::atomic<bool> ended_;
  std::atomic<int32_t> flusher_lock_count_;
  std::atomic<int32_t> reader_lock_count_;

  std::vector< std::thread > flusher_threads_;
  std::vector< std::thread > reader_threads_;
};

TEST(NvwalPinTest, OneFlusherOneReader)     { ConcurrentPinTest::run(1, 1); }
TEST(NvwalPinTest, OneFlusherTwoReaders)    { ConcurrentPinTest::run(1, 2); }
TEST(NvwalPinTest, OneFlusherFourReaders)   { ConcurrentPinTest::run(1, 4); }
TEST(NvwalPinTest, TwoFlushersOneReader)    { ConcurrentPinTest::run(2, 1); }
TEST(NvwalPinTest, TwoFlushersTwoReaders)   { ConcurrentPinTest::run(2, 2); }
TEST(NvwalPinTest, TwoFlushersFourReaders)  { ConcurrentPinTest::run(2, 4); }

}  // namespace nvwaltest

TEST_MAIN_CAPTURE_SIGNALS(NvwalPinTest);
