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

#ifndef NVWAL_IMPL_INIT_H_
#define NVWAL_IMPL_INIT_H_
/**
 * @file nvwal_impl_init.h
 * Internal functions to unit/uninit context objects in libnvwal.
 * @ingroup LIBNVWAL_INTERNAL
 * @addtogroup LIBNVWAL_INTERNAL
 * @{
 */

#include <stdint.h>

#include "nvwal_fwd.h"
#include "nvwal_types.h"

/**
 * Internal implementation of nvwal_init().
 * Separated the implementation from nvwal_api.c because it becomes too big.
 * @copydoc nvwal_init()
 */
nvwal_error_t nvwal_impl_init(
  const struct NvwalConfig* given_config,
  enum NvwalInitMode mode,
  struct NvwalContext* wal);

/**
 * Internal implementation of nvwal_uninit().
 * Separated the implementation from nvwal_api.c because it becomes too big.
 * @copydoc nvwal_uninit()
 */
nvwal_error_t nvwal_impl_uninit(
  struct NvwalContext* wal);


/**
 * Constant to represent the state of an internal thread, like
 * flusher and fsyncher.
 * In order to be safe, both the host thread (the thread that invoked
 * nvwal_init/uninit) and flusher/fsyncher must atomic swap (CAS) to
 * change these states.
 */
enum NvwalThreadState {
  /**
   * Initial state before nvwal_init makes it ready to accept threadss.
   * This value must be zero to allow initialization by memzero.
   * \li When ready, nvwal_init will atomically change this to AcceptStart.
   * No other state-change is expected.
   */
  kNvwalThreadStateBeingInitialized = 0,

  /**
   * nvwal_init is done and now flusher/fsyncher can start.
   * \li flusher/fsyncher might CAS this to Running.
   * \li nvwal_uninit might CAS this to ProhibitStart.
   */
  kNvwalThreadStateAcceptStart,
  /**
   * flusher/fsyncher is running.
   * \li flusher/fsyncher might change this to Stopped.
   * \li nvwal_uninit might CAS this to RunningAndRequestedStop.
   */
  kNvwalThreadStateRunning,

  /**
   * While flusher/fsyncher is running, nvwal_uninit requested it to stop.
   * \li flusher/fsyncher will change this to Stopped. No race.
   */
  kNvwalThreadStateRunningAndRequestedStop,

  /**
   * A terminal state. No more change.
   * flusher/fsyncher ack-ed their stop.
   * nvwal_uninit will wait until it sees this value.
   */
  kNvwalThreadStateStopped,

  /**
   * Another terminal state.
   * nvwal_uninit directly finished from AcceptStart before
   * flusher/fsyncher start.
   *
   */
  kNvwalThreadStateProhibitStart,
};

/**
 * Change the state from kNvwalThreadStateBeingInitialized to
 * kNvwalThreadStateAcceptStart.
 * @see NvwalThreadState
 */
void nvwal_impl_thread_state_get_ready(
  uint8_t* thread_state);

/**
 * An internal method for the flusher/fsyncer thread
 * to change a thread-state variable
 * before it starts.
 * @see NvwalThreadState
 * @return the state as of the completion of this method.
 * \li kNvwalThreadStateRunning when this method successfully and atomically
 * changed the state from
 * kNvwalThreadStateAcceptStart to kNvwalThreadStateRunning.
 * \li kNvwalThreadStateProhibitStart when the WAL context already prohibits
 * a new thread to start.
 * \li All other values mean an unexpected failure, which might happen
 * for whatever reasons.
 */
enum NvwalThreadState nvwal_impl_thread_state_try_start(
  uint8_t* thread_state);

/**
 * An internal method for the host thread
 * to wait until a flusher/fsyncer thread ack-s its start.
 * @see NvwalThreadState
 */
void nvwal_impl_thread_state_wait_for_start(const uint8_t* thread_state);

/**
 * An internal method for the host thread
 * to request a flusher/fsyncer thread to stop and wait until they stop.
 * This method might block until the flusher/fsyncer thread finds the stop-request.
 * @see NvwalThreadState
 */
void nvwal_impl_thread_state_request_and_wait_for_stop(
  uint8_t* thread_state);


/**
 * Called by flusher/fsyncer thread to notify its end.
 * @see NvwalThreadState
 */
void nvwal_impl_thread_state_stopped(
  uint8_t* thread_state);

/** @} */

#endif  /* NVWAL_IMPL_INIT_H_ */
