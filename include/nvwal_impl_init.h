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
  kNvwalThreadStateNotInitialized = 0,

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
 * Change the state from kNvwalThreadStateNotInitialized to
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
 * to request a flusher/fsyncer thread to stop and wait until they stop.
 * This method might block until the flusher/fsyncer thread finds the stop-request.
 * @see NvwalThreadState
 */
void nvwal_impl_thread_state_stop(
  uint8_t* thread_state);


/**
 * Called by flusher/fsyncer thread to notify its end.
 * @see NvwalThreadState
 */
void nvwal_impl_thread_state_stopped(
  uint8_t* thread_state);

/** @} */

#endif  /* NVWAL_IMPL_INIT_H_ */
