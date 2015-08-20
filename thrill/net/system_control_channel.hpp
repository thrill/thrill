/*******************************************************************************
 * thrill/net/system_control_channel.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_SYSTEM_CONTROL_CHANNEL_HEADER
#define C7A_NET_SYSTEM_CONTROL_CHANNEL_HEADER

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/**
 * \brief Provides a non-blocking collection for communication.
 * \details This should be used for system control.
 *
 */
class SystemControlChannel
{ };

/**
 * \brief Callback to be implemented by the master controller.
 */
class MasterControlCallback
{
public:
    /**
     * \brief Called when a worker fails.
     */
    virtual void onWorkerFailure();
    /**
     * \brief Called when a backup is requested by a worker.
     */
    virtual void onBackupRequested();
    /**
     * \brief Called when a backup was done by a worker.
     */
    virtual void onBackupDone();
};

/**
 * \brief Provides a control channel for the master.
 * \details This channel is ansyncronous.
 */
class MasterSystemControlChannel
{
public:
    /**
     * \brief Sets the callback for events.
     * \details The callback is called asynchronously by the network thread.
     *
     * \param callback The callback to set.
     */
    void setCallback(MasterControlCallback callback);

    /**
     * \brief Sends a rollback stage message to all workers.
     * \details This call is async.
     */
    void rollBackStage();

    /**
     * \brief Sends an abort execution message to all workers.
     * \details This call is async.
     */
    void abortExecution();
};

/**
 * \brief Callback to be implemented by the worker controller.
 */
class WorkerControlCallback
{
public:
    /**
     * \brief Called when a rollback should be done.
     */
    virtual void onRollback();

    /**
     * \brief Called when the computation should be aborted.
     */
    virtual void onAbort();
};

/**
 * \brief Provides a control channel for the worker.
 * \details This channel is asynchronous.
 */
class WorkerSystemControlChannel
{
public:
    /**
     * \brief Sets the callback that is called in case of an event.
     *
     * \param callback The callback to call.
     * \details The callback is called asynchronously by the network thread.
     */
    void setCallback(WorkerControlCallback callback);

    /**
     * \brief Requests a backup location from the master.
     * \details This call is async.
     */
    void requestBackupLocation();

    /**
     * \brief Notifies the master about a completed backup.
     * \details This call is async.
     */
    void notifyBackupComplete();
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_SYSTEM_CONTROL_CHANNEL_HEADER

/******************************************************************************/
