/*******************************************************************************
 * c7a/net/system_control_channel.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/system_control_channel.hpp>

namespace c7a {
namespace net {

void MasterSystemControlChannel::setCallback(MasterControlCallback callback)
{ }

void MasterSystemControlChannel::rollBackStage()
{ }

void MasterSystemControlChannel::abortExecution()
{ }

void WorkerSystemControlChannel::setCallback(WorkerControlCallback callback)
{ }

void WorkerSystemControlChannel::requestBackupLocation()
{ }

void WorkerSystemControlChannel::notifyBackupComplete()
{ }

} // namespace net
} // namespace c7a

/******************************************************************************/
