/*******************************************************************************
 * thrill/net/system_control_channel.cpp
 *
 * Part of Project Thrill.
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/system_control_channel.hpp>

namespace thrill {
namespace net {

void MasterSystemControlChannel::setCallback(MasterControlCallback /* callback */)
{ }

void MasterSystemControlChannel::rollBackStage()
{ }

void MasterSystemControlChannel::abortExecution()
{ }

void WorkerSystemControlChannel::setCallback(WorkerControlCallback /* callback */)
{ }

void WorkerSystemControlChannel::requestBackupLocation()
{ }

void WorkerSystemControlChannel::notifyBackupComplete()
{ }

} // namespace net
} // namespace thrill

/******************************************************************************/
