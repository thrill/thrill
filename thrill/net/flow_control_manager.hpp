/*******************************************************************************
 * thrill/net/flow_control_manager.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_FLOW_CONTROL_MANAGER_HEADER
#define THRILL_NET_FLOW_CONTROL_MANAGER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/thread_barrier.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/group.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net_layer
//! \{

class FlowControlChannelManager
{
private:
    //! The shared barrier used to synchronize between worker threads on this
    //! node.
    common::ThreadBarrier barrier_;

    //! The flow control channels associated with this node.
    std::vector<FlowControlChannel> channels_;

    //! Thread local data structure: aligned such that no cache line is shared.
    using LocalData = FlowControlChannel::LocalData;

    //! Array of thread local data, one for each thread.
    std::vector<LocalData> shmem_;

    //! Host-global generation counter
    std::atomic<size_t> generation_ { 0 };

public:
    /*!
     * Initializes a certain count of flow control channels.
     *
     * \param group The net group to use for initialization.
     * \param local_worker_count The count of threads to spawn flow channels for.
     */
    FlowControlChannelManager(Group& group, size_t local_worker_count)
        : barrier_(local_worker_count),
          shmem_(local_worker_count) {
        assert(shmem_.size() == local_worker_count);
        for (size_t i = 0; i < local_worker_count; i++) {
            channels_.emplace_back(group, i, local_worker_count,
                                   barrier_, shmem_.data(), generation_);
        }
    }

    /*!
     * \brief Gets all flow control channels for all threads.
     * \return A flow channel for each thread.
     */
    std::vector<FlowControlChannel>& GetFlowControlChannels() {
        return channels_;
    }

    /*!
     * \brief Gets the flow control channel for a certain thread.
     * \return The flow control channel for a certain thread.
     */
    FlowControlChannel& GetFlowControlChannel(size_t thread_id) {
        return channels_[thread_id];
    }
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_FLOW_CONTROL_MANAGER_HEADER

/******************************************************************************/
