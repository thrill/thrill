/*******************************************************************************
 * thrill/net/flow_control_manager.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_FLOW_CONTROL_MANAGER_HEADER
#define THRILL_NET_FLOW_CONTROL_MANAGER_HEADER

#include <thrill/common/thread_barrier.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/group.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

class FlowControlChannelManager
{
protected:
    /**
     * The shared barrier used to synchronize between worker threads on this node.
     */
    common::ThreadBarrier barrier;

    /**
     * The flow control channels associated with this node.
     */
    std::vector<FlowControlChannel> channels;

    /**
     * Some shared memory to work upon (managed by thread 0).
     */
    void* shmem;

public:
    /**
     * \brief Initializes a certain count of flow control channels.
     *
     * \param group The net group to use for initialization.
     * \param local_worker_count The count of threads to spawn flow channels for.
     *
     */
    explicit FlowControlChannelManager(net::Group& group, int local_worker_count)
        : barrier(local_worker_count), shmem(nullptr) {

        for (int i = 0; i < local_worker_count; i++) {
            channels.emplace_back(group, i, local_worker_count, barrier, &shmem);
        }
    }

    /**
     * \brief Gets all flow control channels for all threads.
     * \return A flow channel for each thread.
     */
    std::vector<FlowControlChannel> & GetFlowControlChannels() {
        return channels;
    }

    /**
     * \brief Gets the flow control channel for a certain thread.
     * \return The flow control channel for a certain thread.
     */
    FlowControlChannel & GetFlowControlChannel(int threadId) {
        return channels[threadId];
    }
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_FLOW_CONTROL_MANAGER_HEADER

/******************************************************************************/
