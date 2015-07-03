/*******************************************************************************
 * c7a/net/flow_control_manager.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_FLOW_CONTROL_MANAGER_HEADER
#define C7A_NET_FLOW_CONTROL_MANAGER_HEADER

#include <string>
#include <vector>
#include <c7a/net/group.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/common/cyclic_barrier.hpp>

namespace c7a {
namespace net {

class FlowControlChannelManager
{
protected:
    /**
     * The shared barrier used to synchronize between worker threads on this node.
     */
    common::Barrier barrier;

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
     * @brief Initializes a certain count of flow control channels.
     *
     * @param group The net group to use for initialization.
     * @param threadCount The count of threads to spawn flow channels for.
     *
     */
    explicit FlowControlChannelManager(net::Group& group, int threadCount) : barrier(threadCount), shmem(NULL) {

        for (int i = 0; i < threadCount; i++) {
            channels.emplace_back(group, i, threadCount, barrier, &shmem);
        }
    }

    /**
     * @brief Gets all flow control channels for all threads.
     * @return A flow channel for each thread.
     */
    std::vector<FlowControlChannel> & GetFlowControlChannels() {
        return channels;
    }

    /**
     * @brief Gets the flow control channel for a certain thread.
     * @return The flow control channel for a certain thread.
     */
    FlowControlChannel & GetFlowControlChannel(int threadId) {
        return channels[threadId];
    }
};

} // namespace net
} // namespace c7a

#endif //! C7A_NET_FLOW_CONTROL_MANAGER_HEADER

/******************************************************************************/
