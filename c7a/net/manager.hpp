/*******************************************************************************
 * c7a/net/manager.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_MANAGER_HEADER
#define C7A_NET_MANAGER_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/net/endpoint.hpp>
#include <c7a/net/group.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/**
 * @brief Initializes communication channels, manages communication channels and
 * handles errors.
 *
 * @details This class is responsible for initializing the three net::Groups for
 * the major network components, SystemControl, FlowControl and DataManagement,
 */
class Manager
{
    static const bool debug = false;

public:
    /**
     * The count of net::Groups to initialize.
     * If this value is changed, the corresponding
     * getters for the net::Groups should be changed as well.
     */
    static const size_t kGroupCount = 3;

    size_t my_rank() {
        return GetSystemGroup().my_connection_id();
    }

    size_t num_hosts() {
        return GetSystemGroup().num_connections();
    }

    //! default constructor
    Manager() { }

    //! non-copyable: delete copy-constructor
    Manager(const Manager&) = delete;
    //! non-copyable: delete assignment operator
    Manager& operator = (const Manager&) = delete;
    //! move-constructor
    Manager(Manager&&) = default;
    //! move-assignment
    Manager& operator = (Manager&&) = default;

    /**
     * @brief Initializes this Manager and initializes all Groups.
     * @details Initializes this Manager and initializes all Groups.
     * When this method returns, the network system is ready to use.
     *
     * @param my_rank_ The rank of the worker that owns this Manager.
     * @param endpoints The ordered list of all endpoints, including the local worker,
     * where the endpoint at position i corresponds to the worker with id i.
     */
    void Initialize(size_t my_rank_,
                    const std::vector<Endpoint>& endpoints);

    //! Construct a mock network, consisting of node_count compute
    //! nodes. Delivers this number of net::Manager objects, which are
    //! internally connected.
    static std::vector<Manager> ConstructLocalMesh(size_t node_count);

    /**
     * @brief Returns the net group for the system control channel.
     */
    Group & GetSystemGroup() {
        return groups_[0];
    }

    /**
     * @brief Returns the net group for the flow control channel.
     */
    Group & GetFlowGroup() {
        return groups_[1];
    }

    /**
     * @brief Returns the net group for the data manager.
     */
    Group & GetDataGroup() {
        return groups_[2];
    }

    void Close() {
        for (size_t i = 0; i < kGroupCount; i++) {
            groups_[i].Close();
        }
    }

private:
    /**
     * The Groups initialized and managed
     * by this Manager.
     */
    Group groups_[kGroupCount];

    /**
     * The rank associated with the local worker.
     */
    size_t my_rank_;

    //! for initialization of members
    friend class Construction;
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_MANAGER_HEADER

/******************************************************************************/
