/*******************************************************************************
 * thrill/net/manager.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MANAGER_HEADER
#define THRILL_NET_MANAGER_HEADER

#include <thrill/net/connection.hpp>
#include <thrill/net/tcp/group.hpp>

#include <array>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

class Construction;

} // namespace tcp

//! \addtogroup net Network Communication
//! \{

/**
 * \brief Initializes communication channels, manages communication channels and
 * handles errors.
 *
 * \details This class is responsible for initializing the three net::Groups for
 * the major network components, SystemControl, FlowControl and DataManagement,
 */
class Manager
{
    static const bool debug = false;

public:
    /*!
     * The count of net::Groups to initialize.
     * If this value is changed, the corresponding
     * getters for the net::Groups should be changed as well.
     */
    static const size_t kGroupCount = 3;

    using Group = tcp::Group;

    size_t my_host_rank() {
        return GetSystemGroup().my_host_rank();
    }

    size_t num_hosts() {
        return GetSystemGroup().num_hosts();
    }

    //! non-copyable: delete copy-constructor
    Manager(const Manager&) = delete;
    //! non-copyable: delete assignment operator
    Manager& operator = (const Manager&) = delete;

    /*!
     * \brief Initializes this Manager and initializes all Groups.
     * \details Initializes this Manager and initializes all Groups.
     * When this method returns, the network system is ready to use.
     *
     * \param my_rank_ The rank of the worker that owns this Manager.
     * \param endpoints The ordered list of all endpoints, including the local worker,
     * where the endpoint at position i corresponds to the worker with id i.
     */
    Manager(size_t my_rank_,
            const std::vector<std::string>& endpoints);

    /*!
     * Construct Manager from already initialized net::Groups.
     */
    Manager(size_t my_rank_,
            std::array<Group, kGroupCount>&& groups);

    //! Construct a mock network, consisting of node_count compute
    //! nodes. Delivers this number of net::Manager objects, which are
    //! internally connected.
    static std::vector<std::unique_ptr<Manager> >
    ConstructLocalMesh(size_t node_count);

    /**
     * \brief Returns the net group for the system control channel.
     */
    Group & GetSystemGroup() {
        return groups_[0];
    }

    /**
     * \brief Returns the net group for the flow control channel.
     */
    Group & GetFlowGroup() {
        return groups_[1];
    }

    /**
     * \brief Returns the net group for the data manager.
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
     * The rank associated with the local worker.
     */
    size_t my_rank_;

    /**
     * The Groups initialized and managed
     * by this Manager.
     */
    std::array<Group, kGroupCount> groups_;

    //! for initialization of members
    friend class tcp::Construction;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MANAGER_HEADER

/******************************************************************************/
