/*******************************************************************************
 * thrill/net/manager.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MANAGER_HEADER
#define THRILL_NET_MANAGER_HEADER

#include <thrill/net/connection.hpp>
#include <thrill/net/group.hpp>

#include <array>
#include <string>
#include <vector>

namespace thrill {
namespace net {

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
     * Construct Manager from already initialized net::Groups.
     */
    explicit Manager(std::array<GroupPtr, kGroupCount>&& groups) noexcept
        : groups_(std::move(groups)) { }

    /*!
      * Construct Manager from already initialized net::Groups.
      */
    explicit Manager(std::vector<GroupPtr>&& groups) {
        assert(groups.size() == kGroupCount);
        std::move(groups.begin(), groups.end(), groups_.begin());
    }

    /**
     * \brief Returns the net group for the system control channel.
     */
    Group & GetSystemGroup() {
        return *groups_[0];
    }

    /**
     * \brief Returns the net group for the flow control channel.
     */
    Group & GetFlowGroup() {
        return *groups_[1];
    }

    /**
     * \brief Returns the net group for the data manager.
     */
    Group & GetDataGroup() {
        return *groups_[2];
    }

    void Close() {
        for (size_t i = 0; i < kGroupCount; i++) {
            groups_[i]->Close();
        }
    }

private:
    /**
     * The Groups initialized and managed by this Manager.
     */
    std::array<GroupPtr, kGroupCount> groups_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MANAGER_HEADER

/******************************************************************************/
