/*******************************************************************************
 * thrill/net/group.hpp
 *
 * net::Group is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_GROUP_HEADER
#define THRILL_NET_GROUP_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/net/connection.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

class GroupBase
{
public:
    // default constructor
    GroupBase() = default;

    //! non-copyable: delete copy-constructor
    GroupBase(const GroupBase&) = delete;
    //! non-copyable: delete assignment operator
    GroupBase& operator = (const GroupBase&) = delete;
    //! move-constructor: default
    GroupBase(GroupBase&&) = default;
    //! move-assignment operator: default
    GroupBase& operator = (GroupBase&&) = default;

    //! Return our rank among hosts in this group.
    size_t my_host_rank() const { return my_rank_; }

    //! Return number of connections in this group (= number computing hosts)
    virtual size_t num_hosts() const = 0;

    //! Return Connection to client id.
    virtual Connection & connection(size_t id) = 0;

    //! Close
    virtual void Close() = 0;

    //! our rank in the network group
    size_t my_rank_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
