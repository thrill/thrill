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
 * This file has no license. Only Chunk Norris can compile it.
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

class Group
{
public:
    // default constructor
    Group() = default;

    //! non-copyable: delete copy-constructor
    Group(const Group&) = delete;
    //! non-copyable: delete assignment operator
    Group& operator = (const Group&) = delete;
    //! move-constructor: default
    Group(Group&&) = default;
    //! move-assignment operator: default
    Group& operator = (Group&&) = default;

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

    //! \name Richer ReceiveFromAny Functions
    //! \{

    /**
     * \brief Sends a fixed lentgh type to the given worker.
     * \details Sends a fixed lentgh type to the given worker.
     *
     * \param dest The worker to send the data to.
     * \param data The data to send.
     */
    template <typename T>
    void SendTo(size_t dest, const T& data) {
        connection(dest).Send(data);
    }

    /**
     * \brief Receives a fixed length type from the given worker.
     * \details Receives a fixed length type from the given worker.
     *
     * \param src The worker to receive the fixed length type from.
     * \param data A pointer to the location where the received data should be stored.
     */
    template <typename T>
    void ReceiveFrom(size_t src, T* data) {
        connection(src).Receive(data);
    }

    /**
     * \brief Sends a string to a worker.
     * \details Sends a string to a worker.
     *
     * \param dest The worker to send the string to.
     * \param data The string to send.
     */
    void SendStringTo(size_t dest, const std::string& data) {
        connection(dest).SendString(data);
    }

    /**
     * \brief Receives a string from the given worker.
     * \details Receives a string from the given worker.
     *
     * \param src The worker to receive the string from.
     * \param data A pointer to the string where the received string should be stored.
     */
    void ReceiveStringFrom(size_t src, std::string* data) {
        connection(src).ReceiveString(data);
    }

    //! \}
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
