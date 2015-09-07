/*******************************************************************************
 * thrill/net/connection.hpp
 *
 * Contains net::Connection, a richer set of network point-to-point primitives.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_CONNECTION_HEADER
#define THRILL_NET_CONNECTION_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/net/exception.hpp>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

// Because Mac OSX does not know MSG_MORE.
// #ifndef MSG_MORE
// #define MSG_MORE 0
// #endif

class Connection
{
public:
    static const bool self_verify_ = common::g_self_verify;

    virtual bool IsValid() const = 0;

    virtual std::string ToString() const = 0;

    // TODO(tb): fixup later: add MSG_MORE flag to send.
    static const int MsgMore = 0;

    //! \name Send Functions
    //! \{

    //! Send a message of given size.
    virtual ssize_t SyncSend(const void* data, size_t size, int flags = 0) = 0;

    //! Send a partial message of given size.
    virtual ssize_t SendOne(const void* data, size_t size) = 0;

    //! Send a fixed-length type T (possibly without length header).
    template <typename T>
    void Send(const T& value) {
        static_assert(std::is_pod<T>::value,
                      "You only want to send POD types as raw values.");

        if (self_verify_) {
            // for communication verification, send sizeof.
            size_t len = sizeof(value);
            if (SyncSend(&len, sizeof(len), MsgMore) != sizeof(len))
                throw Exception("Error during Send", errno);
        }

        if (SyncSend(&value, sizeof(value))
            != static_cast<ssize_t>(sizeof(value)))
            throw Exception("Error during Send", errno);
    }

    //! Send a string buffer.
    void SendString(const void* data, size_t len) {
        if (SyncSend(&len, sizeof(len), MsgMore) != sizeof(len))
            throw Exception("Error during SendString", errno);

        if (SyncSend(data, len) != static_cast<ssize_t>(len))
            throw Exception("Error during SendString", errno);
    }

    //! Send a string message.
    void SendString(const std::string& message) {
        SendString(message.data(), message.size());
    }

    //! \}

    //! \name Receive Functions
    //! \{

    //! Receive a message of given size. The size must match the SyncSend size.
    virtual ssize_t SyncRecv(void* out_data, size_t size) = 0;

    //! Receive a partial message of given size. The size must match.
    virtual ssize_t RecvOne(void* out_data, size_t size) = 0;

    //! Receive a fixed-length type, possibly without length header.
    template <typename T>
    void Receive(T* out_value) {
        static_assert(std::is_pod<T>::value,
                      "You only want to receive POD types as raw values.");

        if (self_verify_) {
            // for communication verification, receive sizeof.
            size_t len = 0;
            if (SyncRecv(&len, sizeof(len)) != sizeof(len))
                throw Exception("Error during Receive", errno);

            // if this fails, then fixed-length type communication desynced.
            die_unequal(len, sizeof(*out_value));
        }

        if (SyncRecv(out_value, sizeof(*out_value))
            != static_cast<ssize_t>(sizeof(*out_value)))
            throw Exception("Error during Receive", errno);
    }

    //! Blocking receive string message from the connected socket.
    void ReceiveString(std::string* outdata) {
        size_t len = 0;

        if (SyncRecv(&len, sizeof(len)) != sizeof(len))
            throw Exception("Error during ReceiveString", errno);

        if (len == 0)
            return;

        outdata->resize(len);

        ssize_t ret = SyncRecv(const_cast<char*>(outdata->data()), len);

        if (ret != static_cast<ssize_t>(len))
            throw Exception("Error during ReceiveString", errno);
    }

    //! \}
};

// \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_CONNECTION_HEADER

/******************************************************************************/
