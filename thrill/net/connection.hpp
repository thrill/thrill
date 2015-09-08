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
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_CONNECTION_HEADER
#define THRILL_NET_CONNECTION_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/serialization.hpp>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/net/buffer_reader.hpp>
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

    //! Synchronous blocking send of the (data,size) packet. if sending fails, a
    //! net::Exception is thrown.
    virtual void SyncSend(const void* data, size_t size, int flags = 0) = 0;

    //! Non-blocking send of a (data,size) message. returns number of bytes
    //! possible to send. check errno for errors.
    virtual ssize_t SendOne(const void* data, size_t size) = 0;

    //! Send any serializable item T.
    template <typename T>
    void Send(const T& value) {
        if (self_verify_) {
            // for communication verification, send hash_code.
            size_t hash_code = typeid(T).hash_code();
            SyncSend(&hash_code, sizeof(hash_code));
        }
        if (std::is_pod<T>::value) {
            // send PODs directly from memory.
            SyncSend(&value, sizeof(value));
        }
        else if (data::Serialization<BufferBuilder, T>::is_fixed_size) {
            // fixed_size items can be sent without size header
            // TODO(tb): make bb allocate on stack.
            BufferBuilder bb;
            data::Serialization<BufferBuilder, T>::Serialize(value, bb);
            SyncSend(bb.data(), bb.size());
        }
        else {
            // variable length items must be prefixed with size header
            BufferBuilder bb;
            data::Serialization<BufferBuilder, T>::Serialize(value, bb);
            size_t size = bb.size();
            SyncSend(&size, sizeof(size), MsgMore);
            SyncSend(bb.data(), bb.size());
        }
    }

    //! \}

    //! \name Receive Functions
    //! \{

    //! Synchronous blocking receive a message of given size. The size must
    //! match the SyncSend size for some network layers may only support
    //! matching messages (read: RDMA!, but also true for the mock net). Throws
    //! a net::Exception on errors.
    virtual void SyncRecv(void* out_data, size_t size) = 0;

    //! Non-blocking receive of at most size data. returns number of bytes
    //! actually received. check errno for errors.
    virtual ssize_t RecvOne(void* out_data, size_t size) = 0;

    //! Receive any serializable item T.
    template <typename T>
    void Receive(T* out_value) {
        if (self_verify_) {
            // for communication verification, receive hash_code.
            size_t hash_code;
            SyncRecv(&hash_code, sizeof(hash_code));
            if (hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::Receive() attempted to receive item "
                          "with different typeid!");
            }
        }
        if (std::is_pod<T>::value) {
            // receive PODs directly into memory.
            SyncRecv(out_value, sizeof(*out_value));
        }
        else if (data::Serialization<BufferBuilder, T>::is_fixed_size) {
            // fixed_size items can be received without size header
            // TODO(tb): make bb allocate on stack.
            Buffer b(data::Serialization<BufferBuilder, T>::fixed_size);
            SyncRecv(b.data(), b.size());
            BufferReader br(b);
            *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
        }
        else {
            // variable length items are prefixed with size header
            size_t size;
            SyncRecv(&size, sizeof(size));
            // receives message
            Buffer b(size);
            SyncRecv(b.data(), size);
            BufferReader br(b);
            *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
        }
    }

    //! \}
};

// \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_CONNECTION_HEADER

/******************************************************************************/
