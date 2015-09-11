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

/*!
 * A Connection represents a link to another peer in a network group. The link
 * need not be an actual stateful TCP connection, but may be reliable and
 * stateless.
 *
 * The Connection class is abstract, and subclasses must exist for every network
 * implementation.
 */
class Connection
{
public:
    //! flag which enables transmission of verification bytes for debugging,
    //! this increases network volume.
    static const bool self_verify_ = common::g_self_verify;

    //! Additional flags for sending or receiving.
    enum Flags : unsigned {
        NoFlags = 0,
        //! indicate that more data is coming, hence, sending a packet may be
        //! delayed. currently only applies to TCP.
        MsgMore = 1
    };

    //! operator to combine flags
    friend inline Flags operator | (const Flags& a, const Flags& b) {
        return static_cast<Flags>(
            static_cast<unsigned>(a) | static_cast<unsigned>(b));
    }

    //! \name Base Status Functions
    //! \{

    //! check whether the connection is (still) valid.
    virtual bool IsValid() const = 0;

    //! return a string representation of this connection, for user output.
    virtual std::string ToString() const = 0;

    //! virtual method to output to a std::ostream
    virtual std::ostream & output_ostream(std::ostream& os) const = 0;

    //! \}

    //! \name Send Functions
    //! \{

    //! Synchronous blocking send of the (data,size) packet. if sending fails, a
    //! net::Exception is thrown.
    virtual void SyncSend(const void* data, size_t size,
                          Flags flags = NoFlags) = 0;

    //! Non-blocking send of a (data,size) message. returns number of bytes
    //! possible to send. check errno for errors.
    virtual ssize_t SendOne(const void* data, size_t size,
                            Flags flags = NoFlags) = 0;

    //! Send any serializable item T. if sending fails, a net::Exception is
    //! thrown.
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

    //! make ostreamable
    friend std::ostream& operator << (std::ostream& os, const Connection& c) {
        return c.output_ostream(os);
    }
};

// \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_CONNECTION_HEADER

/******************************************************************************/
