/*******************************************************************************
 * thrill/net/connection.hpp
 *
 * Contains net::Connection, a richer set of network point-to-point primitives.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_CONNECTION_HEADER
#define THRILL_NET_CONNECTION_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/data/serialization.hpp>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/net/buffer_reader.hpp>
#include <thrill/net/exception.hpp>
#include <thrill/net/fixed_buffer_builder.hpp>

#include <array>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net_layer
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
    static constexpr bool self_verify_ = common::g_self_verify;

    //! typeid().hash_code() is only guaranteed to be equal for the same program
    //! run, hence, we can only use it on loopback networks.
    bool is_loopback_ = false;

    //! Additional flags for sending or receiving.
    enum Flags : size_t {
        NoFlags = 0,
        //! indicate that more data is coming, hence, sending a packet may be
        //! delayed. currently only applies to TCP.
        MsgMore = 1
    };

    //! operator to combine flags
    friend inline Flags operator | (const Flags& a, const Flags& b) {
        return static_cast<Flags>(
            static_cast<size_t>(a) | static_cast<size_t>(b));
    }

    //! \name Base Status Functions
    //! \{

    //! check whether the connection is (still) valid.
    virtual bool IsValid() const = 0;

    //! return a string representation of this connection, for user output.
    virtual std::string ToString() const = 0;

    //! virtual method to output to a std::ostream
    virtual std::ostream& OutputOstream(std::ostream& os) const = 0;

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

    //! Send any serializable POD item T. if sending fails, a net::Exception is
    //! thrown.
    template <typename T>
    typename std::enable_if<std::is_pod<T>::value, void>::type
    Send(const T& value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, send hash_code.
            size_t hash_code = typeid(T).hash_code();
            SyncSend(&hash_code, sizeof(hash_code));
        }
        // send PODs directly from memory.
        SyncSend(&value, sizeof(value));
    }

    //! Send any serializable non-POD fixed-length item T. if sending fails, a
    //! net::Exception is thrown.
    template <typename T>
    typename std::enable_if<
        !std::is_pod<T>::value&&
        data::Serialization<BufferBuilder, T>::is_fixed_size, void>::type
    Send(const T& value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, send hash_code.
            size_t hash_code = typeid(T).hash_code();
            SyncSend(&hash_code, sizeof(hash_code));
        }
        // fixed_size items can be sent without size header
        static constexpr size_t fixed_size
            = data::Serialization<BufferBuilder, T>::fixed_size;
        if (fixed_size < 2 * 1024 * 1024) {
            // allocate buffer on stack (no allocation)
            using FixedBuilder = FixedBufferBuilder<fixed_size>;
            FixedBuilder fb;
            data::Serialization<FixedBuilder, T>::Serialize(value, fb);
            assert(fb.size() == fixed_size);
            SyncSend(fb.data(), fb.size());
        }
        else {
            // too big, use heap allocation
            BufferBuilder bb;
            data::Serialization<BufferBuilder, T>::Serialize(value, bb);
            SyncSend(bb.data(), bb.size());
        }
    }

    //! Send any serializable non-POD variable-length item T. if sending fails,
    //! a net::Exception is thrown.
    template <typename T>
    typename std::enable_if<
        !std::is_pod<T>::value&&
        !data::Serialization<BufferBuilder, T>::is_fixed_size, void>::type
    Send(const T& value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, send hash_code.
            size_t hash_code = typeid(T).hash_code();
            SyncSend(&hash_code, sizeof(hash_code));
        }
        // variable length items must be prefixed with size header
        BufferBuilder bb;
        data::Serialization<BufferBuilder, T>::Serialize(value, bb);
        size_t size = bb.size();
        SyncSend(&size, sizeof(size), MsgMore);
        SyncSend(bb.data(), bb.size());
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

    //! Receive any serializable POD item T.
    template <typename T>
    typename std::enable_if<std::is_pod<T>::value, void>::type
    Receive(T* out_value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, receive hash_code.
            size_t hash_code;
            SyncRecv(&hash_code, sizeof(hash_code));
            if (hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::Receive() attempted to receive item "
                          "with different typeid!");
            }
        }
        // receive PODs directly into memory.
        SyncRecv(out_value, sizeof(*out_value));
    }

    //! Receive any serializable non-POD fixed-length item T.
    template <typename T>
    typename std::enable_if<
        !std::is_pod<T>::value&&
        data::Serialization<BufferBuilder, T>::is_fixed_size, void>::type
    Receive(T* out_value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, receive hash_code.
            size_t hash_code;
            SyncRecv(&hash_code, sizeof(hash_code));
            if (hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::Receive() attempted to receive item "
                          "with different typeid!");
            }
        }
        // fixed_size items can be received without size header
        static constexpr size_t fixed_size
            = data::Serialization<BufferBuilder, T>::fixed_size;
        if (fixed_size < 2 * 1024 * 1024) {
            // allocate buffer on stack (no allocation)
            std::array<uint8_t, fixed_size> b;
            SyncRecv(b.data(), b.size());
            BufferReader br(b.data(), b.size());
            *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
        }
        else {
            // too big, use heap allocation
            Buffer b(data::Serialization<BufferBuilder, T>::fixed_size);
            SyncRecv(b.data(), b.size());
            BufferReader br(b);
            *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
        }
    }

    //! Receive any serializable non-POD fixed-length item T.
    template <typename T>
    typename std::enable_if<
        !std::is_pod<T>::value&&
        !data::Serialization<BufferBuilder, T>::is_fixed_size, void>::type
    Receive(T* out_value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, receive hash_code.
            size_t hash_code;
            SyncRecv(&hash_code, sizeof(hash_code));
            if (hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::Receive() attempted to receive item "
                          "with different typeid!");
            }
        }
        // variable length items are prefixed with size header
        size_t size;
        SyncRecv(&size, sizeof(size));
        // receives message
        Buffer b(size);
        SyncRecv(b.data(), size);
        BufferReader br(b);
        *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
    }

    //! \}

    //! \name Paired SendReceive Methods
    //! \{

    //! Synchronous blocking sending and receive a message of given size. The
    //! size must match the SyncSendRecv size for some network layers may only
    //! support matching messages (read: RDMA!, but also true for the mock
    //! net). Throws a net::Exception on errors.
    virtual void SyncSendRecv(const void* send_data, size_t send_size,
                              void* recv_data, size_t recv_size) = 0;

    //! SendReceive any serializable POD item T.
    template <typename T>
    typename std::enable_if<std::is_pod<T>::value, void>::type
    SendReceive(const T& value, T* out_value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, send/receive hash_code.
            size_t send_hash_code = typeid(T).hash_code(), recv_hash_code;
            SyncSendRecv(&send_hash_code, sizeof(send_hash_code),
                         &recv_hash_code, sizeof(recv_hash_code));
            if (recv_hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::SendReceive() attempted to receive item "
                          "with different typeid!");
            }
        }
        // receive PODs directly into memory.
        SyncSendRecv(&value, sizeof(value), out_value, sizeof(*out_value));
    }

    //! SendReceive any serializable non-POD fixed-length item T.
    template <typename T>
    typename std::enable_if<
        !std::is_pod<T>::value&&
        data::Serialization<BufferBuilder, T>::is_fixed_size, void>::type
    SendReceive(const T& value, T* out_value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, send/receive hash_code.
            size_t send_hash_code = typeid(T).hash_code(), recv_hash_code;
            SyncSendRecv(&send_hash_code, sizeof(send_hash_code),
                         &recv_hash_code, sizeof(recv_hash_code));
            if (recv_hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::SendReceive() attempted to receive item "
                          "with different typeid!");
            }
        }

        // fixed_size items can be sent/recv without size header
        static constexpr size_t fixed_size
            = data::Serialization<BufferBuilder, T>::fixed_size;
        if (fixed_size < 2 * 1024 * 1024) {
            // allocate buffer on stack (no allocation)
            using FixedBuilder = FixedBufferBuilder<fixed_size>;
            FixedBuilder sendb;
            data::Serialization<FixedBuilder, T>::Serialize(value, sendb);
            assert(sendb.size() == fixed_size);
            std::array<uint8_t, fixed_size> recvb;
            SyncSendRecv(sendb.data(), sendb.size(),
                         recvb.data(), recvb.size());
            BufferReader br(recvb.data(), recvb.size());
            *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
        }
        else {
            // too big, use heap allocation
            BufferBuilder sendb;
            data::Serialization<BufferBuilder, T>::Serialize(value, sendb);
            Buffer recvb(data::Serialization<BufferBuilder, T>::fixed_size);
            SyncSendRecv(sendb.data(), sendb.size(),
                         recvb.data(), recvb.size());
            BufferReader br(recvb);
            *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
        }
    }

    //! SendReceive any serializable non-POD fixed-length item T.
    template <typename T>
    typename std::enable_if<
        !std::is_pod<T>::value&&
        !data::Serialization<BufferBuilder, T>::is_fixed_size, void>::type
    SendReceive(const T& value, T* out_value) {
        if (self_verify_ && is_loopback_) {
            // for communication verification, send/receive hash_code.
            size_t send_hash_code = typeid(T).hash_code(), recv_hash_code;
            SyncSendRecv(&send_hash_code, sizeof(send_hash_code),
                         &recv_hash_code, sizeof(recv_hash_code));
            if (recv_hash_code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "Connection::SendReceive() attempted to receive item "
                          "with different typeid!");
            }
        }
        // variable length items must be prefixed with size header
        BufferBuilder sendb;
        data::Serialization<BufferBuilder, T>::Serialize(value, sendb);
        size_t send_size = sendb.size(), recv_size;
        SyncSendRecv(&send_size, sizeof(send_size),
                     &recv_size, sizeof(recv_size));
        // receives message
        Buffer recvb(recv_size);
        SyncSendRecv(sendb.data(), sendb.size(),
                     recvb.data(), recv_size);
        BufferReader br(recvb);
        *out_value = data::Serialization<BufferReader, T>::Deserialize(br);
    }

    //! \}

    //! \name Statistics
    //! {

    //! sent bytes
    std::atomic<size_t> tx_bytes_ { 0 };

    //! received bytes
    std::atomic<size_t> rx_bytes_ = { 0 };

    //! previous read of sent bytes
    size_t prev_tx_bytes_ = 0;

    //! previous read of received bytes
    size_t prev_rx_bytes_ = 0;

    //! }

    //! make ostreamable
    friend std::ostream& operator << (std::ostream& os, const Connection& c) {
        return c.OutputOstream(os);
    }
};

// \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_CONNECTION_HEADER

/******************************************************************************/
