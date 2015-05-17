/*******************************************************************************
 * c7a/data/socket_target.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SOCKET_TARGET_HEADER
#define C7A_DATA_SOCKET_TARGET_HEADER

namespace c7a {
namespace data {
#include <c7a/net/buffer.hpp>
#include <c7a/net/stream.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/emitter_target.hpp>

class SocketTarget : public EmitterTarget {
public:
    SocketTarget(net::NetDispatcher* dispatcher, net::NetConnection* connection, size_t channel_id)
        : dispatcher_(dispatcher),
          connection_(connection),
          id_(channel_id) { }

    virtual void Append(BinaryBuffer buffer) { //virtual does not hurt because not in tight loop
        sLOG << "socket target appends";
        SendHeader(buffer.size());

        net::Buffer payload_buf = buffer.ToBuffer();
        dispatcher_->AsyncWrite(*connection_, std::move(payload_buf));
    }

    void SendHeader(size_t num_bytes) {
        net::StreamBlockHeader header;
        header.channel_id = id_;
        header.expected_bytes = num_bytes;
        net::Buffer header_buffer(&header, sizeof(header));
        dispatcher_->AsyncWrite(*connection_, std::move(header_buffer));
    }

    virtual void Close() {
        sLOG << "socket target closes";
        SendHeader(0);
    }

private:
    static const bool debug = true;
    //need pointers because child class does not initialize them.
    //TODO(ts) do not use raw pointers
    net::NetDispatcher* dispatcher_;
    net::NetConnection* connection_;
    size_t id_;
};

class LoopbackTarget : public EmitterTarget {
public:
    LoopbackTarget(std::shared_ptr<BufferChain> chain)
        : chain_(chain) { }

    virtual void Append(BinaryBuffer buffer) { //virtual does not hurt because not in tight loop
        sLOG << "loopback appends buffer";
        chain_->Append(buffer);
    }

    virtual void Close() {
        sLOG << "loopback closes";
        //We do not close the chain because the other channels are pushing to it.
        //TODO(ts) special case: if worker is alone and the loopback is the only channel, there is no one closing it.
    }

private:
    std::shared_ptr<BufferChain> chain_;
    static const bool debug = true;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SOCKET_TARGET_HEADER

/******************************************************************************/
