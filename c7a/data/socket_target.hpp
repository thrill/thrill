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

class SocketTarget
{
public:
    SocketTarget(net::NetDispatcher& dispatcher, net::NetConnection& connection, size_t channel_id)
        : dispatcher_(dispatcher),
          connection_(connection),
          id_(channel_id) { }

    void Append(net::BinaryBuffer buffer)
    {
        SendHeader(buffer.size());

        net::Buffer payload_buf = buffer.ToBuffer();
        dispatcher_.AsyncWrite(connection_, std::move(payload_buf));
    }

    void SendHeader(size_t num_bytes)
    {
        net::StreamBlockHeader header;
        header.channel_id = id_;
        header.expected_bytes = num_bytes;
        net::Buffer header_buffer(&header, sizeof(header));
        dispatcher_.AsyncWrite(connection_, std::move(header_buffer));
    }

    void Close()
    {
        SendHeader(0);
    }

private:
    net::NetDispatcher& dispatcher_;
    net::NetConnection& connection_;
    size_t id_;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SOCKET_TARGET_HEADER

/******************************************************************************/
