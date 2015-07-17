/*******************************************************************************
 * c7a/data/channel_sink.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_SINK_HEADER
#define C7A_DATA_CHANNEL_SINK_HEADER

#include <c7a/net/buffer.hpp>
#include <c7a/data/stream_block_header.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/data/block.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace data {

//! SocketTarget is an BlockSink that sends data via a network socket to the
//! Channel object on a different worker.
template <size_t BlockSize>
class ChannelSink
{
public:
    using Block = data::Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;

    ChannelSink() { }

    ChannelSink(net::DispatcherThread* dispatcher,
                net::Connection* connection,
                size_t channel_id, size_t own_rank)
        : dispatcher_(dispatcher),
          connection_(connection),
          id_(channel_id),
          own_rank_(own_rank)
    { }

    ChannelSink(ChannelSink&&) = default;

    //! Appends data to the SocketTarget.
    //! Data may be sent but may be delayed.
    void Append(const BlockCPtr& block, size_t block_used,
                size_t nitems, size_t /* first */) {
        if (block_used == 0) return;

        SendHeader(block_used, nitems);

        sLOG1 << "sending block"
              << common::hexdump(block->begin(), block_used);

        // TODO(tb): this copies data!
        net::Buffer payload_buf(block->begin(), block_used);
        // TODO(tb): this does not work as expected: only one AsyncWrite can be
        // active on a fd at the same item, hence packets get lost!
        dispatcher_->AsyncWrite(*connection_, std::move(payload_buf));
    }

    // //! Sends bare data via the socket
    // //! \param data base address of the data
    // //! \param len of data to be sent in bytes
    // //! \param num_elements number of elements in the send-range
    // void Pipe(const void* data, size_t len, size_t num_elements) {
    //     if (len == 0) {
    //         return;
    //     }
    //     SendHeader(len, num_elements);
    //     //TODO(ts) this copies the data.
    //     net::Buffer payload_buf = net::Buffer(data, len);
    //     dispatcher_->AsyncWrite(*connection_, std::move(payload_buf));
    // }

    //! Closes the connection
    void Close() {
        assert(!closed_);
        closed_ = true;

        sLOG << "sending 'close channel' from worker" << own_rank_ << "on" << id_;
        SendHeader(0, 0);
    }

protected:
    static const bool debug = false;

    net::DispatcherThread* dispatcher_ = nullptr;
    net::Connection* connection_ = nullptr;

    size_t id_ = -1;
    size_t own_rank_ = -1;
    bool closed_ = false;

    void SendHeader(size_t num_bytes, size_t elements) {
        StreamBlockHeader header;
        header.channel_id = id_;
        header.expected_bytes = num_bytes;
        header.expected_elements = elements;
        header.sender_rank = own_rank_;
        net::Buffer header_buffer(&header, sizeof(header));
        dispatcher_->AsyncWrite(*connection_, std::move(header_buffer));
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_SINK_HEADER

/******************************************************************************/
