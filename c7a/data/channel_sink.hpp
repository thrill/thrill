/*******************************************************************************
 * c7a/data/channel_sink.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_SINK_HEADER
#define C7A_DATA_CHANNEL_SINK_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/data/block.hpp>
#include <c7a/data/block_sink.hpp>
#include <c7a/data/stream_block_header.hpp>
#include <c7a/net/buffer.hpp>
#include <c7a/net/dispatcher_thread.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * ChannelSink is an BlockSink that sends data via a network socket to the
 * Channel object on a different worker.
 */
class ChannelSink : public BlockSink
{
public:
    using ChannelId = size_t;

    //! Construct invalid ChannelSink, needed for placeholders in sinks arrays
    //! where Blocks are directly sent to local workers.
    ChannelSink() : closed_(true) { }

    /*! ChannelSink sending out to network.
     * \param dispatcher used for sending data via a socket
     * \param connection the socket (aka conneciton) used for the channel
     * \param channel_id the ID that identifies the channel
     * \param my_rank the ID that identifies this computing node globally
     * \param my_worker_id the id that identifies the worker locally
     * \param partners_worker_id the id that identifies the partner worker locally
     */
    ChannelSink(net::DispatcherThread* dispatcher,
                net::Connection* connection,
                ChannelId channel_id, size_t my_rank, size_t my_worker_id, size_t partners_worker_id)
        : dispatcher_(dispatcher),
          connection_(connection),
          id_(channel_id),
          my_rank_(my_rank),
          my_worker_id_(my_worker_id),
          partners_worker_id_(partners_worker_id)
    { }

    ChannelSink(ChannelSink&&) = default;

    //! Appends data to the ChannelSink.  Data may be sent but may be delayed.
    void AppendBlock(const VirtualBlock& vb) override {
        if (vb.size() == 0) return;

        sLOG << "ChannelSink::AppendBlock" << vb;

        StreamBlockHeader header;
        header.channel_id = id_;
        header.size = vb.size();
        header.first_item = vb.first_item_relative();
        header.nitems = vb.nitems();
        header.sender_rank = my_rank_;
        header.sender_worker_id = my_worker_id_;
        header.receiver_worker_id = partners_worker_id_;

        if (debug) {
            sLOG << "sending block" << common::hexdump(vb.ToString());
        }

        dispatcher_->AsyncWrite(
            *connection_,
            // send out Buffer and VirtualBlock, guaranteed to be successive
            header.Serialize(), vb);
    }

    //! Closes the connection
    void Close() override {
        assert(!closed_);
        closed_ = true;

        sLOG << "sending 'close channel' from worker" << my_worker_id_
             << "to worker" << partners_worker_id_
             << "channel" << id_;

        StreamBlockHeader header;
        header.channel_id = id_;
        header.size = 0;
        header.first_item = 0;
        header.nitems = 0;
        header.sender_rank = my_rank_;
        header.sender_worker_id = my_worker_id_;
        header.receiver_worker_id = partners_worker_id_;
        dispatcher_->AsyncWrite(*connection_, header.Serialize());
    }

    //! return close flag
    bool closed() const { return closed_; }

protected:
    static const bool debug = false;

    net::DispatcherThread* dispatcher_ = nullptr;
    net::Connection* connection_ = nullptr;

    size_t id_ = -1;
    size_t my_rank_ = -1;
    size_t my_worker_id_ = -1;
    size_t partners_worker_id_ = -1;
    bool closed_ = false;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_SINK_HEADER

/******************************************************************************/
