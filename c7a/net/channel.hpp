/*******************************************************************************
 * c7a/net/channel.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_CHANNEL_HEADER
#define C7A_NET_CHANNEL_HEADER

#include <c7a/net/net_connection.hpp>
#include <c7a/net/stream.hpp>
#include <c7a/data/binary_buffer.hpp>
#include <c7a/data/buffer_chain.hpp>

#include <vector>
#include <string>

namespace c7a {
namespace net {
//! \ingroup net
//! \{

//! A Channel is a collection of \ref Stream instances and bundles them to a
//! logical communication channel.
//!
//! There exists only one stream per socket at a time.
//! The channel keeps track of all active channels and counts the closed ones.
//!
//! As soon as the number of expected streams is reached, the channel is marked
//! as finished and no more data will arrive.
//!
//! Block headers are put into streams that poll more data from the socket.
//! As soon as the block is exhausted, the socket polling responsibility
//! is transfered back to the channel multiplexer.
//!
//! This class is the state machine for the callback-hell from NetDispatcher
class Channel {
public:
    //! Called to transfer the polling responsibility back to the channel multiplexer
    typedef std::function<void (NetConnection& s)> ReleaseSocketCallback;

    //! Creates a new channel instance
    Channel(NetDispatcher& dispatcher, ReleaseSocketCallback release_callback,
            int id, int expected_streams, std::shared_ptr<data::BufferChain> target)
        : dispatcher_(dispatcher),
          release_(release_callback),
          id_(id),
          expected_streams_(expected_streams),
          finished_streams_(0),
          target_(target) { }

    void CloseLoopback() {
        CloseStream();
    }

    //! Takes over the polling responsibility from the channel multiplexer
    //!
    //! This is the start state of the callback state machine.
    //! end-of-streams are handled directly
    //! all other block headers are parsed
    void PickupStream(NetConnection& s, struct StreamBlockHeader head) {
        Stream* stream = new Stream(s, head);
        if (stream->IsFinished()) {
            sLOG << "end of stream on" << stream->socket << "in channel" << id_;
            CloseStream();
        }
        else {
            sLOG << "pickup stream on" << stream->socket << "in channel" << id_;
            active_streams_++;
            ReadFromStream(stream);
        }
    }

    //! Indicates whether all streams are finished
    bool Finished() const {
        return finished_streams_ == expected_streams_;
    }

    int Id() {
        return id_;
    }

private:
    static const bool debug = true;
    NetDispatcher& dispatcher_;
    ReleaseSocketCallback release_;

    int id_;
    int active_streams_;
    int expected_streams_;
    int finished_streams_;

    std::shared_ptr<data::BufferChain> target_;

    //! Decides if there are more elements to read of a new stream block header
    //! is expected (transfers control back to multiplexer)
    void ReadFromStream(Stream* stream) {
        if (stream->bytes_read < stream->header.expected_bytes) {
            ExpectData(stream);
        }
        else {
            sLOG << "reached end of block on" << stream->socket << "in channel" << id_;
            active_streams_--;
            stream->ResetHead();
            release_(stream->socket);
            delete stream;
        }
    }

    void CloseStream() {
        finished_streams_++;
        if (finished_streams_ == expected_streams_) {
            sLOG << "channel" << id_ << " is closed";
            target_->Close();
        }
        else {
            sLOG << "channel" << id_ << " is not closed yet (expect:" << expected_streams_ << "actual:" << finished_streams_ << ")";
        }
    }

    //! Expect data to arrive at the socket.
    //! Size of element is known
    inline void ExpectData(Stream* stream) {
        auto exp_size = stream->header.expected_bytes - stream->bytes_read;
        auto callback = std::bind(&Channel::ConsumeData, this, std::placeholders::_1, std::placeholders::_2, stream);
        sLOG << "expect data with" << exp_size
             << "bytes on" << stream->socket << "in channel" << id_;
        dispatcher_.AsyncRead(stream->socket, exp_size, callback);
    }

    inline void ConsumeData(NetConnection& s, Buffer&& buffer, Stream* stream) {
        (void)s;
        sLOG << "read data on" << stream->socket << "in channel" << id_;
        stream->bytes_read += buffer.size();
        target_->Append(data::BinaryBuffer(buffer.data(), buffer.size()));
        ReadFromStream(stream);
    }
};
} // namespace net
} // namespace c7a

#endif // !C7A_NET_CHANNEL_HEADER

/******************************************************************************/
