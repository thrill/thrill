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

#include <c7a/net/connection.hpp>
#include <c7a/net/stream.hpp>
#include <c7a/data/binary_buffer_builder.hpp>
#include <c7a/data/binary_buffer.hpp>
#include <c7a/data/buffer_chain.hpp>
#include <c7a/common/stats.hpp>

#include <vector>
#include <string>
#include <sstream>

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
//! This class is the state machine for the callback-hell from Dispatcher
class Channel
{
public:
    //! Called to transfer the polling responsibility back to the channel multiplexer
    typedef std::function<void (Connection& s)> ReleaseSocketCallback;

    //! Creates a new channel instance
    Channel(DispatcherThread& dispatcher,
            ReleaseSocketCallback release_callback,
            size_t id, int expected_streams,
            std::shared_ptr<data::BufferChain> target,
            std::shared_ptr<common::Stats> stats)
        : dispatcher_(dispatcher),
          release_(release_callback),
          id_(id),
          expected_streams_(expected_streams),
          finished_streams_(0),
          bytes_received_(0),
          target_(target),
          stats_(stats),
          waiting_timer_(stats_->CreateTimer("channel::" + std::to_string(id), "wait_timer")),
          wait_counter_(stats_->CreateTimedCounter("channel::" + std::to_string(id), "wait_counter")),
          header_arrival_counter_(stats_->CreateTimedCounter("channel::" + std::to_string(id), "header_arrival"))
    { }

    void CloseLoopback() {
        CloseStream();
    }

    //! Takes over the polling responsibility from the channel multiplexer
    //!
    //! This is the start state of the callback state machine.
    //! end-of-streams are handled directly
    //! all other block headers are parsed
    void PickupStream(Connection& s, struct StreamBlockHeader head) {
        std::stringstream stats_group;
        stats_group << "channel::" << id_ << "::" << s.GetPeerAddress();
        Stream* stream = new Stream(s, head, stats_->CreateTimer(stats_group.str(), "block::lifetime"));
        stream->lifetime_timer->Start();
        header_arrival_counter_->Trigger();
        if (stream->IsFinished()) {
            sLOG << "end of stream on" << stream->socket << "in channel" << id_;
            stream->lifetime_timer->Stop();
            *waiting_timer_ += stream->wait_timer; //accumulate
            bytes_received_ += stream->bytes_read;
            CloseStream();
            release_(stream->socket);
            delete stream;
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

    size_t Id() {
        return id_;
    }

private:
    static const bool debug = false;
    DispatcherThread& dispatcher_;
    ReleaseSocketCallback release_;

    size_t id_;
    int active_streams_;
    int expected_streams_;
    int finished_streams_;
    size_t bytes_received_;

    std::shared_ptr<data::BufferChain> target_;
    data::BinaryBufferBuilder data_;

    std::shared_ptr<common::Stats> stats_;
    common::TimerPtr waiting_timer_;
    common::TimedCounterPtr wait_counter_;
    common::TimedCounterPtr header_arrival_counter_;

    //! Decides if there are more elements to read of a new stream block header
    //! is expected (transfers control back to multiplexer)
    void ReadFromStream(Stream* stream) {
        if (stream->bytes_read < stream->header.expected_bytes) {
            ExpectData(stream);
        }
        else {
            sLOG << "reached end of block on" << stream->socket << "in channel" << id_;
            data_.set_elements(stream->header.expected_elements);
            target_->Append(data_);
            data_.Detach();
            data_ = data::BinaryBufferBuilder(data::BinaryBuffer::DEFAULT_SIZE);
            active_streams_--;
            stream->lifetime_timer->Stop();
            *waiting_timer_ += stream->wait_timer; //accumulate
            bytes_received_ += stream->bytes_read;
            stream->ResetHead();
            wait_counter_->Trigger();
            release_(stream->socket);
            delete stream;
        }
    }

    void CloseStream() {
        finished_streams_++;
        if (finished_streams_ == expected_streams_) {
            sLOG << "channel" << id_ << " is closed";
            stats_->AddReport("channel::bytes_read", std::to_string(id_), std::to_string(bytes_received_));
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
        stream->wait_timer.Start();
        wait_counter_->Trigger();
        dispatcher_.AsyncRead(stream->socket, exp_size, callback);
    }

    inline void ConsumeData(Connection& /*s*/, Buffer&& buffer, Stream* stream) {
        stream->wait_timer.Stop();
        sLOG << "read data on" << stream->socket << "in channel" << id_;
        stream->bytes_read += buffer.size();
        data_.Append(buffer.data(), buffer.size());
        ReadFromStream(stream);
    }
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_CHANNEL_HEADER

/******************************************************************************/
