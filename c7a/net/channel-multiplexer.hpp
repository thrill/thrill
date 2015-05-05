#pragma once

#include <stdlib.h> //free
#include <cassert>
#include <c7a/data/data_manager.hpp>
#include <c7a/net/net-exception.hpp>
#include <c7a/common/logger.hpp>
#include "socket.hpp"

namespace c7a {
namespace net {
#define BUFFER_SIZE 1024

//! Block header is sent before a sequence of blocsk
//! it indicates the number of elements and their
//! boundaries
//
//! A BlockHeader with num_elements = 0 marks the end of a stream
struct BlockHeader {
    size_t channel_id;
    size_t num_elements;
    size_t boundaries[];

    template <class SocketType>
    bool ReadFromSocket(SocketType& socket)
    {
        int flags = 0;

        //how many elements are in the header & which channel number?
        auto expected_size = sizeof(num_elements) + sizeof(channel_id);
        auto read = socket.recv(&num_elements, expected_size, flags);
        if (read != expected_size)
            return false;
        SpacingLogger(true) << "expecting" << num_elements << "elements";
        expected_size = sizeof(size_t) * num_elements;

        //read #num_elements boundary informations
        read = socket.recv(&boundaries, expected_size, flags);
        if (read != expected_size)
            return false;
    }
};

//Forward Magic
class ChannelSink;

//! A Stream is the state of the byte stream of one channel on one socket
//
//! Streams are opened when the first BlockHeader is received
//! As long as the BlockHeader has num_elements that are not received yet,
//! it is active
//! BlockHeaders can be replaced by new ones when consume is called.
//! Streams are closed when a BlockHeader was consumed that indicated so.
//
//! HEAD(_, 2, x x)|elem1|elem2|HEAD(_, 1, x)|emle3|HEAD(_, 0)
//! 1. open stream (done externally)
//! 2. consume will read 2 elements -> becomes inactive
//! 3. consume reactivates when 2nd header is read
//! 4. consume will read elem3
//! 5. consume will read 0-Header closes stream
class Stream
{
public:
    Stream(struct BlockHeader header, std::shared_ptr<ChannelSink> channel)
        : channel_(channel),
          current_head_(header) { }

    bool IsActive() { return BytesRemaining() > 0; }
    bool IsClosed() { return current_head_.num_elements == 0; }
    std::shared_ptr<ChannelSink> GetChannel() { return channel_; }

private:
    std::shared_ptr<ChannelSink> channel_;
    struct BlockHeader current_head_;
    size_t read_; //# bytes read
    bool closed_;
    size_t BytesRemaining()
    {
        return current_head_.boundaries[current_head_.num_elements - 1] - read_;
    }
};

//! Data channel for receiving data from other workers
class ChannelSink : std::enable_shared_from_this<ChannelSink>
{
public:
    //! Creates instance that has num_senders active senders
    //! Writes received data to targetDIA
    ChannelSink() { }

    //! Creates a stream in this Channel and returns a reference to it
    std::shared_ptr<Stream> AddStream(int fd, BlockHeader header)
    {
        auto stream_ptr = std::make_shared<Stream>(header, shared_from_this());
        active_streams_.insert(std::make_pair(fd, stream_ptr));
        return stream_ptr;
    }

    std::shared_ptr<Stream> GetStream(int fd)
    {
        if (!HasStreamOn(fd))
            return std::shared_ptr<Stream>();
        return active_streams_[fd];
    }

    bool HasStreamOn(int fd)
    {
        return active_streams_.find(fd) != active_streams_.end();
    }

    template <class SocketType>
    void Consume(SocketType& socket) { }

private:
    std::map<int, std::shared_ptr<Stream> > active_streams_;
};

//! Multiplexes virtual Connections on NetDispatcher
template <class SocketType>
class ChannelMultiplexer
{
public:
    //! Called by the network dispatcher
    void Consume(SocketType& connection)
    {
        int fd = connection.GetFileDescriptor();

        //new socket: -> is a new stream
        if (!KnowsSocket(fd) || !HasActiveStream(fd)) {
            auto header = ReadBlockHeader(connection);
            ChannelPtr channel;
            if (!HasChannel(header.channel_id)) {
                channel = CreateChannel(fd, header.channel_id);
            }
            else {
                channel = channels_[header.channel_id];
            }

            StreamPtr stream;
            if (!channel->HasStreamOn(fd))
                SetActiveStream(fd, channel->AddStream(fd, header));
            else
                SetActiveStream(fd, channel->GetStream(fd));
        }

        GetActiveStream(fd)->GetChannel()->Consume(connection);
    }

    bool HasChannel(int id)
    {
        return channels_.find(id) != channels_.end();
    }

    std::shared_ptr<ChannelSink> PickupChannel(int id)
    {
        return channels_[id];
    }

private:
    typedef std::shared_ptr<ChannelSink> ChannelPtr;
    typedef std::shared_ptr<Stream> StreamPtr;

    //! Channels have an ID in block headers
    std::map<int, ChannelPtr> channels_;

    //!Streams per Socket
    std::map<int, StreamPtr> active_stream_on_fd_;
    std::map<int, std::map<int, ChannelPtr> > channels_on_fd_;

    ChannelPtr CreateChannel(int fd, int channel_id)
    {
        //create channel and make accessible via ID
        std::shared_ptr<ChannelSink> channel;
        channels_.insert(std::make_pair(channel_id, channel));

        //associate channel with fd
        assert(KnowsSocket(fd));
        channels_on_fd_[fd].insert(std::make_pair(channel_id, channel));
        return channel;
    }

    void SetActiveStream(int fd, StreamPtr stream)
    {
        assert(KnowsSocket(fd));
        active_stream_on_fd_[fd] = stream;
    }

    bool HasActiveStream(int fd)
    {
        return GetActiveStream(fd) != nullptr;
    }

    std::shared_ptr<Stream> GetActiveStream(int fd)
    {
        return active_stream_on_fd_[fd];
    }

    bool KnowsSocket(int fd)
    {
        return channels_on_fd_.find(fd) != channels_on_fd_.end();
    }

    //!Reads, creates, and returns the header of a block
    BlockHeader ReadBlockHeader(SocketType& socket)
    {
        //TODO we assume that the header can ALWAYS be read. Handle edge case
        //where this does not happen

        int flags = 0;
        struct BlockHeader header;
        if (!header.ReadFromSocket<SocketType>(socket))
            throw c7a::NetException("Error while reading Block header");

        return header;
    }
};
}
}
