#pragma once

#include <c7a/net/socket.hpp>
#include <c7a/net/stream.hpp>
namespace c7a {
namespace net {
//! \ingroup net
//! \{

//! A Chanel is a collection of \ref Stream instances and bundles them to a
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
class Channel
{
public:
    //! Called to transfer the polling responsibility back to the channel multiplexer
    typedef std::function<void (Socket& s)> ReleaseSocketCallback;

    //! Creates a new channel instance
    Channel(NetDispatcher& dispatcher, ReleaseSocketCallback release_callback, int id, int expected_streams)
        : dispatcher_(dispatcher),
          release_(release_callback),
          id_(id),
          expected_streams_(expected_streams) { }

    //! Takes over the polling responsibility from the channel multiplexer
    //!
    //! This is the start state of the callback state machine.
    //! end-of-streams are handled directly
    //! all other block headers are parsed
    void PickupStream(Socket& s, struct StreamBlockHeader head)
    {
        struct Stream* stream = new struct Stream (s, head);
        if (stream->IsFinished()) {
            LOG << "end of stream on socket" << stream->socket.GetFileDescriptor() << "in channel" << id_;
            finished_streams_++;
        }
        else {
            LOG << "pickup stream on socket" << stream->socket.GetFileDescriptor() << "in channel" << id_;
            active_streams_++;
            size_t expected_size = stream->header.num_elements * sizeof(size_t);

            auto callback = std::bind(&Channel::ReadSecondHeaderPartFrom,
                                      this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      stream);

            dispatcher_.AsyncRead(stream->socket, expected_size, callback);
        }
    }

    //! Indicates whether all streams are finished
    bool Finished() const
    {
        return finished_streams_ == expected_streams_;
    }

    //! Accesses the data of the channel
    const std::vector<std::string> & Data()
    {
        return data_;
    }

private:
    static const bool debug = true;
    NetDispatcher& dispatcher_;
    ReleaseSocketCallback release_;

    int id_;
    int active_streams_;
    int expected_streams_;
    int finished_streams_;
    std::vector<std::string> data_;

    //!The first header part is read in the channel multiplexer to see which
    //!channel the package belongs to
    //!The second part of the header (boundaries of the block) is read here
    //!
    //! This is the second state of the callback state machine
    void ReadSecondHeaderPartFrom(Socket& s, const std::string& buffer, struct Stream* stream)
    {
        (void)s; //supress 'unused paramter' warning - needs to be in parameter list though
        LOG << "read #elements on socket" << stream->socket.GetFileDescriptor() << "in channel" << id_;
        assert(stream->header.num_elements > 0);

        stream->header.ParseBoundaries(buffer);
        ReadFromStream(stream);
    }

    //! Decides if there are more elements to read of a new stream block header
    //! is expected (transfers control back to multiplexer)
    void ReadFromStream(struct Stream* stream)
    {
        if (stream->elements_read < stream->header.num_elements) {
            ExpectData(stream);
        }
        else {
            LOG << "reached end of block on socket" << stream->socket.GetFileDescriptor() << "in channel" << id_;
            active_streams_--;
            stream->ResetHead();
            release_(stream->socket);
            delete stream;
        }
    }

    //! Expect data to arrive at the socket.
    //! Size of element is known
    inline void ExpectData(struct Stream* stream)
    {
        //TODO we might want to read multiple items in a batch and cut them
        //into single pieces later...
        auto exp_size = stream->header.boundaries[stream->elements_read++];
        auto callback = std::bind(&Channel::ConsumeData, this, std::placeholders::_1, std::placeholders::_2, stream);
        LOG << "expect data with " << exp_size << " bytes on socket" << stream->socket.GetFileDescriptor() << "in channel" << id_;
        dispatcher_.AsyncRead(stream->socket, exp_size, callback);
    }

    inline void ConsumeData(Socket& s, const std::string& buffer, struct Stream* stream)
    {
        LOG << "read data on socket" << stream->socket.GetFileDescriptor() << "in channel" << id_;
        data_.push_back(buffer);  //TODO give buffer to AsyncRead instead of copying data here!
        ReadFromStream(stream);
    }
};
//! \}
}
}
