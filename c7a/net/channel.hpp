#pragma once

#include <c7a/net/socket.hpp>
#include <c7a/net/stream.hpp>
namespace c7a {
namespace net {

class Channel {
public:

    typedef std::function<void(Socket& s)> ReleaseSocketCallback;

    Channel(NetDispatcher& dispatcher, ReleaseSocketCallback release_callback, int id, int expected_streams)
        : dispatcher_(dispatcher)
        , release_(release_callback)
        , id_(id)
        , expected_streams_(expected_streams) { }

    void PickupStream(struct Stream& stream) {
        if (stream.IsFinished()) {
            LOG << "end of stream on socket" << stream.socket.GetFileDescriptor() << "in channel" << id_;
            finished_streams_++;
        } else {
            LOG << "pickup stream on socket" << stream.socket.GetFileDescriptor() << "in channel" << id_;
            active_streams_++;
            auto expected_size = stream.header.num_elements * sizeof(size_t);

            auto callback = std::bind(&Channel::ReadSecondHeaderPartFrom,
                                        this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        stream);

            dispatcher_.AsyncRead(stream.socket, expected_size, callback);
        }
    }

    bool Finished() {
        return finished_streams_ == expected_streams_;
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

    void ReadSecondHeaderPartFrom(Socket& s, const std::string& buffer, struct Stream& stream) {
        (void) s;//supress 'unused paramter' warning - needs to be in parameter list though
        LOG << "read #elements on socket" << stream.socket.GetFileDescriptor() << "in channel" << id_;
        assert(stream.header.channel_id = id_);
        assert(stream.header.num_elements > 0);

        stream.header.ParseBoundaries(buffer);
        ReadFromStream(stream);
    }

    void ReadFromStream(struct Stream& stream) {
        if (stream.elements_read < stream.header.num_elements) {
            ExpectData(stream);
        } else {
            LOG << "reached end of block on socket" << stream.socket.GetFileDescriptor() << "in channel" << id_;
            active_streams_--;
            stream.ResetHead();
            release_(stream.socket);
        }
    }

    inline void ExpectData(struct Stream& stream) {
        auto exp_size = stream.header.boundaries[stream.elements_read];
        auto callback = std::bind(&Channel::ConsumeData, this, std::placeholders::_1, std::placeholders::_2, stream);
        LOG << "expect data on socket" << stream.socket.GetFileDescriptor() << "in channel" << id_;
        dispatcher_.AsyncRead(stream.socket, exp_size, callback);
    }

    inline void ConsumeData(Socket& s, const std::string& buffer, struct Stream& stream) {
         LOG << "read data on socket" << stream.socket.GetFileDescriptor() << "in channel" << id_;
         data_.push_back(buffer); //TODO give buffer to AsyncRead instead of copying data here!
         ExpectData(stream);
    }
};

}}
