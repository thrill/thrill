#pragma once

#include <stdlib.h> //free
#include <cassert>
#include <c7a/data/data_manager.hpp>
#include "socket.hpp"

namespace c7a {
namespace net {

#define BUFFER_SIZE 1024
/*
struct PackageHeader {
    size_t num_elements; //number of elements
    size_t[] borders;//mark ends of elements

    int parse(void* data, size_t len) {
        //Do we have a #elements part?
        if (len < sizeof(size_t))
            return -1;

        //read #elements
        num_elements = (size_t*) data;

        //read element border array
        if (len < sizeof(size_t) + num_elements * sizeof(size_t))
            return -1;
        borders = data + sizeof(size_t);

        //return #bytes to skip
        return sizeof(size_t) + num_elements * sizeof(size_t);
    }
};

struct PackagePart {
    struct PackagePart* next = nullptr;
    void* data;
    size_t len;

    PackagePart~() {
        free(data);
    }
};

struct PackageHead {
    struct PackageHeader header;
    size_t current;
    struct PackagePart* head;

    size_t GetBytesLeft() {
        return GetTotalBytes() - current;
    }

    size_t GetTotalBytes() {
        return header.borders[header.num_elements - 1];
    }
};

//! Data channel for receiving data from other workers
class ChannelSink {
public:
    //! Creates instance that has num_senders active senders
    //! Writes received data to targetDIA
    ChannelSink(int num_senders, BlockEmitter<Blob> target) {
        active_senders_ = new bool[num_senders];
        has_active_sender_ = num_senders > 0;
        target_ = target;
    }

    //! Signals that a sender of the channel is closed
    //! Returns true if more senders are active
    bool SenderClosed(int sender_id) {
        assert(sender_id >= 0);
        assert(sender_id < sizeof(active_senders_));

        active_senders_[sender_id] = false;
        bool result = false;
        for (int i = 0; !result && i < sizeof(active_senders_); i++)
            if (active_senders_[i])
                result = true;
        has_active_sender_ = result;
        return result;
    }

    //! Call this if the network communication aborted
    void Abort {} //TODO implement me

    ~ChannelSink() {
        free(active_senders_);
    }
private:
    bool[] active_senders_;
    bool has_active_sender_;
    BlockEmitter<Blob> target_;
};
*/

//! Block header is sent before a sequence of blocsk
//! it indicates the number of elements and their
//! boundaries
struct BlockHeader {
    size_t num_elements;
    size_t boundaries[];
};

//! Multiplexes virtual Connections on NetDispatcher
class ChannelMultiplexer {
public:
    //! Called by the network dispatcher
    void Consume(Socket& connection) {
        //int fd = connection.GetFileDescriptor();
        int flags = 0;
        struct BlockHeader header;
        connection.recv_one(&header, sizeof(header), flags);
    }


private:

};
}}
