#pragma once

#include <stdlib.h> //free
#include <stdio.h> //mempcy

#include <c7a/net/socket.hpp>

namespace c7a {
namespace net {

//! Block header is sent before a sequence of blocks
//! it indicates the number of elements and their
//! boundaries
//
//! A StreamBlockHeader with num_elements = 0 marks the end of a stream
struct StreamBlockHeader {
    size_t channel_id;
    size_t num_elements;
    size_t* boundaries = 0;

    void ParseIdAndNumElem(const std::string& buffer) {
        memcpy(&channel_id, buffer.c_str(), sizeof(channel_id));
        memcpy(&num_elements, buffer.c_str() + sizeof(channel_id), sizeof(num_elements));
        boundaries = new size_t[sizeof(size_t) * num_elements];
    }

    void ParseBoundaries(const std::string& buffer) {
        memcpy(&boundaries, buffer.c_str(), sizeof(size_t) * num_elements);
    }

    std::string Serialize() {
        size_t size = sizeof(size_t) * (num_elements + 2);
        std::string result(size, '0');
        size_t offset0 = (size_t)result.data();
        size_t offset1 = offset0 + sizeof(channel_id);;
        size_t offset2 = offset1 + sizeof(num_elements);;

        memcpy((void*)offset0, &channel_id,   sizeof(channel_id));
        memcpy((void*)offset1, &num_elements, sizeof(num_elements));
        memcpy((void*)offset2, &boundaries,   sizeof(size_t) * num_elements);
        return result;
    }

    void Reset() {
        num_elements = 0;
        //TODO delete boundaries w/o double-freeing
    }

    bool IsStreamEnd() const {
        return num_elements == 0;
    }

    ~StreamBlockHeader() {
        Reset();
        //if (boundaries)
            //delete [] boundaries;
    }
};
struct Stream {
    struct StreamBlockHeader header;
    Socket& socket;
    int elements_read = 0;

    Stream(Socket& socket, struct StreamBlockHeader header)
        : header(header)
        , socket(socket) { }

    void ResetHead() {
        elements_read = 0;
        header.Reset();
    }

    bool IsFinished() const {
        return header.IsStreamEnd();
    }
};


}}

