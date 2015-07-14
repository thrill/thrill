/*******************************************************************************
 * c7a/net/stream.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_STREAM_HEADER
#define C7A_NET_STREAM_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/common/stats_timer.hpp>

#include <cstdlib>
#include <cstdio>
#include <string>

namespace c7a {
namespace net {

//! \ingroup net
//! \{

//! Block header is sent before a sequence of blocks
//! it indicates the number of elements and their
//! boundaries
//!
//! Provides a serializer and two partial deserializers
//! A StreamBlockHeader with num_elements = 0 marks the end of a stream
struct StreamBlockHeader {
    size_t      channel_id;
    size_t      expected_bytes;
    size_t      expected_elements;
    size_t      sender_rank;

    //! Reads the channel id and the number of elements in this block
    void        ParseHeader(const std::string& buffer) {
        assert(buffer.size() == sizeof(StreamBlockHeader));
        size_t offset1 = sizeof(channel_id);
        size_t offset2 = offset1 + sizeof(expected_bytes);
        size_t offset3 = offset2 + sizeof(expected_elements);
        memcpy(&channel_id, buffer.c_str() + 0, sizeof(channel_id));
        memcpy(&expected_bytes, buffer.c_str() + offset1, sizeof(expected_bytes));
        memcpy(&expected_elements, buffer.c_str() + offset2, sizeof(expected_elements));
        memcpy(&sender_rank, buffer.c_str() + offset3, sizeof(sender_rank));
    }

    //! Serializes the whole block struct into a buffer
    std::string Serialize() {
        size_t size = sizeof(StreamBlockHeader);
        char* result = new char[size];
        char* offset0 = result;
        char* offset1 = offset0 + sizeof(channel_id);
        char* offset2 = offset1 + sizeof(expected_elements);
        char* offset3 = offset2 + sizeof(expected_bytes);

        memcpy(offset0, &channel_id, sizeof(channel_id));
        memcpy(offset1, &expected_bytes, sizeof(expected_bytes));
        memcpy(offset2, &expected_elements, sizeof(expected_elements));
        memcpy(offset3, &sender_rank, sizeof(sender_rank));
        return std::string(result, size);
    }

    //! resets to a End-of-Stream block header
    void        Reset() {
        expected_bytes = 0;
        expected_elements = 0;
        sender_rank = 0;
    }

    //! Indicates if this is the end-of-stream block header
    bool        IsStreamEnd() const {
        return expected_bytes == 0;
    }

    //! Frees all memory of the block struct
    ~StreamBlockHeader() {
        Reset();
    }
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_STREAM_HEADER

/******************************************************************************/
