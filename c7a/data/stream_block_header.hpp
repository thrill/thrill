/*******************************************************************************
 * c7a/data/stream_block_header.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_STREAM_BLOCK_HEADER_HEADER
#define C7A_DATA_STREAM_BLOCK_HEADER_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/net/buffer_builder.hpp>
#include <c7a/net/buffer_reader.hpp>
#include <c7a/common/stats_timer.hpp>

#include <cstdlib>
#include <cstdio>
#include <string>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
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
    void        ParseHeader(const net::Buffer& buffer) {
        assert(buffer.size() == sizeof(StreamBlockHeader));
        net::BufferReader br(buffer);
        channel_id = br.Get<size_t>();
        expected_bytes = br.Get<size_t>();
        expected_elements = br.Get<size_t>();
        sender_rank = br.Get<size_t>();
    }

    //! Serializes the whole block struct into a buffer
    net::Buffer Serialize() {
        net::BufferBuilder bb;
        bb.Reserve(4 * sizeof(size_t));
        bb.Put<size_t>(channel_id);
        bb.Put<size_t>(expected_bytes);
        bb.Put<size_t>(expected_elements);
        bb.Put<size_t>(sender_rank);
        return bb.ToBuffer();
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

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_STREAM_BLOCK_HEADER_HEADER

/******************************************************************************/
