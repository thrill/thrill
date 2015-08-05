/*******************************************************************************
 * c7a/data/stream_block_header.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_STREAM_BLOCK_HEADER_HEADER
#define C7A_DATA_STREAM_BLOCK_HEADER_HEADER

#include <c7a/net/buffer_builder.hpp>
#include <c7a/net/buffer_reader.hpp>
#include <c7a/net/connection.hpp>

#include <cstdio>
#include <cstdlib>
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
    size_t      size;
    size_t      first_item;
    size_t      nitems;
    size_t      sender_rank;
    size_t      receiver_worker_id;
    size_t      sender_worker_id;

    //! Reads the channel id and the number of elements in this block
    void        ParseHeader(const net::Buffer& buffer) {
        assert(buffer.size() == sizeof(StreamBlockHeader));
        net::BufferReader br(buffer);
        channel_id = br.Get<size_t>();
        size = br.Get<size_t>();
        first_item = br.Get<size_t>();
        nitems = br.Get<size_t>();
        sender_rank = br.Get<size_t>();
        receiver_worker_id = br.Get<size_t>();
        sender_worker_id = br.Get<size_t>();
    }

    //! Serializes the whole block struct into a buffer
    net::Buffer Serialize() {
        net::BufferBuilder bb;
        bb.Reserve(4 * sizeof(size_t));
        bb.Put<size_t>(channel_id);
        bb.Put<size_t>(size);
        bb.Put<size_t>(first_item);
        bb.Put<size_t>(nitems);
        bb.Put<size_t>(sender_rank);
        bb.Put<size_t>(receiver_worker_id);
        bb.Put<size_t>(sender_worker_id);
        return bb.ToBuffer();
    }

    //! Indicates if this is the end-of-stream block header
    bool        IsStreamEnd() const {
        return size == 0;
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_STREAM_BLOCK_HEADER_HEADER

/******************************************************************************/
