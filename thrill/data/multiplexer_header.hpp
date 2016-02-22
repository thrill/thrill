/*******************************************************************************
 * thrill/data/multiplexer_header.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MULTIPLEXER_HEADER_HEADER
#define THRILL_DATA_MULTIPLEXER_HEADER_HEADER

#include <thrill/data/block.hpp>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/net/buffer_reader.hpp>
#include <thrill/net/connection.hpp>

#include <cstdio>
#include <cstdlib>
#include <string>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

enum class MagicByte : uint8_t {
    Invalid, CatStreamBlock, MixStreamBlock, PartitionBlock
};

struct BlockHeader {
public:
    MagicByte           magic = MagicByte::Invalid;
    size_t              size = 0;
    size_t              first_item = 0;
    size_t              num_items = 0;

    BlockHeader() = default;

    explicit BlockHeader(MagicByte m, const PinnedBlock& b)
        : magic(m),
          size(b.size()),
          first_item(b.first_item_relative()),
          num_items(b.num_items())
    { }

    static const size_t header_size = sizeof(MagicByte) + 3 * sizeof(size_t);

    static const size_t total_size = header_size + 4 * sizeof(size_t);

    void                SerializeBlockHeader(net::BufferBuilder& bb) const {
        bb.Put<MagicByte>(magic);
        bb.Put<size_t>(size);
        bb.Put<size_t>(first_item);
        bb.Put<size_t>(num_items);
    }

    void                ParseBlockHeader(net::BufferReader& br) {
        magic = br.Get<MagicByte>();
        size = br.Get<size_t>();
        first_item = br.Get<size_t>();
        num_items = br.Get<size_t>();
    }
};

//! Block header is sent before a sequence of blocks
//! it indicates the number of elements and their
//! boundaries
//!
//! Provides a serializer and two partial deserializers
//! A StreamBlockHeader with num_elements = 0 marks the end of a stream
struct StreamBlockHeader : public BlockHeader {
    size_t stream_id = 0;
    size_t sender_rank = 0;
    size_t receiver_local_worker_id = 0;
    size_t sender_local_worker_id = 0;

    StreamBlockHeader() = default;

    explicit StreamBlockHeader(MagicByte m, const PinnedBlock& b)
        : BlockHeader(m, b)
    { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        SerializeBlockHeader(bb);
        bb.Put<size_t>(stream_id);
        bb.Put<size_t>(sender_rank);
        bb.Put<size_t>(receiver_local_worker_id);
        bb.Put<size_t>(sender_local_worker_id);
    }

    //! Reads the stream id and the number of elements in this block
    void ParseHeader(net::BufferReader& br) {
        ParseBlockHeader(br);
        stream_id = br.Get<size_t>();
        sender_rank = br.Get<size_t>();
        receiver_local_worker_id = br.Get<size_t>();
        sender_local_worker_id = br.Get<size_t>();
    }

    //! Indicates if this is the end-of-line block header
    bool IsEnd() const {
        return size == 0;
    }
};

struct PartitionBlockHeader : public BlockHeader {
    size_t partition_set_id = 0;
    size_t partition_index = 0;
    size_t sender_rank = 0;
    size_t receiver_local_worker_id = 0;
    size_t sender_local_worker_id = 0;

    PartitionBlockHeader() = default;

    explicit PartitionBlockHeader(const PinnedBlock& b)
        : BlockHeader(MagicByte::PartitionBlock, b)
    { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        SerializeBlockHeader(bb);
        bb.Put<size_t>(partition_set_id);
        bb.Put<size_t>(partition_index);
        bb.Put<size_t>(sender_rank);
        bb.Put<size_t>(receiver_local_worker_id);
        bb.Put<size_t>(sender_local_worker_id);
    }

    //! Reads the stream id and the number of elements in this block
    void ParseHeader(net::BufferReader& br) {
        ParseBlockHeader(br);
        partition_set_id = br.Get<size_t>();
        partition_index = br.Get<size_t>();
        sender_rank = br.Get<size_t>();
        receiver_local_worker_id = br.Get<size_t>();
        sender_local_worker_id = br.Get<size_t>();
    }

    //! Indicates if this is the end-of-line block header
    bool IsEnd() const {
        return size == 0;
    }
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MULTIPLEXER_HEADER_HEADER

/******************************************************************************/
