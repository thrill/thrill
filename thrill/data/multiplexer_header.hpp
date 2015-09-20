/*******************************************************************************
 * thrill/data/multiplexer_header.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
    INVALID, CAT_CHANNEL_BLOCK, MIXED_CHANNEL_BLOCK, PARTITION_BLOCK
};

struct BlockHeader {
public:
    MagicByte           magic = MagicByte::INVALID;
    size_t              size = 0;
    size_t              first_item = 0;
    size_t              num_items = 0;

    BlockHeader() = default;

    explicit BlockHeader(MagicByte m, const Block& b)
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
//! A ChannelBlockHeader with num_elements = 0 marks the end of a channel
struct ChannelBlockHeader : public BlockHeader {
    size_t channel_id;
    size_t sender_rank;
    size_t receiver_local_worker_id;
    size_t sender_local_worker_id;

    ChannelBlockHeader() = default;

    explicit ChannelBlockHeader(MagicByte m, const Block& b)
        : BlockHeader(m, b)
    { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        SerializeBlockHeader(bb);
        bb.Put<size_t>(channel_id);
        bb.Put<size_t>(sender_rank);
        bb.Put<size_t>(receiver_local_worker_id);
        bb.Put<size_t>(sender_local_worker_id);
    }

    //! Reads the channel id and the number of elements in this block
    void ParseHeader(net::BufferReader& br) {
        ParseBlockHeader(br);
        channel_id = br.Get<size_t>();
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
    size_t partition_set_id;
    size_t partition_index;
    size_t sender_rank;
    size_t receiver_local_worker_id;
    size_t sender_local_worker_id;

    PartitionBlockHeader() = default;

    explicit PartitionBlockHeader(const Block& b)
        : BlockHeader(MagicByte::PARTITION_BLOCK, b)
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

    //! Reads the channel id and the number of elements in this block
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
