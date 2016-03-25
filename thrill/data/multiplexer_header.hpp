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
#include <thrill/data/stream.hpp>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/net/buffer_reader.hpp>
#include <thrill/net/connection.hpp>

#include <cstdio>
#include <cstdlib>
#include <string>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

class MultiplexerHeader
{
public:
    MagicByte magic = MagicByte::Invalid;
    size_t size = 0;
    size_t first_item = 0;
    size_t num_items = 0;

    MultiplexerHeader() = default;

    explicit MultiplexerHeader(MagicByte m, const PinnedBlock& b)
        : magic(m),
          size(b.size()),
          first_item(b.first_item_relative()),
          num_items(b.num_items())
    { }

    static constexpr size_t header_size = sizeof(MagicByte) + 3 * sizeof(size_t);

    static constexpr size_t total_size = header_size + 3 * sizeof(size_t);

    void SerializeMultiplexerHeader(net::BufferBuilder& bb) const {
        bb.Put<MagicByte>(magic);
        bb.Put<size_t>(size);
        bb.Put<size_t>(first_item);
        bb.Put<size_t>(num_items);
    }

    void ParseMultiplexerHeader(net::BufferReader& br) {
        magic = br.Get<MagicByte>();
        size = br.Get<size_t>();
        first_item = br.Get<size_t>();
        num_items = br.Get<size_t>();
    }
};

/*!
 * Block header is sent before a sequence of blocks it indicates the number of
 * elements and their boundaries
 *
 * Provides a serializer and two partial deserializers. A
 * StreamMultiplexerHeader with size = 0 marks the end of a stream.
 */
class StreamMultiplexerHeader : public MultiplexerHeader
{
public:
    size_t stream_id = 0;
    size_t receiver_local_worker = 0;
    //! global worker rank of sender
    size_t sender_worker = 0;

    StreamMultiplexerHeader() = default;

    explicit StreamMultiplexerHeader(MagicByte m, const PinnedBlock& b)
        : MultiplexerHeader(m, b)
    { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        SerializeMultiplexerHeader(bb);
        bb.Put<size_t>(stream_id);
        bb.Put<size_t>(receiver_local_worker);
        bb.Put<size_t>(sender_worker);
    }

    //! Reads the stream id and the number of elements in this block
    void ParseHeader(net::BufferReader& br) {
        ParseMultiplexerHeader(br);
        stream_id = br.Get<size_t>();
        receiver_local_worker = br.Get<size_t>();
        sender_worker = br.Get<size_t>();
    }

    //! Indicates if this is the end-of-line block header
    bool IsEnd() const {
        return size == 0;
    }

    //! Calculate the sender host_rank from sender_worker and workers_per_host.
    size_t CalcHostRank(size_t workers_per_host) const {
        return sender_worker / workers_per_host;
    }
};

class PartitionMultiplexerHeader : public MultiplexerHeader
{
public:
    size_t partition_set_id = 0;
    size_t partition_index = 0;
    size_t receiver_local_worker = 0;
    size_t sender_worker = 0;

    PartitionMultiplexerHeader() = default;

    explicit PartitionMultiplexerHeader(const PinnedBlock& b)
        : MultiplexerHeader(MagicByte::PartitionBlock, b)
    { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        SerializeMultiplexerHeader(bb);
        bb.Put<size_t>(partition_set_id);
        bb.Put<size_t>(partition_index);
        bb.Put<size_t>(receiver_local_worker);
        bb.Put<size_t>(sender_worker);
    }

    //! Reads the stream id and the number of elements in this block
    void ParseHeader(net::BufferReader& br) {
        ParseMultiplexerHeader(br);
        partition_set_id = br.Get<size_t>();
        partition_index = br.Get<size_t>();
        receiver_local_worker = br.Get<size_t>();
        sender_worker = br.Get<size_t>();
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
