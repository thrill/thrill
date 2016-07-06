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
    static constexpr bool self_verify = common::g_self_verify;

    MagicByte magic = MagicByte::Invalid;
    uint32_t size = 0;
    uint32_t num_items = 0;
    // previous two bits are packed with first_item
    uint32_t first_item : 30;
    //! typecode self verify
    bool typecode_verify : 1;
    //! is last block piggybacked indicator
    bool is_last_block : 1;

    MultiplexerHeader() = default;

    explicit MultiplexerHeader(MagicByte m, const PinnedBlock& b)
        : magic(m),
          size(b.size()),
          num_items(b.num_items()),
          first_item(b.first_item_relative()),
          typecode_verify(b.typecode_verify()) {
        if (!self_verify)
            assert(!typecode_verify);
    }

    static constexpr size_t header_size =
        sizeof(MagicByte) + 3 * sizeof(uint32_t);

    static constexpr size_t total_size =
        header_size + 3 * sizeof(size_t);
} THRILL_ATTRIBUTE_PACKED;

static_assert(sizeof(MultiplexerHeader) == MultiplexerHeader::header_size,
              "MultiplexerHeader has invalid size");

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
        : MultiplexerHeader(m, b) { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        bb.Reserve(MultiplexerHeader::total_size);
        bb.Put<StreamMultiplexerHeader>(*this);
    }

    //! Reads the stream id and the number of elements in this block
    static StreamMultiplexerHeader Parse(net::BufferReader& br) {
        return br.Get<StreamMultiplexerHeader>();
    }

    //! Indicates if this is the end-of-line block header
    bool IsEnd() const {
        return size == 0;
    }

    //! Calculate the sender host_rank from sender_worker and workers_per_host.
    size_t CalcHostRank(size_t workers_per_host) const {
        return sender_worker / workers_per_host;
    }
} THRILL_ATTRIBUTE_PACKED;

static_assert(sizeof(StreamMultiplexerHeader) == MultiplexerHeader::total_size,
              "StreamMultiplexerHeader has invalid size");

class PartitionMultiplexerHeader : public MultiplexerHeader
{
public:
    size_t partition_set_id = 0;
    // probably needed
    // size_t partition_index = 0;
    size_t receiver_local_worker = 0;
    size_t sender_worker = 0;

    PartitionMultiplexerHeader() = default;

    explicit PartitionMultiplexerHeader(const PinnedBlock& b)
        : MultiplexerHeader(MagicByte::PartitionBlock, b) { }

    //! Serializes the whole block struct into a buffer
    void Serialize(net::BufferBuilder& bb) const {
        bb.Reserve(MultiplexerHeader::total_size);
        bb.Put<PartitionMultiplexerHeader>(*this);
    }

    //! Reads the stream id and the number of elements in this block
    static PartitionMultiplexerHeader Parse(net::BufferReader& br) {
        return br.Get<PartitionMultiplexerHeader>();
    }

    //! Indicates if this is the end-of-line block header
    bool IsEnd() const {
        return size == 0;
    }
} THRILL_ATTRIBUTE_PACKED;

static_assert(
    sizeof(PartitionMultiplexerHeader) == MultiplexerHeader::total_size,
    "PartitionMultiplexerHeader has invalid size");

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MULTIPLEXER_HEADER_HEADER

/******************************************************************************/
