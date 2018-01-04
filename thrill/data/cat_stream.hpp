/*******************************************************************************
 * thrill/data/cat_stream.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_CAT_STREAM_HEADER
#define THRILL_DATA_CAT_STREAM_HEADER

#include <thrill/data/block_queue.hpp>
#include <thrill/data/cat_block_source.hpp>
#include <thrill/data/stream.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

/*!
 * A Stream is a virtual set of connections to all other worker instances,
 * hence a "Stream" bundles them to a logical communication context. We call an
 * individual connection from a worker to another worker a "Host".
 *
 * To use a Stream, one can get a vector of BlockWriter via OpenWriters() of
 * outbound Stream. The vector is of size of workers in the system.
 * One can then write items destined to the
 * corresponding worker. The written items are buffered into a Block and only
 * sent when the Block is full. To force a send, use BlockWriter::Flush(). When
 * all items are sent, the BlockWriters **must** be closed using
 * BlockWriter::Close().
 *
 * To read the inbound Connection items, one can get a vector of BlockReader via
 * OpenReaders(), which can then be used to read items sent by individual
 * workers.
 *
 * Alternatively, one can use OpenReader() to get a BlockReader which delivers
 * all items from *all* worker in worker order (concatenating all inbound
 * Connections).
 *
 * As soon as all attached streams of the Stream have been Close() the number of
 * expected streams is reached, the stream is marked as finished and no more
 * data will arrive.
 */
class CatStreamData final : public StreamData
{
public:
    static constexpr bool debug = false;
    static constexpr bool debug_data = false;

    using BlockQueueSource = ConsumeBlockQueueSource;
    using BlockQueueReader = BlockReader<BlockQueueSource>;

    using CatBlockSource = data::CatBlockSource<DynBlockSource>;
    using CatBlockReader = BlockReader<CatBlockSource>;

    using Reader = BlockQueueReader;
    using CatReader = CatBlockReader;

    using Handle = CatStream;

    //! Creates a new stream instance
    CatStreamData(Multiplexer& multiplexer, size_t send_size_limit,
                  const StreamId& id, size_t local_worker_id, size_t dia_id);

    //! non-copyable: delete copy-constructor
    CatStreamData(const CatStreamData&) = delete;
    //! non-copyable: delete assignment operator
    CatStreamData& operator = (const CatStreamData&) = delete;
    //! move-constructor: default
    CatStreamData(CatStreamData&&) = default;

    ~CatStreamData() final;

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id);

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    Writers GetWriters() final;

    //! Creates a BlockReader for each worker. The BlockReaders are attached to
    //! the BlockQueues in the Stream and wait for further Blocks to arrive or
    //! the Stream's remote close. These Readers _always_ consume!
    std::vector<Reader> GetReaders();

    //! Gets a CatBlockSource which includes all incoming queues of this stream.
    CatBlockSource GetCatBlockSource(bool consume);

    //! Creates a BlockReader which concatenates items from all workers in
    //! worker rank order. The BlockReader is attached to one \ref
    //! CatBlockSource which includes all incoming queues of this stream.
    CatReader GetCatReader(bool consume);

    //! Open a CatReader (function name matches a method in File and MixStream).
    CatReader GetReader(bool consume);

    //! shuts the stream down.
    void Close() final;

    //! Indicates if the stream is closed - meaning all remaining streams have
    //! been closed. This does *not* include the loopback stream
    bool closed() const final;

private:
    bool is_closed_ = false;

    struct SeqReordering;

    //! Block Sequence numbers
    std::vector<SeqReordering> seq_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;

    //! called from Multiplexer when there is a new Block on a
    //! Stream.
    void OnStreamBlock(size_t from, uint32_t seq, PinnedBlock&& b);

    void OnStreamBlockOrdered(size_t from, PinnedBlock&& b);

    //! Returns the loopback queue for the worker of this stream.
    BlockQueue * loopback_queue(size_t from_worker_id);
};

// we have two types of CatStream smart pointers: one for internal use in the
// Multiplexer (ordinary CountingPtr), and another for public handles in the
// DIANodes. Once all public handles are deleted, the CatStream is deactivated.
using CatStreamDataPtr = tlx::CountingPtr<CatStreamData>;

using CatStreamSet = StreamSet<CatStreamData>;
using CatStreamSetPtr = tlx::CountingPtr<CatStreamSet>;

//! Ownership handle onto a CatStreamData
class CatStream final : public Stream
{
public:
    using Writer = CatStreamData::Writer;
    using Reader = CatStreamData::Reader;

    using CatReader = CatStreamData::CatReader;

    explicit CatStream(const CatStreamDataPtr& ptr);

    //! When the user handle is destroyed, close the stream (but maybe not
    //! destroy the data object)
    ~CatStream();

    const StreamId& id() const final;

    //! Return stream data reference
    StreamData& data() final;

    //! Return stream data reference
    const StreamData& data() const final;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    Writers GetWriters() final;

    //! Creates a BlockReader for each worker. The BlockReaders are attached to
    //! the BlockQueues in the Stream and wait for further Blocks to arrive or
    //! the Stream's remote close. These Readers _always_ consume!
    std::vector<Reader> GetReaders();

    //! Creates a BlockReader which concatenates items from all workers in
    //! worker rank order. The BlockReader is attached to one \ref
    //! CatBlockSource which includes all incoming queues of this stream.
    CatReader GetCatReader(bool consume);

    //! Open a CatReader (function name matches a method in File and MixStream).
    CatReader GetReader(bool consume);

private:
    CatStreamDataPtr ptr_;
};

using CatStreamPtr = tlx::CountingPtr<CatStream>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CAT_STREAM_HEADER

/******************************************************************************/
