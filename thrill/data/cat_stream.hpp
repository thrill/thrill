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
#include <thrill/data/stream_sink.hpp>

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
class CatStream final : public Stream
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

    using Handle = CatStreamHandle;

    //! Creates a new stream instance
    CatStream(Multiplexer& multiplexer, const StreamId& id,
              size_t local_worker_id, size_t dia_id);

    //! non-copyable: delete copy-constructor
    CatStream(const CatStream&) = delete;
    //! non-copyable: delete assignment operator
    CatStream& operator = (const CatStream&) = delete;
    //! move-constructor: default
    CatStream(CatStream&&) = default;

    ~CatStream() final;

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id);

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<Writer> GetWriters() final;

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

    //! StreamSink objects are receivers of Blocks outbound for other worker.
    std::vector<StreamSink> sinks_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;

    //! called from Multiplexer when there is a new Block on a
    //! Stream.
    void OnStreamBlock(size_t from, PinnedBlock&& b);

    //! called from Multiplexer when a CatStream closed notification was
    //! received.
    void OnCloseStream(size_t from);

    //! Returns the loopback queue for the worker of this stream.
    BlockQueue * loopback_queue(size_t from_worker_id);
};

// we have two types of CatStream smart pointers: one for internal use in the
// Multiplexer (ordinary CountingPtr), and another for public handles in the
// DIANodes. Once all public handles are deleted, the CatStream is deactivated.
using CatStreamIntPtr = tlx::CountingPtr<CatStream>;

using CatStreamSet = StreamSet<CatStream>;
using CatStreamSetPtr = tlx::CountingPtr<CatStreamSet>;

//! Ownership handle onto a CatStream
class CatStreamHandle : public tlx::ReferenceCounter
{
public:
    explicit CatStreamHandle(const CatStreamIntPtr& ptr)
        : ptr_(ptr) { }

    ~CatStreamHandle() {
        ptr_->Close();
    }

    const StreamId& id() const { return ptr_->id(); }

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<CatStream::Writer> GetWriters() {
        return ptr_->GetWriters();
    }

    //! Creates a BlockReader for each worker. The BlockReaders are attached to
    //! the BlockQueues in the Stream and wait for further Blocks to arrive or
    //! the Stream's remote close. These Readers _always_ consume!
    std::vector<CatStream::Reader> GetReaders() {
        return ptr_->GetReaders();
    }

    //! Creates a BlockReader which concatenates items from all workers in
    //! worker rank order. The BlockReader is attached to one \ref
    //! CatBlockSource which includes all incoming queues of this stream.
    CatStream::CatReader GetCatReader(bool consume) {
        return ptr_->GetCatReader(consume);
    }

    //! Open a CatReader (function name matches a method in File and MixStream).
    CatStream::CatReader GetReader(bool consume) {
        return ptr_->GetReader(consume);
    }

    //! shuts the stream down.
    void Close() {
        return ptr_->Close();
    }

    template <typename ItemType>
    void Scatter(File& source, const std::vector<size_t>& offsets,
                 bool consume = false) {
        return ptr_->template Scatter<ItemType>(source, offsets, consume);
    }

private:
    CatStreamIntPtr ptr_;
};

using CatStreamPtr = tlx::CountingPtr<CatStreamHandle>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CAT_STREAM_HEADER

/******************************************************************************/
