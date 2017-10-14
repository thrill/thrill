/*******************************************************************************
 * thrill/data/mix_stream.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MIX_STREAM_HEADER
#define THRILL_DATA_MIX_STREAM_HEADER

#include <thrill/data/mix_block_queue.hpp>
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
 * outbound Stream. The vector is of size of workers in the system.  One can
 * then write items destined to the corresponding worker. The written items are
 * buffered into a Block and only sent when the Block is full. To force a send,
 * use BlockWriter::Flush(). When all items are sent, the BlockWriters **must**
 * be closed using BlockWriter::Close().
 *
 * The MixStream allows reading of items from all workers in an unordered
 * sequence, without waiting for any of the workers to complete sending items.
 */
class MixStream final : public Stream
{
    static constexpr bool debug = false;

public:
    using MixReader = MixBlockQueueReader;

    using Handle = MixStreamHandle;

    //! Creates a new stream instance
    MixStream(Multiplexer& multiplexer, const StreamId& id,
              size_t local_worker_id, size_t dia_id);

    //! non-copyable: delete copy-constructor
    MixStream(const MixStream&) = delete;
    //! non-copyable: delete assignment operator
    MixStream& operator = (const MixStream&) = delete;
    //! move-constructor: default
    MixStream(MixStream&&) = default;

    ~MixStream() final;

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id);

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<Writer> GetWriters() final;

    //! Creates a BlockReader which mixes items from all workers.
    MixReader GetMixReader(bool consume);

    //! Open a MixReader (function name matches a method in File and CatStream).
    MixReader GetReader(bool consume);

    //! shuts the stream down.
    void Close() final;

    //! Indicates if the stream is closed - meaning all remaining outbound
    //! queues have been closed.
    bool closed() const final;

private:
    //! flag if Close() was completed
    bool is_closed_ = false;

    //! StreamSink objects are receivers of Blocks outbound for other worker.
    std::vector<StreamSink> sinks_;

    //! BlockQueue to store incoming Blocks with source.
    MixBlockQueue queue_;

    //! vector of MixBlockQueueSink which serve as loopback BlockSinks into
    //! the MixBlockQueue
    std::vector<MixBlockQueueSink> loopback_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;
    friend class MixBlockQueueSink;

    //! called from Multiplexer when there is a new Block for this Stream.
    void OnStreamBlock(size_t from, PinnedBlock&& b);

    //! called from Multiplexer when a MixStream closed notification was
    //! received.
    void OnCloseStream(size_t from);

    //! Returns the loopback queue for the worker of this stream.
    MixBlockQueueSink * loopback_queue(size_t from_worker_id);
};

// we have two types of MixStream smart pointers: one for internal use in the
// Multiplexer (ordinary CountingPtr), and another for public handles in the
// DIANodes. Once all public handles are deleted, the MixStream is deactivated.
using MixStreamIntPtr = tlx::CountingPtr<MixStream>;

using MixStreamSet = StreamSet<MixStream>;
using MixStreamSetPtr = tlx::CountingPtr<MixStreamSet>;

//! Ownership handle onto a MixStream
class MixStreamHandle : public tlx::ReferenceCounter
{
public:
    explicit MixStreamHandle(const MixStreamIntPtr& ptr)
        : ptr_(ptr) { }

    ~MixStreamHandle() {
        ptr_->Close();
    }

    const StreamId& id() const { return ptr_->id(); }

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<MixStream::Writer> GetWriters() {
        return ptr_->GetWriters();
    }

    //! Creates a BlockReader which concatenates items from all workers in
    //! worker rank order. The BlockReader is attached to one \ref
    //! MixBlockSource which includes all incoming queues of this stream.
    MixStream::MixReader GetMixReader(bool consume) {
        return ptr_->GetMixReader(consume);
    }

    //! Open a MixReader (function name matches a method in File and CatStream).
    MixStream::MixReader GetReader(bool consume) {
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
    MixStreamIntPtr ptr_;
};

using MixStreamPtr = tlx::CountingPtr<MixStreamHandle>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIX_STREAM_HEADER

/******************************************************************************/
