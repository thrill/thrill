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
class MixStreamData final : public StreamData
{
    static constexpr bool debug = false;

public:
    using MixReader = MixBlockQueueReader;

    using Handle = MixStream;

    //! Creates a new stream instance
    MixStreamData(StreamSetBase* stream_set_base,
                  Multiplexer& multiplexer, size_t send_size_limit,
                  const StreamId& id, size_t local_worker_id, size_t dia_id);

    //! non-copyable: delete copy-constructor
    MixStreamData(const MixStreamData&) = delete;
    //! non-copyable: delete assignment operator
    MixStreamData& operator = (const MixStreamData&) = delete;
    //! move-constructor: default
    MixStreamData(MixStreamData&&) = default;

    ~MixStreamData() final;

    //! return stream type string
    const char * stream_type() final;

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id);

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    Writers GetWriters() final;

    //! Creates a BlockReader which mixes items from all workers.
    MixReader GetMixReader(bool consume);

    //! Open a MixReader (function name matches a method in File and CatStream).
    MixReader GetReader(bool consume);

    //! shuts the stream down.
    void Close() final;

    //! Indicates if the stream is closed - meaning all remaining outbound
    //! queues have been closed.
    bool closed() const final;

    //! check if inbound queue is closed
    bool is_queue_closed(size_t from);

private:
    //! flag if Close() was completed
    bool is_closed_ = false;

    struct SeqReordering {
        //! current top sequence number
        uint32_t                  seq_ = 0;

        //! queue of waiting Blocks, ordered by sequence number
        std::map<uint32_t, Block> waiting_;
    };

    //! Block Sequence numbers
    std::vector<SeqReordering> seq_;

    //! BlockQueue to store incoming Blocks with source.
    MixBlockQueue queue_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;
    friend class StreamSink;

    //! called from Multiplexer when there is a new Block for this Stream.
    void OnStreamBlock(size_t from, uint32_t seq, Block&& b);

    //! called to process PinnedBlock in sequence
    void OnStreamBlockOrdered(size_t from, Block&& b);
};

// we have two types of MixStream smart pointers: one for internal use in the
// Multiplexer (ordinary CountingPtr), and another for public handles in the
// DIANodes. Once all public handles are deleted, the MixStream is deactivated.
using MixStreamDataPtr = tlx::CountingPtr<MixStreamData>;

using MixStreamSet = StreamSet<MixStreamData>;
using MixStreamSetPtr = tlx::CountingPtr<MixStreamSet>;

//! Ownership handle onto a MixStream
class MixStream final : public Stream
{
public:
    using Writer = MixStreamData::Writer;

    using MixReader = MixStreamData::MixReader;

    explicit MixStream(const MixStreamDataPtr& ptr);

    //! When the user handle is destroyed, close the stream (but maybe not
    //! destroy the data object)
    ~MixStream();

    //! Return stream id
    const StreamId& id() const final;

    //! Return stream data reference
    StreamData& data() final;

    //! Return stream data reference
    const StreamData& data() const final;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    Writers GetWriters();

    //! Creates a BlockReader which concatenates items from all workers in an
    //! arbitrary order.
    MixReader GetMixReader(bool consume);

    //! Open a MixReader (function name matches a method in File and CatStream).
    MixReader GetReader(bool consume);

private:
    MixStreamDataPtr ptr_;
};

using MixStreamPtr = tlx::CountingPtr<MixStream>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIX_STREAM_HEADER

/******************************************************************************/
