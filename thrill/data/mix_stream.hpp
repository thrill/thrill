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
public:
    using MixReader = MixBlockQueueReader;

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
    std::vector<Writer>
    GetWriters(size_t block_size = default_block_size) final;

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
    static constexpr bool debug = false;

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

using MixStreamPtr = common::CountingPtr<MixStream>;

using MixStreamSet = StreamSet<MixStream>;
using MixStreamSetPtr = common::CountingPtr<MixStreamSet>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIX_STREAM_HEADER

/******************************************************************************/
