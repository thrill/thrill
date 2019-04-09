/*******************************************************************************
 * thrill/data/stream.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_STREAM_HEADER
#define THRILL_DATA_STREAM_HEADER

#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/stream_data.hpp>
#include <thrill/data/stream_sink.hpp>

#include <mutex>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

/******************************************************************************/
//! Stream - base class for CatStream and MixStream

class Stream : public tlx::ReferenceCounter
{
public:
    using Writer = StreamData::Writer;
    using Writers = StreamData::Writers;

    virtual ~Stream();

    //! Return stream id
    virtual const StreamId& id() const = 0;

    //! Return stream data reference
    virtual StreamData& data() = 0;

    //! Return stream data reference
    virtual const StreamData& data() const = 0;

    //! shuts down the stream, waits for all closing blocks
    void Close();

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    virtual Writers GetWriters() = 0;

    /*!
     * Scatters a File to many worker: elements from [offset[0],offset[1]) are
     * sent to the first worker, elements from [offset[1], offset[2]) are sent
     * to the second worker, ..., elements from [offset[my_rank -
     * 1],offset[my_rank]) are copied locally, ..., elements from
     * [offset[num_workers - 1], offset[num_workers]) are sent to the last
     * worker.
     *
     * The number of given offsets must be equal to the
     * net::Group::num_hosts() * workers_per_host_ + 1.
     *
     * /param source File containing the data to be scattered.
     *
     * /param offsets - as described above. offsets.size must be equal to
     * num_workers + 1
     */
    template <typename ItemType>
    void Scatter(File& source, const std::vector<size_t>& offsets,
                 bool consume = false) {
        if (consume)
            return ScatterConsume<ItemType>(source, offsets);
        else
            return ScatterKeep<ItemType>(source, offsets);
    }

    /*!
     * Consuming Version of Scatter() see documentation there.
     */
    template <typename ItemType>
    void ScatterConsume(File& source, const std::vector<size_t>& offsets) {

        File::ConsumeReader reader =
            source.GetConsumeReader(/* prefetch_size */ 0);
        size_t current = 0;

        {
            // discard first items in Reader
            size_t limit = offsets[0];
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // discard one item (with deserialization)
                reader.template Next<ItemType>();
            }
#else
            if (current != limit) {
                reader.template GetItemBatch<ItemType>(limit - current);
                current = limit;
            }
#endif
        }

        Writers writers = GetWriters();

        size_t num_workers = writers.size();
        assert(offsets.size() == num_workers + 1);

        for (size_t worker = 0; worker < num_workers; ++worker) {
            // write [current,limit) to this worker
            size_t limit = offsets[worker + 1];
            assert(current <= limit);
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // move over one item (with deserialization and serialization)
                writers[worker](reader.template Next<ItemType>());
            }
#else
            if (current != limit) {
                writers[worker].AppendBlocks(
                    reader.template GetItemBatch<ItemType>(limit - current));
                current = limit;
            }
#endif
            writers[worker].Close();
        }
    }

    /*!
     * Keep Version of Scatter() see documentation there.
     */
    template <typename ItemType>
    void ScatterKeep(File& source, const std::vector<size_t>& offsets) {

        File::KeepReader reader = source.GetKeepReader(/* prefetch_size */ 0);
        size_t current = 0;

        {
            // discard first items in Reader
            size_t limit = offsets[0];
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // discard one item (with deserialization)
                reader.template Next<ItemType>();
            }
#else
            if (current != limit) {
                reader.template GetItemBatch<ItemType>(limit - current);
                current = limit;
            }
#endif
        }

        Writers writers = GetWriters();

        size_t num_workers = writers.size();
        assert(offsets.size() == num_workers + 1);

        for (size_t worker = 0; worker < num_workers; ++worker) {
            // write [current,limit) to this worker
            size_t limit = offsets[worker + 1];
            assert(current <= limit);
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // move over one item (with deserialization and serialization)
                writers[worker](reader.template Next<ItemType>());
            }
#else
            if (current != limit) {
                writers[worker].AppendBlocks(
                    reader.template GetItemBatch<ItemType>(limit - current));
                current = limit;
            }
#endif
            writers[worker].Close();
        }
    }

    /**************************************************************************/

    //! \name Statistics
    //! \{

    //! return number of items transmitted
    size_t tx_items() const;

    //! return number of bytes transmitted
    size_t tx_bytes() const;

    //! return number of blocks transmitted
    size_t tx_blocks() const;

    //! return number of items received
    size_t rx_items() const;

    //! return number of bytes received
    size_t rx_bytes() const;

    //! return number of blocks received
    size_t rx_blocks() const;

    /*------------------------------------------------------------------------*/

    //! return number of items transmitted via network excluding internal tx
    size_t tx_net_items() const;

    //! return number of bytes transmitted via network excluding internal tx
    size_t tx_net_bytes() const;

    //! return number of blocks transmitted via network excluding internal tx
    size_t tx_net_blocks() const;

    //! return number of items received via network excluding internal tx
    size_t rx_net_items() const;

    //! return number of bytes received via network excluding internal tx
    size_t rx_net_bytes() const;

    //! return number of blocks received via network excluding internal tx
    size_t rx_net_blocks() const;

    /*------------------------------------------------------------------------*/

    //! return number of items transmitted via internal loopback queues
    size_t tx_int_items() const;

    //! return number of bytes transmitted via internal loopback queues
    size_t tx_int_bytes() const;

    //! return number of blocks transmitted via internal loopback queues
    size_t tx_int_blocks() const;

    //! return number of items received via network internal loopback queues
    size_t rx_int_items() const;

    //! return number of bytes received via network internal loopback queues
    size_t rx_int_bytes() const;

    //! return number of blocks received via network internal loopback queues
    size_t rx_int_blocks() const;

    //! \}
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_HEADER

/******************************************************************************/
