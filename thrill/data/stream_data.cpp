/*******************************************************************************
 * thrill/data/stream_data.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/stream.hpp>

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>

namespace thrill {
namespace data {

/******************************************************************************/
// StreamData

StreamData::StreamData(Multiplexer& multiplexer, const StreamId& id,
                       size_t local_worker_id, size_t dia_id)
    : sem_queue_(16 * 1024 * 1024),
      id_(id),
      local_worker_id_(local_worker_id),
      dia_id_(dia_id),
      multiplexer_(multiplexer)
{ }

StreamData::~StreamData() = default;

void StreamData::OnAllClosed(const char* stream_type) {
    multiplexer_.logger()
        << "class" << "StreamData"
        << "event" << "close"
        << "id" << id_
        << "type" << stream_type
        << "dia_id" << dia_id_
        << "worker_rank"
        << (my_host_rank() * multiplexer_.workers_per_host())
        + local_worker_id_
        << "rx_net_items" << rx_net_items_
        << "rx_net_bytes" << rx_net_bytes_
        << "rx_net_blocks" << rx_net_blocks_
        << "tx_net_items" << tx_net_items_
        << "tx_net_bytes" << tx_net_bytes_
        << "tx_net_blocks" << tx_net_blocks_
        << "rx_int_items" << rx_int_items_
        << "rx_int_bytes" << rx_int_bytes_
        << "rx_int_blocks" << rx_int_blocks_
        << "tx_int_items" << tx_int_items_
        << "tx_int_bytes" << tx_int_bytes_
        << "tx_int_blocks" << tx_int_blocks_;
}

/******************************************************************************/
// StreamData::Writers

StreamData::Writers::Writers(size_t my_worker_rank)
    : my_worker_rank_(my_worker_rank)
{ }

void StreamData::Writers::Close() {
    // close BlockWriters in a cyclic fashion
    size_t s = size();
    for (size_t i = 0; i < s; ++i) {
        operator [] ((i + my_worker_rank_) % s).Close();
    }
}

StreamData::Writers::~Writers() {
    Close();
}

} // namespace data
} // namespace thrill

/******************************************************************************/
