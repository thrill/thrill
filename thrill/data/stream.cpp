/*******************************************************************************
 * thrill/data/stream.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/stream.hpp>

namespace thrill {
namespace data {

Stream::Stream(Multiplexer& multiplexer, const StreamId& id,
               size_t local_worker_id, size_t dia_id)
    : id_(id),
      local_worker_id_(local_worker_id),
      dia_id_(dia_id),
      multiplexer_(multiplexer),
      remaining_closing_blocks_((num_hosts() - 1) * workers_per_host())
{ }

Stream::~Stream() { }

void Stream::OnAllClosed() {
    multiplexer_.logger()
        << "class" << "Stream"
        << "event" << "close"
        << "id" << id_
        << "dia_id" << dia_id_
        << "worker_rank"
        << (my_host_rank() * multiplexer_.workers_per_host())
        + local_worker_id_
        << "rx_bytes" << rx_bytes_
        << "rx_blocks" << rx_blocks_
        << "tx_bytes" << tx_bytes_
        << "tx_blocks" << tx_blocks_;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
