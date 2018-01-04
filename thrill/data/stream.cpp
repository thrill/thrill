/*******************************************************************************
 * thrill/data/stream.cpp
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
// Stream

Stream::~Stream()
{ }

void Stream::Close() {
    return data().Close();
}

/*----------------------------------------------------------------------------*/

size_t Stream::tx_items() const {
    return tx_net_items() + tx_int_items();
}

size_t Stream::tx_bytes() const {
    return tx_net_bytes() + tx_int_bytes();
}

size_t Stream::tx_blocks() const {
    return tx_net_blocks() + tx_int_blocks();
}

size_t Stream::rx_items() const {
    return rx_net_items() + rx_int_items();
}

size_t Stream::rx_bytes() const {
    return rx_net_bytes() + rx_int_bytes();
}

size_t Stream::rx_blocks() const {
    return rx_net_blocks() + rx_int_blocks();
}

/*----------------------------------------------------------------------------*/

size_t Stream::tx_net_items() const {
    return data().tx_net_items_;
}

size_t Stream::tx_net_bytes() const {
    return data().tx_net_bytes_;
}

size_t Stream::tx_net_blocks() const {
    return data().tx_net_blocks_;
}

size_t Stream::rx_net_items() const {
    return data().rx_net_items_;
}

size_t Stream::rx_net_bytes() const {
    return data().rx_net_bytes_;
}

size_t Stream::rx_net_blocks() const {
    return data().rx_net_blocks_;
}

/*----------------------------------------------------------------------------*/

size_t Stream::tx_int_items() const {
    return data().tx_int_items_;
}

size_t Stream::tx_int_bytes() const {
    return data().tx_int_bytes_;
}

size_t Stream::tx_int_blocks() const {
    return data().tx_int_blocks_;
}

size_t Stream::rx_int_items() const {
    return data().rx_int_items_;
}

size_t Stream::rx_int_bytes() const {
    return data().rx_int_bytes_;
}

size_t Stream::rx_int_blocks() const {
    return data().rx_int_blocks_;
}

/*----------------------------------------------------------------------------*/

} // namespace data
} // namespace thrill

/******************************************************************************/
