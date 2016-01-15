/*******************************************************************************
 * thrill/data/block.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>

namespace thrill {
namespace data {

/******************************************************************************/
// Block

std::ostream& operator << (std::ostream& os, const Block& b) {
    os << "[Block " << std::hex << &b << std::dec
       << " byte_block_=" << std::hex << b.byte_block_.get() << std::dec;
    if (b.IsValid()) {
        os << " begin_=" << b.begin_
           << " end_=" << b.end_
           << " first_item_=" << b.first_item_
           << " num_items_=" << b.num_items_;
        // << " data=" << common::Hexdump(b.ToString());
    }
    return os << "]";
}

PinnedBlock Block::PinWait(size_t local_worker_id) const {
    std::future<PinnedBlock> pin = Pin(local_worker_id);
    pin.wait();
    return pin.get();
}

std::future<PinnedBlock> Block::Pin(size_t local_worker_id) const {
    return byte_block()->block_pool_->PinBlock(*this, local_worker_id);
}

/******************************************************************************/
// PinnedBlock

std::string PinnedBlock::ToString() const {
    if (!IsValid()) return std::string();
    return std::string(
        reinterpret_cast<const char*>(data_begin()), size());
}

std::ostream& operator << (std::ostream& os, const PinnedBlock& b) {
    os << "[PinnedBlock "
       << " block_=" << static_cast<const Block&>(b);
    if (b.byte_block_)
        os << " pin_count_=" << b.byte_block_->pin_count_str();
    return os << "]";
}

} // namespace data
} // namespace thrill

/******************************************************************************/
