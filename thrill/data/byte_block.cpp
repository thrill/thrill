/*******************************************************************************
 * thrill/data/byte_block.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/block_pool.hpp>
#include <thrill/data/byte_block.hpp>

#include <sstream>
#include <string>

namespace thrill {
namespace data {

ByteBlock::ByteBlock(Byte* data, size_t size, BlockPool* block_pool)
    : data_(data), size_(size),
      block_pool_(block_pool),
      pin_count_(block_pool_->workers_per_host())
{ }

void ByteBlock::deleter(ByteBlock* bb) {
    sLOG << "ByteBlock::deleter() pin_count_" << bb->pin_count_str();
    assert(bb->total_pins_ == 0);

    if (bb->reference_count() == 0) {
        assert(bb->block_pool_);
        bb->block_pool_->DestroyBlock(bb);
    }
    operator delete (bb);
}

void ByteBlock::deleter(const ByteBlock* bb) {
    return deleter(const_cast<ByteBlock*>(bb));
}

std::string ByteBlock::pin_count_str() const {
    return "[" + common::Join(",", pin_count_) + "]";
}

void ByteBlock::IncPinCount(size_t local_worker_id) {
    return block_pool_->IncBlockPinCount(this, local_worker_id);
}

void ByteBlock::DecPinCount(size_t local_worker_id) {
    return block_pool_->DecBlockPinCount(this, local_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
