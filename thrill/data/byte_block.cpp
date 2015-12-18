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

namespace thrill {
namespace data {

ByteBlock::ByteBlock(Byte* data, size_t size, BlockPool* block_pool)
    : data_(data), size_(size), block_pool_(block_pool), pin_count_(0)
{ }

void ByteBlock::deleter(ByteBlock* bb) {
    sLOG << "ByteBlock::deleter() pin_count_" << bb->pin_count_;
    assert(bb->pin_count_ == 0);

    if (bb->reference_count() == 0) {
        assert(bb->block_pool_);
        bb->block_pool_->DestroyBlock(bb);
    }
    operator delete (bb);
}

void ByteBlock::deleter(const ByteBlock* bb) {
    return deleter(const_cast<ByteBlock*>(bb));
}

void ByteBlock::IncPinCount(size_t local_worker_id) {
    size_t p = ++pin_count_;
    LOG << "ByteBlock::IncPinCount() ++pin_count=" << p
        << " local_worker_id=" << local_worker_id;
}

void ByteBlock::DecPinCount(size_t local_worker_id) {
    size_t p = --pin_count_;
    LOG << "ByteBlock::DecPinCount() --pin_count=" << p
        << " local_worker_id=" << local_worker_id;
    if (p == 0) {
        // TODO(tb): this is a race condition: pin was zero and putting it into
        // blockpool's list must be an atomic operation, which cannot be
        // interrupted by another thread raising the pin again.
        block_pool_->UnpinBlock(this, local_worker_id);
    }
}

} // namespace data
} // namespace thrill

/******************************************************************************/
