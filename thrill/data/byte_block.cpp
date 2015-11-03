/*******************************************************************************
 * thrill/data/byte_block.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/block_pool.hpp>
#include <thrill/data/byte_block.hpp>

namespace thrill {
namespace data {

void ByteBlock::deleter(ByteBlock* bb) {
    assert(bb->pin_count_ == 0);

    // some blocks are created in 'detached' state (tests etc)
    if (bb->block_pool_ && bb->reference_count() == 0) {
        bb->block_pool_->DestroyBlock(bb);
    }
    operator delete (bb);
}
void ByteBlock::deleter(const ByteBlock* bb) {
    return deleter(const_cast<ByteBlock*>(bb));
}

void ByteBlock::Pin(common::delegate<void()>&& callback) {
    block_pool_->PinBlock(this, std::move(callback));
}

void ByteBlock::DecreasePinCount() {
    block_pool_->UnpinBlock(this);
}

void ByteBlock::IncreasePinCount() {
    pin_count_++;
}

ByteBlock::ByteBlock(Byte* data, size_t size, BlockPool* block_pool, bool pinned, size_t swap_token)
    : data_(data), size_(size), block_pool_(block_pool), pin_count_(pinned ? 1 : 0), swap_token_(swap_token) { }

using ByteBlockPtr = ByteBlock::ByteBlockPtr;

} // namespace data
} // namespace thrill

/******************************************************************************/
