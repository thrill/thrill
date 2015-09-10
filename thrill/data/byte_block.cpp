/*******************************************************************************
 * thrill/data/byte_block.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
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

ByteBlock::ByteBlock(Byte* data, size_t size, BlockPool* block_pool, bool pinned, size_t swap_token)
    : data_(data), size_(size), block_pool_(block_pool), pin_count_(pinned ? 1 : 0), swap_token_(swap_token) { }

using ByteBlockPtr = ByteBlock::ByteBlockPtr;
}
}

/******************************************************************************/
