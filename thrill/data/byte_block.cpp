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
    assert(bb->head.pin_count_ == 0);

    // some blocks are created in 'detached' state (tests etc)
    if (bb->head.block_pool_ && bb->reference_count() == 0) {
        bb->head.block_pool_->FreeBlockMemory(bb->size());
        bb->head.block_pool_->DestroyBlock(bb);
    }
    operator delete (bb);
}
void ByteBlock::deleter(const ByteBlock* bb) {
    return deleter(const_cast<ByteBlock*>(bb));
}

ByteBlock::ByteBlock(size_t size, BlockPool* block_pool, bool pinned)
    : head({
               size, block_pool, 0, pinned
           }) { }

//! Construct a block of given size.
ByteBlockPtr ByteBlock::Allocate(
    size_t block_size, BlockPool* block_pool) {
    // this counts only the bytes and excludes the header. why? -tb
    block_pool->ClaimBlockMemory(block_size);

    // allocate a new block of uninitialized memory
    ByteBlock* block =
        static_cast<ByteBlock*>(
            operator new (
                sizeof(common::ReferenceCount) + sizeof(head) + block_size));

    // initialize block using constructor
    new (block)ByteBlock(block_size, block_pool);

    // wrap allocated ByteBlock in a shared_ptr. TODO(tb) figure out how to do
    // this whole procedure with std::make_shared.
    return ByteBlockPtr(block);
}

using ByteBlockPtr = ByteBlock::ByteBlockPtr;
}
}

/******************************************************************************/
