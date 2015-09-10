/*******************************************************************************
 * thrill/data/byte_block.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BYTE_BLOCK_HEADER
#define THRILL_DATA_BYTE_BLOCK_HEADER
#include <thrill/common/counting_ptr.hpp>

namespace thrill {
namespace data {

//! default size of blocks in File, Channel, BlockQueue, etc.
static const size_t default_block_size = 2 * 1024 * 1024;

//! type of underlying memory area
using Byte = uint8_t;

// forward declaration (definition further below)
class BlockPool;
/*!
 * A ByteBlock is the basic storage units of containers like File, BlockQueue,
 * etc. It consists of a fixed number of bytes without any type and meta
 * information. Conceptually a ByteBlock is written _once_ and can then be
 * shared read-only between containers using shared_ptr<const ByteBlock>
 * reference counting inside a Block, which adds meta information.
 *
 * ByteBlocks can be swapped to disk, which decreases their size to 0.
 */
class ByteBlock : public common::ReferenceCount
{
public:
    //! deleter for CountingPtr<ByteBlock>
    static void deleter(ByteBlock* bb);
    static void deleter(const ByteBlock* bb);

    using ByteBlockPtr = common::CountingPtr<ByteBlock, deleter>;
    using ByteBlockCPtr = common::CountingPtr<const ByteBlock, deleter>;

protected:
    //! the memory block itself is referenced as it is in a a separate memory
    //! region that can be swapped out
    Byte* data_;

    //! the allocated size of the buffer in bytes, excluding the size_ field
    size_t   size_;

    //! reference to BlockPool for deletion.
    BlockPool* block_pool_;

    //! counts the number of pins in this block
    //! this is not atomic since a) head would not be a POD and
    //! b) the count is only modified by BlockPool which is thread-safe
    size_t   pin_count_;

    //! token that is used with mem::PageMapper
    size_t   swap_token_;


    // BlockPool is a friend to modify the head's pin_count_
    friend class BlockPool;

    //! Constructor to initialize ByteBlock in a buffer of memory. Protected,
    //! use BlockPoolAllocate() for construction.
    //!\param memory the memory address of the byte-blocks data. nullptr if swapped out
    //!\param size the size of the block in bytes
    //!\param block_pool the block pool that manages this ByteBlock
    //!\param pinned whether the block was created in pinned state
    explicit ByteBlock(Byte* memory, size_t size, BlockPool* block_pool, bool pinned, size_t swap_token);

    //! No default construction of Byteblock
    ByteBlock() = delete;

public:
    //! mutable data accessor to memory block
    Byte * data() { return data_; }
    //! const data accessor to memory block
    const Byte * data() const { return data_; }

    //! mutable data accessor to beginning of memory block
    Byte * begin() { return data_; }
    //! const data accessor to beginning of memory block
    const Byte * begin() const { return data_; }

    //! mutable data accessor beyond end of memory block
    Byte * end() { return data_ + size_; }
    //! const data accessor beyond end of memory block
    const Byte * end() const { return data_ + size_; }

    //! the block size
    size_t size() const { return size_; }

    //! true if block resides in memory
    bool in_memory() const {
        return data_ != nullptr;
    }
};

#endif // !THRILL_DATA_BYTE_BLOCK_HEADER
using ByteBlockPtr = ByteBlock::ByteBlockPtr;
} //namespace data
} //namespace thrill

/******************************************************************************/
