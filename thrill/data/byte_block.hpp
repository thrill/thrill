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
    struct {
        //! the allocated size of the buffer in bytes, excluding the size_ field
        size_t   size_;

        //! reference to BlockPool for deletion.
        BlockPool* block_pool_;

        //! counts the number of pins in this block
        //! this is not atomic since a) head would not be a POD and
        //! b) the count is only modified by BlockPool which is thread-safe
        size_t   pin_count_;

        //! Indicates that block resides out of memory (on disk)
        bool     swapped_out_;
    } head;

    //! the memory block itself follows here, this is just a placeholder
    Byte data_[1];

    // BlockPool is a friend to modify the head's pin_count_
    friend class BlockPool;

    //! Constructor to initialize ByteBlock in a buffer of memory. Protected,
    //! use BlockPoolAllocate() for construction.
    explicit ByteBlock(size_t size, BlockPool* block_pool, bool pinned = false);

    //! Construct a block of given size.
    static ByteBlock* Allocate(
        size_t block_size, BlockPool* block_pool, bool pinned = false);

    //! No default construction of Byteblock
    ByteBlock() = delete;

public:
    //! Construct a block of given size WIHTOUT pool management
    //! Do this only when the block should not be accounted by memory
    //! management. Use only recommended for tests
    static ByteBlock* Allocate(size_t block_size) {
        return Allocate(block_size, nullptr);
    }

    //! mutable data accessor to memory block
    Byte * data() { return data_; }
    //! const data accessor to memory block
    const Byte * data() const { return data_; }

    //! mutable data accessor to beginning of memory block
    Byte * begin() { return data_; }
    //! const data accessor to beginning of memory block
    const Byte * begin() const { return data_; }

    //! mutable data accessor beyond end of memory block
    Byte * end() { return data_ + head.size_; }
    //! const data accessor beyond end of memory block
    const Byte * end() const { return data_ + head.size_; }

    //! the block size
    size_t size() const { return head.size_; }

    //! indicates whether this block is backed to disk
    bool swapable() const { return head.size_ == default_block_size - sizeof(common::ReferenceCount) - sizeof(head); }
};

#endif // !THRILL_DATA_BYTE_BLOCK_HEADER
using ByteBlockPtr = ByteBlock::ByteBlockPtr;
} //namespace data
} //namespace thrill

/******************************************************************************/
