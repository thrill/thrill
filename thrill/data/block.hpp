/*******************************************************************************
 * thrill/data/block.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_HEADER
#define THRILL_DATA_BLOCK_HEADER

#include <thrill/data/block_pool.hpp>
#include <thrill/mem/manager.hpp>

#include <cassert>
#include <memory>
#include <ostream>
#include <string>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

//! type of underlying memory area
using Byte = uint8_t;

//! default size of blocks in File, Channel, BlockQueue, etc.
static const size_t default_block_size = 2 * 1024 * 1024;

/*!
 * A ByteBlock is the basic storage units of containers like File, BlockQueue,
 * etc. It consists of a fixed number of bytes without any type and meta
 * information. Conceptually a ByteBlock is written _once_ and can then be
 * shared read-only between containers using shared_ptr<const ByteBlock>
 * reference counting inside a Block, which adds meta information.
 */
class ByteBlock
{
protected:
    struct {
        //! the allocated size of the buffer in bytes, excluding the size_ field
        size_t   size_;

        //! reference to BlockPool for deletion.
        BlockPool* block_pool_;
    } head;

    //! the memory block itself follows here, this is just a placeholder
    Byte data_[1];

    //! Constructor to initialize ByteBlock in a buffer of memory. Protected,
    //! use Allocate() for construction.
    explicit ByteBlock(size_t size, BlockPool* block_pool)
        : head({ size, block_pool }) { }

    //! deleted for shared_ptr<ByteBlock>
    static void deleter(ByteBlock* bb) {
        bb->head.block_pool_->FreeBlock(bb->size());
        operator delete (bb);
    }

public:
    //! Construct a block of given size.
    static std::shared_ptr<ByteBlock> Allocate(
        size_t block_size, BlockPool& block_pool) {
        // this counts only the bytes and excludes the header. why? -tb
        block_pool.AllocateBlock(block_size);

        // allocate a new block of uninitialized memory
        ByteBlock* block =
            static_cast<ByteBlock*>(operator new (sizeof(head) + block_size));

        // initialize block using constructor
        new (block)ByteBlock(block_size, &block_pool);

        // wrap allocated ByteBlock in a shared_ptr. TODO(tb) figure out how to do
        // this whole procedure with std::make_shared.
        return std::shared_ptr<ByteBlock>(block, deleter);
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
};

using ByteBlockPtr = std::shared_ptr<ByteBlock>;
using ByteBlockCPtr = std::shared_ptr<const ByteBlock>;

/**
 * Block combines a reference to a read-only \ref ByteBlock and book-keeping
 * information. The book-keeping meta-information currently is the start of the
 * first item, the ends of the item range, and the number of items in the range.
 *
 * Multiple Block instances can share the same ByteBlock but have different
 * book-keeping / meta- information!
 *
 * <pre>
 *     +--+---------+---------+-------------+---------+-----+
 *     |  |Item1    |Item2    |Item3        |Item4    |Item5|(partial)
 *     +--+---------+---------+-------------+---------+-----+
 *        ^         ^                                       ^
 *        begin     first_item    nitems=5                  end
 * </pre>
 */
class Block
{
public:
    Block() { }

    Block(const ByteBlockCPtr& byte_block,
          size_t begin, size_t end, size_t first_item, size_t nitems)
        : byte_block_(byte_block),
          begin_(begin), end_(end), first_item_(first_item), nitems_(nitems)
    { }

    //! Return whether the enclosed ByteBlock is valid.
    bool IsValid() const { return byte_block_ != nullptr; }

    //! Releases the reference to the ByteBlock and resets book-keeping info
    void Release() {
        byte_block_ = ByteBlockCPtr();
    }

    // Return block as std::string (for debugging), includes eventually cut off
    // elements form the beginning included
    std::string ToString() const {
        return std::string(
            reinterpret_cast<const char*>(data_begin()), size());
    }

    //! access to byte_block_
    const ByteBlockCPtr & byte_block() const { return byte_block_; }

    //! return number of items beginning in this block
    size_t nitems() const { return nitems_; }

    //! accessor to begin_
    void set_begin(size_t i) { begin_ = i; }

    //! accessor to end_
    void set_end(size_t i) { end_ = i; }

    //! return pointer to beginning of valid data
    const Byte * data_begin() const {
        assert(byte_block_);
        return byte_block_->begin() + begin_;
    }

    //! return pointer to end of valid data
    const Byte * data_end() const {
        assert(byte_block_);
        return byte_block_->begin() + end_;
    }

    //! return length of valid data in bytes.
    size_t size() const { return end_ - begin_; }

    //! accessor to first_item_ (absolute in ByteBlock)
    size_t first_item() const { return first_item_; }

    //! return the first_item_offset relative to data_begin().
    size_t first_item_relative() const { return first_item_ - begin_; }

    friend std::ostream&
    operator << (std::ostream& os, const Block& b) {
        os << "[Block " << std::hex << &b << std::dec
           << " byte_block_=" << std::hex << b.byte_block_.get() << std::dec;
        if (b.IsValid()) {
            os << " begin_=" << b.begin_
               << " end_=" << b.end_
               << " first_item_=" << b.first_item_
               << " nitems_=" << b.nitems_;
        }
        return os << "]";
    }

protected:
    //! referenced ByteBlock
    ByteBlockCPtr byte_block_;

    //! beginning offset of valid bytes to read
    size_t begin_;

    //! one byte beyond the end of the valid bytes in the ByteBlock (can be used to
    //! virtually shorten a ByteBlock)
    size_t end_;

    //! offset of first valid element in the ByteBlock in absolute bytes from
    //! byte_block_->begin().
    size_t first_item_;

    //! number of valid items that _start_ in this block (includes cut-off
    //! element at the end)
    size_t nitems_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_HEADER

/******************************************************************************/
