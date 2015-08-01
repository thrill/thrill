/*******************************************************************************
 * c7a/data/block.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_HEADER
#define C7A_DATA_BLOCK_HEADER

#include <cassert>
#include <memory>
#include <ostream>
#include <string>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

//! type of underlying memory area
using Byte = uint8_t;

//! default size of blocks in File, Channel, BlockQueue, etc.
static const size_t default_block_size = 2 * 1024 * 1024;

/*!
 * A Block is the basic storage units of containers like File, BlockQueue,
 * etc. It consists of a fixed number of bytes without any type and meta
 * information. Conceptually a Block is written _once_ and can then be shared
 * read-only between containers using shared_ptr<const Block> reference
 * counting.
 */
class Block
{
protected:
    //! the allocated size of the buffer in bytes, excluding the size_ field.
    size_t size_;

    //! the memory block itself follows here, this is just a placeholder
    Byte data_[1];

    //! Constructor to initialize Block in a buffer of memory. Protected, use
    //! Allocate() for construction.
    explicit Block(size_t size) : size_(size) { }

public:
    //! Construct a block of given size.
    static std::shared_ptr<Block> Allocate(size_t block_size) {
        // allocate a new block of uninitialized memory
        Block* block =
            static_cast<Block*>(operator new (sizeof(size_t) + block_size));

        // initialize block using constructor
        new (block)Block(block_size);

        // wrap allocated Block in a shared_ptr. TODO(tb) figure out how to do
        // this whole procedure with std::make_shared.
        return std::shared_ptr<Block>(block);
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
    Byte * end() { return data_ + size_; }
    //! const data accessor beyond end of memory block
    const Byte * end() const { return data_ + size_; }

    //! the block size
    size_t size() const { return size_; }
};

using BlockPtr = std::shared_ptr<Block>;
using BlockCPtr = std::shared_ptr<const Block>;

/**
 * VirtualBlock combines a reference to a read-only \ref Block and book-keeping
 * information. The book-keeping metainformation currently is the start of the
 * first item, the ends of the item range, and the number of items in the range.
 *
 * Multiple VirtualBlock instances can share the same Block but have different
 * book-keeping information!
 *
 * <pre>
 *     +--+---------+---------+-------------+---------+-----+
 *     |  |Item1    |Item2    |Item3        |Item4    |Item5|(partial)
 *     +--+---------+---------+-------------+---------+-----+
 *        ^         ^                                       ^
 *        begin     first_item    nitems=5                  end
 * </pre>
 */
class VirtualBlock
{
public:
    VirtualBlock() { }

    VirtualBlock(const BlockCPtr& block,
                 size_t begin, size_t end, size_t first_item, size_t nitems)
        : block_(block),
          begin_(begin), end_(end), first_item_(first_item), nitems_(nitems)
    { }

    //! Return whether the enclosed block is valid.
    bool IsValid() const { return block_ != nullptr; }

    //! Releases the reference to the block and resets book-keeping info
    void Release() {
        block_ = BlockCPtr();
    }

    // Return virtual block as std::string (for debugging), includes eventually
    // cut off elements form the beginning included
    std::string ToString() const {
        return std::string(
            reinterpret_cast<const char*>(data_begin()), size());
    }

    //! access to block_
    const BlockCPtr & block() const { return block_; }

    //! return number of items beginning in this block
    size_t nitems() const { return nitems_; }

    //! accessor to begin_
    void set_begin(size_t i) { begin_ = i; }

    //! accessor to end_
    void set_end(size_t i) { end_ = i; }

    //! return pointer to beginning of valid data
    const Byte * data_begin() const {
        assert(block_);
        return block_->begin() + begin_;
    }

    //! return pointer to end of valid data
    const Byte * data_end() const {
        assert(block_);
        return block_->begin() + end_;
    }

    //! return length of valid data in bytes.
    size_t size() const { return end_ - begin_; }

    //! accessor to first_item_ (absolute in block)
    size_t first_item() const { return first_item_; }

    //! return the first_item_offset relative to data_begin().
    size_t first_item_relative() const { return first_item_ - begin_; }

    friend std::ostream&
    operator << (std::ostream& os, const VirtualBlock& c) {
        os << "[VirtualBlock " << std::hex << &c << std::dec
           << " block_=" << std::hex << c.block_.get() << std::dec;
        if (c.IsValid()) {
            os << " begin_=" << c.begin_
               << " end_=" << c.end_
               << " first_item_=" << c.first_item_
               << " nitems_=" << c.nitems_;
        }
        return os << "]";
    }

protected:
    //! referenced block
    BlockCPtr block_;

    //! beginning offset of valid bytes to read
    size_t begin_;

    //! one byte beyond the end of the valid bytes in the block (can be used to
    //! virtually shorten a block)
    size_t end_;

    //! offset of first valid element in the block in absolute bytes from
    //! block_->begin().
    size_t first_item_;

    //! number of valid items that _start_ in this block (includes cut-off
    //! element at the end)
    size_t nitems_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_HEADER

/******************************************************************************/
