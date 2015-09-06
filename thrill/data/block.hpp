/*******************************************************************************
 * thrill/data/block.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_HEADER
#define THRILL_DATA_BLOCK_HEADER

#include <thrill/common/counting_ptr.hpp>
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

//! default size of blocks in File, Channel, BlockQueue, etc.
static const size_t default_block_size = 2 * 1024 * 1024;

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
 *        begin     first_item    num_items=5               end
 * </pre>
 */
class Block
{
public:
    Block() { }

    Block(const ByteBlockPtr& byte_block,
          size_t begin, size_t end, size_t first_item, size_t num_items)
        : byte_block_(byte_block),
          begin_(begin), end_(end),
          first_item_(first_item), num_items_(num_items)
    { }

    //! Return whether the enclosed ByteBlock is valid.
    bool IsValid() const {
        return byte_block_;
    }

    //! Releases the reference to the ByteBlock and resets book-keeping info
    void Release() {
        byte_block_ = ByteBlockPtr();
    }

    // Return block as std::string (for debugging), includes eventually cut off
    // elements form the beginning included
    std::string ToString() const {
        return std::string(
            reinterpret_cast<const char*>(data_begin()), size());
    }

    //! access to byte_block_
    const ByteBlockPtr & byte_block() const { return byte_block_; }

    //! access to byte_block_ (mutable)
    ByteBlockPtr & byte_block() { return byte_block_; }

    //! return number of items beginning in this block
    size_t num_items() const { return num_items_; }

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
    size_t first_item_absolute() const { return first_item_; }

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
               << " num_items_=" << b.num_items_;
        }
        return os << "]";
    }

protected:
    //! referenced ByteBlock
    ByteBlockPtr byte_block_;

    //! beginning offset of valid bytes to read
    size_t begin_;

    //! one byte beyond the end of the valid bytes in the ByteBlock (can be used
    //! to virtually shorten a ByteBlock)
    size_t end_;

    //! offset of first valid element in the ByteBlock in absolute bytes from
    //! byte_block_->begin().
    size_t first_item_;

    //! number of valid items that _start_ in this block (includes cut-off
    //! element at the end)
    size_t num_items_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_HEADER

/******************************************************************************/
