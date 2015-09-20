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
#include <thrill/common/future.hpp>
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
    Block() : pinned_(false) { }

    //! Creates a block that points to the given data::ByteBlock with the given offsets
    //! The block is not pinned.
    Block(const ByteBlockPtr& byte_block,
          size_t begin, size_t end, size_t first_item, size_t num_items)
        : byte_block_(byte_block),
          begin_(begin), end_(end),
          first_item_(first_item), num_items_(num_items), pinned_(false)
    {
    }

    //! Moves the block - the pinned property is moved as well
    //! the 'other' block is afterwards unpinned
    Block(Block&& other) {
        byte_block_ = other.byte_block_;
        begin_ = other.begin_;
        end_ = other.end_;
        first_item_ = other.first_item_;
        num_items_ = other.num_items_;
        pinned_ = other.pinned_;

        // we do not have to change the pin_count_
        other.pinned_ = false;

        other.byte_block_ = nullptr;
        other.begin_ = 0;
        other.end_ = 0;
        other.first_item_ = 0;
        other.num_items_ = 0;
    }

    Block(const Block& other) {
        *this = other;
    }

    //! assigns a block. If this block is pinned it is unpinned before
    //! re-assigned.
    Block& operator = (const Block& other) {
        UnpinMaybe();
        byte_block_ = other.byte_block_;
        begin_ = other.begin_;
        end_ = other.end_;
        first_item_ = other.first_item_;
        num_items_ = other.num_items_;
        pinned_ = other.pinned_;
        if (pinned_ && byte_block_)
            byte_block_->IncreasePinCount();
        return *this;
    }

    //! Return whether the enclosed ByteBlock is valid.
    bool IsValid() const {
        return byte_block_;
    }

    //! Releases the reference to the ByteBlock and resets book-keeping info
    void Release() {
        UnpinMaybe();
        byte_block_ = ByteBlockPtr();
    }

    ~Block() {
        UnpinMaybe();
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
               << " num_items_=" << b.num_items_
               << " pinned=" << (b.pinned_ ? "yes" : "no");
        }
        return os << "]";
    }

protected:
    Block(const ByteBlockPtr& byte_block,
          size_t begin, size_t end, size_t first_item, size_t num_items, bool pinned)
        : byte_block_(byte_block),
          begin_(begin), end_(end),
          first_item_(first_item), num_items_(num_items),
          pinned_(pinned)
    { }

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

    //! whether this Block is pointing to a pinned ByteBlock. Can only be set
    //! during initialization
    bool pinned_;

    //! Creates a pinned copy of the Block
    //! If the underlying data::ByteBlock is already pinned, the Future is directly filled with a copy if this block
    //! Otherwise an async pin call will be issued
    common::Future<Block>&& Pin() {
        //future required for passing result from backgroud thread (which calls the callback) back to caller's thread
        common::Future<Block> result;
        //pinned blocks can be returned straigt away
        if (pinned_) {
            result << std::move(Block( byte_block_, begin_, end_, first_item_, num_items_, pinned_ ));
        } else {
            //call pin with callback that creates new, pinned block
            byte_block_->Pin([&](){
                result << std::move(Block( byte_block_, begin_, end_, first_item_, num_items_, true ));
            });
        }
        return std::move(result);
    }

    //! Unpins the underlying byte block if it is valid and pinned
    void UnpinMaybe() {
        if(byte_block_ && pinned_)
            byte_block_->DecreasePinCount();
    }
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_HEADER

/******************************************************************************/
