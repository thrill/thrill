/*******************************************************************************
 * thrill/data/block.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_HEADER
#define THRILL_DATA_BLOCK_HEADER

#include <thrill/common/counting_ptr.hpp>
#include <thrill/common/future.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/mem/manager.hpp>

#include <cassert>
#include <future>
#include <memory>
#include <ostream>
#include <string>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

class PinnedBlock;

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
    //! default-ctor: create invalid Block.
    Block() = default;

    Block(const Block& other) = default;
    Block& operator = (Block& other) = default;
    Block(Block&& other) = default;
    Block& operator = (Block&& other) = default;

    //! Creates a block that points to the given data::ByteBlock with the given
    //! offsets The block can be initialized as pinned or not.
    Block(ByteBlockPtr&& byte_block,
          size_t begin, size_t end, size_t first_item, size_t num_items)
        : byte_block_(std::move(byte_block)),
          begin_(begin), end_(end),
          first_item_(first_item), num_items_(num_items) { }

    //! Return whether the enclosed ByteBlock is valid.
    bool IsValid() const {
        return byte_block_;
    }

    //! access to byte_block_
    const ByteBlockPtr & byte_block() const { return byte_block_; }

    //! access to byte_block_ (mutable)
    ByteBlockPtr & byte_block() { return byte_block_; }

    //! return number of items beginning in this block
    size_t num_items() const { return num_items_; }

    //! return number of pins in underlying ByteBlock
    size_t pin_count() const {
        assert(byte_block_);
        return byte_block_->pin_count();
    }

    //! accessor to begin_
    void set_begin(size_t i) { begin_ = i; }

    //! accessor to end_
    void set_end(size_t i) { end_ = i; }

    //! return length of valid data in bytes.
    size_t size() const { return end_ - begin_; }

    //! accessor to first_item_ (absolute in ByteBlock)
    size_t first_item_absolute() const { return first_item_; }

    //! return the first_item_offset relative to data_begin().
    size_t first_item_relative() const { return first_item_ - begin_; }

    friend std::ostream& operator << (std::ostream& os, const Block& b) {
        os << "[Block " << std::hex << &b << std::dec
           << " byte_block_=" << std::hex << b.byte_block_.get() << std::dec;
        if (b.IsValid()) {
            os << " begin_=" << b.begin_
               << " end_=" << b.end_
               << " first_item_=" << b.first_item_
               << " num_items_=" << b.num_items_;
            // << " data=" << common::Hexdump(b.ToString());
        }
        return os << "]";
    }

    //! Creates a pinned copy of this Block. If the underlying data::ByteBlock
    //! is already pinned, the Future is directly filled with a copy if this
    //! block.  Otherwise an async pin call will be issued.
    std::future<PinnedBlock> Pin() const;

    PinnedBlock PinNow() const;

    //! Return block as std::string (for debugging), includes eventually cut off
    //! elements form the beginning included
    std::string ToString() const;

protected:
    static const bool debug = false;

    //! referenced ByteBlock
    ByteBlockPtr byte_block_;

    //! beginning offset of valid bytes to read
    size_t begin_ = 0;

    //! one byte beyond the end of the valid bytes in the ByteBlock (can be used
    //! to virtually shorten a ByteBlock)
    size_t end_ = 0;

    //! offset of first valid element in the ByteBlock in absolute bytes from
    //! byte_block_->begin().
    size_t first_item_ = 0;

    //! number of valid items that _start_ in this block (includes cut-off
    //! element at the end)
    size_t num_items_ = 0;
};

class PinnedBlock : public Block
{
public:
    //! Create invalid PinnedBlock.
    PinnedBlock() = default;

    //! Creates a block that points to the given data::PinnedByteBlock with the
    //! given offsets. The returned block is also pinned, the pin is transfered!
    PinnedBlock(PinnedByteBlockPtr&& byte_block,
                size_t begin, size_t end, size_t first_item, size_t num_items)
        : Block(std::move(byte_block), begin, end, first_item, num_items) {
        LOG << "PinnedBlock::Acquire() from new PinnedByteBlock";
    }

    //! copy-ctor: increment underlying's pin count
    PinnedBlock(const PinnedBlock& pb) noexcept
        : Block(pb) { if (byte_block_) byte_block_->IncPinCount(); }

    //! move-ctor: move underlying's pin
    PinnedBlock(PinnedBlock&& pb) noexcept : Block(std::move(pb)) {
        assert(!pb.byte_block_);
    }

    //! copy-assignment: copy underlying and increase pin count
    PinnedBlock& operator = (PinnedBlock& pb) noexcept {
        if (this == &pb) return *this;
        // first acquire other's pin count
        if (pb.byte_block_) pb.byte_block_->IncPinCount();
        // then release the current one
        if (byte_block_) byte_block_->DecPinCount();
        // copy over Block information
        Block::operator = (pb);
        return *this;
    }

    //! move-assignment: move underlying, release current's pin
    PinnedBlock& operator = (PinnedBlock&& pb) noexcept {
        if (this == &pb) return *this;
        // release the current one
        if (byte_block_)
            byte_block_->DecPinCount();
        // move over Block information, keep other's pin count
        Block::operator = (std::move(pb));
        // invalidate other block
        assert(!pb.byte_block_);
        return *this;
    }

    ~PinnedBlock() {
        if (byte_block_)
            byte_block_->DecPinCount();
    }

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

    //! extract ByteBlock including it's pin.
    PinnedByteBlockPtr StealPinnedByteBlock() {
        return PinnedByteBlockPtr(std::move(byte_block_));
    }

    //! access to byte_block_ which is pinned.
    PinnedByteBlockPtr pinned_byte_block() const {
        PinnedByteBlockPtr pbb(byte_block_);
        if (pbb.valid()) pbb->IncPinCount();
        return pbb;
    }

    //! Return block as std::string (for debugging), includes eventually cut off
    //! elements form the beginning included
    std::string ToString() const {
        if (!IsValid()) return std::string();
        return std::string(
            reinterpret_cast<const char*>(data_begin()), size());
    }

protected:
    //! protected construction from an unpinned block AFTER the pin was taken,
    //! this method does NOT pin it.
    explicit PinnedBlock(const Block& b) : Block(b) { }

    //! friend for creating PinnedBlock in Pin() using protected constructor
    friend class Block;
};

inline
std::future<PinnedBlock> Block::Pin() const {

    std::promise<PinnedBlock> result;

    byte_block_->IncPinCount();
    result.set_value(PinnedBlock(*this));

#if 0
    // pinned blocks can be returned straigt away
    if (pinned_ || 1) {
        sLOG << "block " << byte_block_->data_ << " was already pinned";
        *result << Block(*this);
    }
    else {
        sLOG << "request pin for block " << byte_block_->swap_token_;
        // call pin with callback that creates new, pinned block
        byte_block_->GetPin(
            [&]() {
                Block b(*this);
                b.pinned_ = true;
                sLOG << "block " << byte_block_->swap_token_ << "/" << byte_block_->data_ << " is now pinned";
                *result << std::move(b);
            });
    }
#endif

    return result.get_future();
}

inline
PinnedBlock Block::PinNow() const {
    std::future<PinnedBlock> pin = Pin();
    pin.wait();
    return pin.get();
}

inline std::string Block::ToString() const {
    return PinNow().ToString();
}

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_HEADER

/******************************************************************************/
