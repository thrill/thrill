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
#include <thrill/common/logger.hpp>
#include <thrill/data/byte_block.hpp>
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

    //! mutable access to byte_block_
    ByteBlockPtr & byte_block() { return byte_block_; }

    //! return number of items beginning in this block
    size_t num_items() const { return num_items_; }

    //! return number of pins in underlying ByteBlock
    size_t pin_count(size_t local_worker_id) const {
        assert(byte_block_);
        return byte_block_->pin_count(local_worker_id);
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

    friend std::ostream& operator << (std::ostream& os, const Block& b);

    //! Creates a pinned copy of this Block. If the underlying data::ByteBlock
    //! is already pinned, the Future is directly filled with a copy if this
    //! block.  Otherwise an async pin call will be issued.
    std::future<PinnedBlock> Pin(size_t local_worker_id) const;

    //! Convenience function to call Pin() and wait for the future.
    PinnedBlock PinWait(size_t local_worker_id) const;

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

/*!
 * A pinned / pin-counted derivative of a Block. By holding a pin, it is a
 * guaranteed that the contained ByteBlock's data is loaded in RAM. Since pins
 * are counted per thread, the PinnedBlock is a counting pointer plus a thread
 * id. An ordinary Block can be pinned by calling Pin(), which delivers a future
 * PinnedBlock, which is available once the data is actually loaded.
 *
 * Be careful to move PinnedBlock as must as possible, since copying costs a
 * pinning and an unpinning operation, whereas moving is free.
 */
class PinnedBlock : public Block
{
public:
    //! Create invalid PinnedBlock.
    PinnedBlock() = default;

    //! Creates a block that points to the given data::PinnedByteBlock with the
    //! given offsets. The returned block is also pinned, the pin is transfered!
    PinnedBlock(PinnedByteBlockPtr&& byte_block,
                size_t begin, size_t end, size_t first_item, size_t num_items)
        : Block(std::move(byte_block), begin, end, first_item, num_items),
          local_worker_id_(byte_block.local_worker_id()) {
        LOG << "PinnedBlock::Acquire() from new PinnedByteBlock"
            << " for local_worker_id=" << local_worker_id_;
    }

    //! copy-ctor: increment underlying's pin count
    PinnedBlock(const PinnedBlock& pb) noexcept
        : Block(pb), local_worker_id_(pb.local_worker_id_) {
        if (byte_block_) byte_block_->IncPinCount(local_worker_id_);
    }

    //! move-ctor: move underlying's pin
    PinnedBlock(PinnedBlock&& pb) noexcept
        : Block(std::move(pb)), local_worker_id_(pb.local_worker_id_) {
        assert(!pb.byte_block_);
    }

    //! copy-assignment: copy underlying and increase pin count
    PinnedBlock& operator = (PinnedBlock& pb) noexcept {
        if (this == &pb) return *this;
        // first acquire other's pin count
        if (pb.byte_block_) pb.byte_block_->IncPinCount(pb.local_worker_id_);
        // then release the current one
        if (byte_block_) byte_block_->DecPinCount(local_worker_id_);
        // copy over Block information
        Block::operator = (pb);
        local_worker_id_ = pb.local_worker_id_;
        return *this;
    }

    //! move-assignment: move underlying, release current's pin
    PinnedBlock& operator = (PinnedBlock&& pb) noexcept {
        if (this == &pb) return *this;
        // release the current one
        if (byte_block_) byte_block_->DecPinCount(local_worker_id_);
        // move over Block information, keep other's pin count
        Block::operator = (std::move(pb));
        local_worker_id_ = pb.local_worker_id_;
        // invalidate other block
        assert(!pb.byte_block_);
        return *this;
    }

    ~PinnedBlock() {
        if (byte_block_)
            byte_block_->DecPinCount(local_worker_id_);
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

    //! release pin on block and reset Block pointer to nullptr
    void Reset() {
        if (byte_block_) {
            byte_block_->DecPinCount(local_worker_id_);
            byte_block_.reset();
        }
    }

    //! extract ByteBlock including it's pin. afterwards, this PinnedBlock is
    //! invalid.
    PinnedByteBlockPtr StealPinnedByteBlock() {
        return PinnedByteBlockPtr(std::move(byte_block_), local_worker_id_);
    }

    //! copy the underlying byte_block_ into a new PinnedByteBlockPtr, which
    //! increases the pin count. use StealPinnedByteBlock to move the underlying
    //! pin out (cheaper).
    PinnedByteBlockPtr CopyPinnedByteBlock() const {
        PinnedByteBlockPtr pbb(byte_block_, local_worker_id_);
        if (pbb.valid()) pbb->IncPinCount(local_worker_id_);
        return pbb;
    }

    //! Return block as std::string (for debugging), includes eventually cut off
    //! elements form the beginning included
    std::string ToString() const;

    //! not available in PinnedBlock
    std::future<PinnedBlock> Pin() const = delete;

    //! not available in PinnedBlock
    PinnedBlock PinWait() const = delete;

    //! make ostreamable for debugging
    friend std::ostream& operator << (std::ostream& os, const PinnedBlock& b);

private:
    //! protected construction from an unpinned block AFTER the pin was taken,
    //! this method does NOT pin it.
    PinnedBlock(const Block& b, size_t local_worker_id)
        : Block(b), local_worker_id_(local_worker_id) { }

    //! thread id of holder of pin
    size_t local_worker_id_;

    //! friend for creating PinnedBlock from unpinned Block in PinBlock() using
    //! protected constructor.
    friend class BlockPool;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_HEADER

/******************************************************************************/
