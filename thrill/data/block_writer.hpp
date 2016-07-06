/*******************************************************************************
 * thrill/data/block_writer.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_WRITER_HEADER
#define THRILL_DATA_BLOCK_WRITER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/item_serialization_tools.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/serialization.hpp>

#include <algorithm>
#include <deque>
#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

/*!
 * An Exception is thrown by BlockWriter when the underlying sink does not allow
 * allocation of a new block, which is needed to serialize the item.
 */
class FullException : public std::exception
{
public:
    FullException() : std::exception() { }
};

/*!
 * BlockWriter contains a temporary Block object into which a) any serializable
 * item can be stored or b) any arbitrary integral data can be appended. It
 * counts how many serializable items are stored and the offset of the first new
 * item. When a Block is full it is emitted to an attached BlockSink, like a
 * File, a ChannelSink, etc. for further delivery. The BlockWriter takes care of
 * segmenting items when a Block is full.
 */
template <typename BlockSink>
class BlockWriter
    : public common::ItemWriterToolsBase<BlockWriter<BlockSink> >
{
public:
    static constexpr bool debug = false;

    static constexpr bool self_verify = common::g_self_verify;

    //! Start build (appending blocks) to a File
    explicit BlockWriter(BlockSink* sink,
                         size_t max_block_size = default_block_size)
        : sink_(sink),
          // block_size_(std::min(size_t(start_block_size), max_block_size)),
          block_size_(max_block_size),
          max_block_size_(max_block_size) {
        assert(max_block_size_ > 0);
    }

    //! default constructor
    BlockWriter() = default;

    //! non-copyable: delete copy-constructor
    BlockWriter(const BlockWriter&) = delete;
    //! non-copyable: delete assignment operator
    BlockWriter& operator = (const BlockWriter&) = delete;

    //! move-constructor
    BlockWriter(BlockWriter&& bw) noexcept
        : bytes_(std::move(bw.bytes_)),
          current_(std::move(bw.current_)),
          end_(std::move(bw.end_)),
          nitems_(std::move(bw.nitems_)),
          first_offset_(std::move(bw.first_offset_)),
          sink_(std::move(bw.sink_)),
          do_queue_(std::move(bw.do_queue_)),
          sink_queue_(std::move(bw.sink_queue_)),
          block_size_(std::move(bw.block_size_)),
          max_block_size_(std::move(bw.max_block_size_)),
          closed_(std::move(bw.closed_)) {
        // set closed flag -> disables destructor
        bw.closed_ = true;
    }

    //! move-assignment
    BlockWriter& operator = (BlockWriter&& bw) noexcept {
        if (this == &bw) return *this;

        bytes_ = std::move(bw.bytes_);
        current_ = std::move(bw.current_);
        end_ = std::move(bw.end_);
        nitems_ = std::move(bw.nitems_);
        first_offset_ = std::move(bw.first_offset_);
        sink_ = std::move(bw.sink_);
        do_queue_ = std::move(bw.do_queue_);
        sink_queue_ = std::move(bw.sink_queue_);
        block_size_ = std::move(bw.block_size_);
        max_block_size_ = std::move(bw.max_block_size_);
        closed_ = std::move(bw.closed_);
        // set closed flag -> disables destructor
        bw.closed_ = true;
        return *this;
    }

    //! On destruction, the last partial block is flushed.
    ~BlockWriter() {
        if (!closed_)
            Close();
    }

    //! Explicitly close the writer
    void Close() {
        if (closed_) return;
        closed_ = true;
        Flush();
        if (sink_)
            sink_->Close();
    }

    //! Return whether an actual BlockSink is attached.
    bool IsValid() const { return sink_ != nullptr; }

    //! Flush the current block (only really meaningful for a network sink).
    void Flush() {
        if (!bytes_) return;
        // don't flush if the block is truly empty.
        if (current_ == bytes_->begin() && nitems_ == 0) return;

        if (do_queue_) {
            sLOG << "Flush(): queue" << bytes_.get();
            sink_queue_.emplace_back(
                std::move(bytes_), 0, current_ - bytes_->begin(),
                first_offset_, nitems_,
                static_cast<bool>(/* typecode_verify */ self_verify));
        }
        else {
            sLOG << "Flush(): flush" << bytes_.get();
            sink_->AppendPinnedBlock(
                PinnedBlock(std::move(bytes_), 0, current_ - bytes_->begin(),
                            first_offset_, nitems_,
                            static_cast<bool>(/* typecode_verify */ self_verify)));
        }

        // reset
        nitems_ = 0;
        bytes_ = PinnedByteBlockPtr();
        current_ = end_ = nullptr;
    }

    //! Directly write Blocks to the underlying BlockSink (after flushing the
    //! current one if need be).
    void AppendBlocks(const std::vector<Block>& blocks) {
        Flush();
        for (const Block& b : blocks)
            sink_->AppendBlock(b);
    }

    //! Directly write Blocks to the underlying BlockSink (after flushing the
    //! current one if need be).
    void AppendBlocks(const std::deque<Block>& blocks) {
        Flush();
        for (const Block& b : blocks)
            sink_->AppendBlock(b);
    }

    //! \name Appending (Generic) Serializable Items
    //! \{

    //! Mark beginning of an item.
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    BlockWriter& MarkItem() {
        if (current_ == end_)
            Flush(), AllocateBlock();

        if (nitems_ == 0)
            first_offset_ = current_ - bytes_->begin();

        ++nitems_;

        return *this;
    }

    //! Put appends a complete item, or fails with a FullException.
    template <typename T>
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    BlockWriter& Put(const T& x) {
        assert(!closed_);

        if (!BlockSink::allocate_can_fail_)
            return PutUnsafe<T>(x);
        else
            return PutSafe<T>(x);
    }

    //! PutNoSelfVerify appends a complete item without any self
    //! verification information, or fails with a FullException.
    template <typename T>
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    BlockWriter& PutNoSelfVerify(const T& x) {
        assert(!closed_);

        if (!BlockSink::allocate_can_fail_)
            return PutUnsafe<T, true>(x);
        else
            return PutSafe<T, true>(x);
    }

    //! appends a complete item, or fails safely with a FullException.
    template <typename T, bool NoSelfVerify = false>
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    BlockWriter& PutSafe(const T& x) {
        assert(!closed_);

        if (current_ == end_) {
            // if current block full: flush it, BEFORE enabling queuing, because
            // the previous item is complete.
            try {
                Flush(), AllocateBlock();
            }
            catch (FullException&) {
                // non-fatal allocation error: will be handled below.
            }
        }

        if (!bytes_) {
            sLOG << "!bytes";
            throw FullException();
        }

        // store beginning item of this item and other information for unwind.
        Byte* initial_current = current_;
        size_t initial_nitems = nitems_;
        size_t initial_first_offset = first_offset_;
        do_queue_ = true;

        try {
            MarkItem();
            if (self_verify && !NoSelfVerify) {
                // for self-verification, prefix T with its hash code
                PutRaw(typeid(T).hash_code());
            }
            Serialization<BlockWriter, T>::Serialize(x, *this);

            // item fully serialized, push out finished blocks.
            while (!sink_queue_.empty()) {
                sink_->AppendPinnedBlock(sink_queue_.front());
                sink_queue_.pop_front();
            }

            do_queue_ = false;

            return *this;
        }
        catch (FullException&) {
            // if BlockSink signaled full, then unwind adding of the item.

            while (!sink_queue_.empty()) {
                sLOG << "releasing" << bytes_.get();
                sink_->ReleaseByteBlock(bytes_);

                PinnedBlock b = sink_queue_.back();
                sink_queue_.pop_back();

                bytes_ = std::move(b).StealPinnedByteBlock();
            }

            sLOG << "reset" << bytes_.get();

            current_ = initial_current;
            end_ = bytes_->end();
            nitems_ = initial_nitems;
            first_offset_ = initial_first_offset;
            do_queue_ = false;

            throw;
        }
    }

    //! appends a complete item, or aborts with a FullException.
    template <typename T, bool NoSelfVerify = false>
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    BlockWriter& PutUnsafe(const T& x) {
        assert(!closed_);

        try {
            if (current_ == end_) {
                Flush(), AllocateBlock();
            }

            MarkItem();
            if (self_verify && !NoSelfVerify) {
                // for self-verification, prefix T with its hash code
                PutRaw(typeid(T).hash_code());
            }
            Serialization<BlockWriter, T>::Serialize(x, *this);
        }
        catch (FullException&) {
            throw std::runtime_error(
                      "BlockSink was full even though declared infinite");
        }

        return *this;
    }

    //! \}

    //! \name Appending Write Functions
    //! \{

    //! Append a memory range to the block
    BlockWriter& Append(const void* data, size_t size) {
        assert(!closed_);

        const Byte* cdata = reinterpret_cast<const Byte*>(data);

        while (THRILL_UNLIKELY(current_ + size > end_)) {
            // partial copy of beginning of buffer
            size_t partial_size = end_ - current_;
            std::copy(cdata, cdata + partial_size, current_);

            cdata += partial_size;
            size -= partial_size;
            current_ += partial_size;

            Flush(), AllocateBlock();
        }

        // copy remaining bytes.
        std::copy(cdata, cdata + size, current_);
        current_ += size;

        return *this;
    }

    //! Append a single byte to the block
    BlockWriter& PutByte(Byte data) {
        assert(!closed_);

        if (THRILL_UNLIKELY(current_ == end_))
            Flush(), AllocateBlock();

        *current_++ = data;
        return *this;
    }

    //! Append to contents of a std::string, excluding the null (which isn't
    //! contained in the string size anyway).
    BlockWriter& Append(const std::string& str) {
        return Append(str.data(), str.size());
    }

    //! Put (append) a single item of the template type T to the buffer. Be
    //! careful with implicit type conversions!
    template <typename Type>
    BlockWriter& PutRaw(const Type& item) {
        static_assert(std::is_pod<Type>::value,
                      "You only want to PutRaw() POD types as raw values.");

        assert(!closed_);

        // fast path for writing item into block if it fits.
        if (THRILL_LIKELY(current_ + sizeof(Type) <= end_)) {
            *reinterpret_cast<Type*>(current_) = item;

            current_ += sizeof(Type);
            return *this;
        }

        return Append(&item, sizeof(item));
    }

    //! \}

private:
    //! Allocate a new block (overwriting the existing one).
    void AllocateBlock() {
        bytes_ = sink_->AllocateByteBlock(block_size_);
        if (!bytes_) {
            sLOG << "AllocateBlock(): throw due to invalid block";
            throw FullException();
        }
        sLOG << "AllocateBlock(): good, got" << bytes_.get();
        // increase block size, up to max.
        if (block_size_ < max_block_size_) block_size_ *= 2;

        current_ = bytes_->begin();
        end_ = bytes_->end();
        nitems_ = 0;
        first_offset_ = 0;
    }

    //! current block, already allocated as shared ptr, since we want to use
    //! make_shared.
    PinnedByteBlockPtr bytes_;

    //! current write pointer into block.
    Byte* current_ = nullptr;

    //! current end of block pointer. this is == bytes_.end(), just one
    //! indirection less.
    Byte* end_ = nullptr;

    //! number of items in current block
    size_t nitems_ = 0;

    //! offset of first item
    size_t first_offset_ = 0;

    //! file or stream sink to output blocks to.
    BlockSink* sink_ = nullptr;

    //! boolean whether to queue blocks
    bool do_queue_ = false;

    //! queue of blocks to flush when the current item has fully been serialized
    std::deque<PinnedBlock> sink_queue_;

    //! size of data blocks to construct
    size_t block_size_;

    //! size of data blocks to construct
    size_t max_block_size_;

    //! Flag if Close was called explicitly
    bool closed_ = false;
};

//! alias for BlockWriter which outputs to a generic BlockSink.
using DynBlockWriter = BlockWriter<data::BlockSink>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_WRITER_HEADER

/******************************************************************************/
