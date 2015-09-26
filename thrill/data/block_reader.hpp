/*******************************************************************************
 * thrill/data/block_reader.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_READER_HEADER
#define THRILL_DATA_BLOCK_READER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/item_serialization_tools.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/serialization.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * BlockReader takes Block objects from BlockSource and allows reading of
 * a) serializable Items or b) arbitray data from the Block sequence. It takes
 * care of fetching the next Block when the previous one underruns and also of
 * data items split between two Blocks.
 */
template <typename BlockSource>
class BlockReader
    : public common::ItemReaderToolsBase<BlockReader<BlockSource> >
{
public:
    static const bool self_verify = common::g_self_verify;

    //! Start reading a File
    explicit BlockReader(BlockSource&& source)
        : source_(std::move(source)) { }

    //! Return reference to enclosed BlockSource
    BlockSource & source() { return source_; }

    //! non-copyable: delete copy-constructor
    BlockReader(const BlockReader&) = delete;
    //! non-copyable: delete assignment operator
    BlockReader& operator = (const BlockReader&) = delete;
    //! move-constructor: default
    BlockReader(BlockReader&&) = default;
    //! move-assignment operator: default
    BlockReader& operator = (BlockReader&&) = default;

    //! \name Reading (Generic) Items
    //! \{

    //! Next() reads a complete item T
    template <typename T>
    T Next() {
        assert(HasNext());
        assert(num_items_ > 0);
        --num_items_;

        if (self_verify) {
            // for self-verification, T is prefixed with its hash code
            size_t code = Get<size_t>();
            if (code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "BlockReader::Next() attempted to retrieve item "
                          "with different typeid!");
            }
        }
        return Serialization<BlockReader, T>::Deserialize(*this);
    }

    //! Next() reads a complete item T, without item counter or self
    //! verification
    template <typename T>
    T NextNoSelfVerify() {
        assert(HasNext());
        return Serialization<BlockReader, T>::Deserialize(*this);
    }

    //! HasNext() returns true if at least one more item is available.
    bool HasNext() {
        while (current_ == end_) {
            if (!NextBlock()) {
                return false;
            }
        }
        return true;
    }

    //! Return complete contents until empty as a std::vector<T>. Use this only
    //! if you are sure that it will fit into memory, -> only use it for tests.
    template <typename ItemType>
    std::vector<ItemType> ReadComplete() {
        std::vector<ItemType> out;
        while (HasNext()) out.emplace_back(Next<ItemType>());
        return out;
    }

    //! Read n items, however, do not deserialize them but deliver them as a
    //! vector of Block objects. This is used to take out a range of
    //! items, the internal item cursor is advanced by n.
    template <typename ItemType>
    std::vector<Block> GetItemBatch(size_t n) {
        static const bool debug = false;

        std::vector<Block> out;
        if (n == 0) return out;

        die_unless(HasNext());
        assert(bytes_);

        const Byte* begin_output = current_;
        size_t first_output = current_ - bytes_->begin();

        // inside the if-clause the current_ may not point to a valid item
        // boundary.
        if (n >= num_items_)
        {
            // *** if the current block still contains items, push it partially

            if (n >= num_items_) {
                // construct first Block using current_ pointer
                out.emplace_back(
                    bytes_,
                    // valid range: excludes preceding items.
                    current_ - bytes_->begin(), end_ - bytes_->begin(),
                    // first item is at begin_ (we may have dropped some)
                    current_ - bytes_->begin(),
                    // remaining items in this block
                    num_items_);

                sLOG << "partial first:" << out.back();

                n -= num_items_;

                // get next block. if not possible -> may be okay since last
                // item might just terminate the current block.
                if (!NextBlock()) {
                    assert(n == 0);
                    sLOG << "exit1 after batch.";
                    return out;
                }
            }

            // *** then append complete blocks without deserializing them

            while (n >= num_items_) {
                out.emplace_back(
                    bytes_,
                    // full range is valid.
                    current_ - bytes_->begin(), end_ - bytes_->begin(),
                    first_item_, num_items_);

                sLOG << "middle:" << out.back();

                n -= num_items_;

                if (!NextBlock()) {
                    assert(n == 0);
                    sLOG << "exit2 after batch.";
                    return out;
                }
            }

            // move current_ to the first valid item of the block we got (at
            // least one NextBlock() has been called). But when constructing the
            // last Block, we have to include the partial item in the
            // front.
            begin_output = current_;
            first_output = first_item_;

            current_ = bytes_->begin() + first_item_;
        }

        // put prospective last block into vector.

        out.emplace_back(
            bytes_,
            // full range is valid.
            begin_output - bytes_->begin(), end_ - bytes_->begin(),
            first_output, n);

        // skip over remaining items in this block, there while collect all
        // blocks needed for those items via block_collect_. There can be more
        // than one block necessary for Next if an item is large!

        block_collect_ = &out;
        if (Serialization<BlockReader, ItemType>::is_fixed_size) {
            Skip(n, n * ((self_verify ? sizeof(size_t) : 0) +
                         Serialization<BlockReader, ItemType>::fixed_size));
        }
        else {
            while (n > 0) {
                Next<ItemType>();
                --n;
            }
        }
        block_collect_ = nullptr;

        out.back().set_end(current_ - bytes_->begin());

        sLOG << "partial last:" << out.back();

        sLOG << "exit3 after batch:"
             << "current_=" << current_ - bytes_->begin();

        return out;
    }

    //! \}

    //! \name Cursor Reading Methods
    //! \{

    //! Fetch a number of unstructured bytes from the current block, advancing
    //! the cursor.
    BlockReader & Read(void* outdata, size_t size) {

        Byte* cdata = reinterpret_cast<Byte*>(outdata);

        while (THRILL_UNLIKELY(current_ + size > end_)) {
            // partial copy of remainder of block
            size_t partial_size = end_ - current_;
            std::copy(current_, current_ + partial_size, cdata);

            cdata += partial_size;
            size -= partial_size;

            if (!NextBlock())
                throw std::runtime_error("Data underflow in BlockReader.");
        }

        // copy rest from current block
        std::copy(current_, current_ + size, cdata);
        current_ += size;

        return *this;
    }

    //! Fetch a number of unstructured bytes from the buffer as std::string,
    //! advancing the cursor.
    std::string Read(size_t datalen) {
        std::string out(datalen, 0);
        Read(const_cast<char*>(out.data()), out.size());
        return out;
    }

    //! Advance the cursor given number of bytes without reading them.
    BlockReader & Skip(size_t items, size_t bytes) {
        while (THRILL_UNLIKELY(current_ + bytes > end_)) {
            bytes -= end_ - current_;
            // deduct number of remaining items in skipped block from item skip
            // counter.
            items -= num_items_;
            if (!NextBlock())
                throw std::runtime_error("Data underflow in BlockReader.");
        }
        current_ += bytes;
        // the last line skipped over the remaining "items" number of items.
        num_items_ -= items;
        return *this;
    }

    //! Fetch a single byte from the current block, advancing the cursor.
    Byte GetByte() {
        // loop, since blocks can actually be empty.
        while (THRILL_UNLIKELY(current_ == end_)) {
            if (!NextBlock())
                throw std::runtime_error("Data underflow in BlockReader.");
        }
        return *current_++;
    }

    //! Fetch a single item of the template type Type from the buffer,
    //! advancing the cursor. Be careful with implicit type conversions!
    template <typename Type>
    Type Get() {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Get() POD types as raw values.");

        Type ret;

        // fast path for reading item from block if it fits.
        if (THRILL_LIKELY(current_ + sizeof(Type) <= end_)) {
            ret = *reinterpret_cast<const Type*>(current_);
            current_ += sizeof(Type);
        }
        else {
            Read(&ret, sizeof(ret));
        }

        return ret;
    }

    //! \}

private:
    //! Instance of BlockSource. This is NOT a reference, as to enable embedding
    //! of FileBlockSource to compose classes into File::Reader.
    BlockSource source_;

    //! The current block being read, this holds a shared pointer reference.
    ByteBlockPtr bytes_;

    //! current read pointer into current block of file.
    const Byte* current_ = nullptr;

    //! pointer to end of current block.
    const Byte* end_ = nullptr;

    //! offset of first valid item in block (needed only during direct copying
    //! of Blocks).
    size_t first_item_;

    //! remaining number of items starting in this block
    size_t num_items_ = 0;

    //! pointer to vector to collect blocks in GetItemRange.
    std::vector<Block>* block_collect_ = nullptr;

    //! Call source_.NextBlock with appropriate parameters
    bool NextBlock() {
        Block b = source_.NextBlock();
        sLOG0 << "BlockReader::NextBlock" << b;

        bytes_ = b.byte_block();
        if (!b.IsValid()) return false;

        if (block_collect_)
            block_collect_->emplace_back(b);

        current_ = b.data_begin();
        end_ = b.data_end();
        first_item_ = b.first_item_absolute();
        num_items_ = b.num_items();
        return true;
    }
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_READER_HEADER

/******************************************************************************/
