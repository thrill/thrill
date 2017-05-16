/*******************************************************************************
 * thrill/data/file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_FILE_HEADER
#define THRILL_DATA_FILE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/dyn_block_reader.hpp>

#include <tlx/die.hpp>

#include <cassert>
#include <deque>
#include <functional>
#include <limits>
#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

class KeepFileBlockSource;
class ConsumeFileBlockSource;

/*!
 * A File is an ordered sequence of Block objects for storing items. By using
 * the Block indirection, the File can be composed using existing Block objects
 * (via reference counting), but only contain a subset of the items in those
 * Blocks. This may be used for Zip() and Repartition().
 *
 * A File can be written using a BlockWriter instance, which is delivered by
 * GetWriter(). Thereafter it can be read (multiple times) using a BlockReader,
 * delivered by GetReader().
 *
 * Using a prefixsum over the number of items in a Block, one can seek to the
 * block contained any item offset in log_2(Blocks) time, though seeking within
 * the Block goes sequentially.
 */
class File : public virtual BlockSink, public tlx::ReferenceCounter
{
public:
    using Writer = BlockWriter<File>;
    using Reader = DynBlockReader;
    using KeepReader = BlockReader<KeepFileBlockSource>;
    using ConsumeReader = BlockReader<ConsumeFileBlockSource>;
    using DynWriter = DynBlockWriter;

    static constexpr size_t default_prefetch = 2;

    //! Constructor from BlockPool
    File(BlockPool& block_pool, size_t local_worker_id, size_t dia_id);

    //! non-copyable: delete copy-constructor
    File(const File&) = delete;
    //! non-copyable: delete assignment operator
    File& operator = (const File&) = delete;
    //! move-constructor: default
    File(File&&) = default;
    //! move-assignment operator: default
    File& operator = (File&&) = default;

    //! Return a copy of the File (explicit copy-constructor)
    File Copy() const;

    //! \name Methods of a BlockSink
    //! \{

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(const Block& b) {
        if (b.size() == 0) return;
        num_items_sum_.push_back(num_items() + b.num_items());
        size_bytes_ += b.size();
        stats_bytes_ += b.size();
        stats_items_ += b.num_items();
        blocks_.push_back(b);
    }

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(Block&& b) {
        if (b.size() == 0) return;
        num_items_sum_.push_back(num_items() + b.num_items());
        size_bytes_ += b.size();
        stats_bytes_ += b.size();
        stats_items_ += b.num_items();
        blocks_.emplace_back(std::move(b));
    }

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(const Block& b, bool /* is_last_block */) final {
        return AppendBlock(b);
    }

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(Block&& b, bool /* is_last_block */) final {
        return AppendBlock(std::move(b));
    }

    void Close() final;

    //! write out stats
    ~File();

    //! Free all Blocks in the File and deallocate vectors
    void Clear();

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    static constexpr bool allocate_can_fail_ = false;

    //! \}

    //! \name Writers and Readers
    //! \{

    //! Get BlockWriter.
    Writer GetWriter(size_t block_size = default_block_size);

    //! Get BlockWriter.
    DynWriter GetDynWriter(size_t block_size = default_block_size);

    /*!
     * Get BlockReader or a consuming BlockReader for beginning of File
     *
     * \attention If consume is true, the reader consumes the File's contents
     * UNCONDITIONALLY, the File will always be emptied whether all items were
     * read via the Reader or not.
     */
    Reader GetReader(
        bool consume, size_t num_prefetch = File::default_prefetch);

    //! Get BlockReader for beginning of File
    KeepReader GetKeepReader(
        size_t num_prefetch = File::default_prefetch) const;

    /*!
     * Get consuming BlockReader for beginning of File
     *
     * \attention The reader consumes the File's contents UNCONDITIONALLY, the
     * File will always be emptied whether all items were read via the Reader or
     * not.
     */
    ConsumeReader GetConsumeReader(
        size_t num_prefetch = File::default_prefetch);

    //! Get BlockReader seeked to the corresponding item index
    template <typename ItemType>
    KeepReader GetReaderAt(
        size_t index, size_t prefetch = default_prefetch) const;

    //! Read complete File into a std::string, obviously, this should only be
    //! used for debugging!
    std::string ReadComplete() const;

    //! \}

    //! Return the number of blocks
    size_t num_blocks() const { return blocks_.size(); }

    //! Return the number of items in the file
    size_t num_items() const {
        return num_items_sum_.size() ? num_items_sum_.back() : 0;
    }

    //! Returns true if the File is empty.
    bool empty() const { return blocks_.empty(); }

    //! Return the number of bytes of user data in this file.
    size_t size_bytes() const { return size_bytes_; }

    //! Return reference to a block
    const Block& block(size_t i) const {
        assert(i < blocks_.size());
        return blocks_[i];
    }

    //! Returns constant reference to all Blocks in the File.
    const std::deque<Block>& blocks() const { return blocks_; }

    //! Return number of items starting in block i
    size_t ItemsStartIn(size_t i) const {
        assert(i < blocks_.size());
        return num_items_sum_[i] - (i == 0 ? 0 : num_items_sum_[i - 1]);
    }

    //! Get item at the corresponding position. Do not use this
    //! method for reading multiple successive items.
    template <typename ItemType>
    ItemType GetItemAt(size_t index) const;

    /*!
     * Get index of the given item, or the next greater item, in this file. The
     * file has to be ordered according to the given compare function. The tie
     * value can be used to make a decision in case of many successive equal
     * elements.  The tie is compared with the local rank of the element.
     *
     * WARNING: This method uses GetItemAt combined with a binary search and is
     * therefore not efficient. The method should be reimplemented in near
     * future.
     */
    template <typename ItemType, typename CompareFunction = std::less<ItemType> >
    size_t GetIndexOf(const ItemType& item, size_t tie,
                      size_t left, size_t right,
                      const CompareFunction& func = CompareFunction()) const;

    /*!
     * Get index of the given item, or the next greater item, in this file. The
     * file has to be ordered according to the given compare function. The tie
     * value can be used to make a decision in case of many successive equal
     * elements.  The tie is compared with the local rank of the element.
     *
     * WARNING: This method uses GetItemAt combined with a binary search and is
     * therefore not efficient. The method should be reimplemented in near
     * future.
     */
    template <typename ItemType, typename CompareFunction = std::less<ItemType> >
    size_t GetIndexOf(const ItemType& item, size_t tie,
                      const CompareFunction& less = CompareFunction()) const {
        // start binary search with range [0,num_items)
        return GetIndexOf(item, tie, 0, num_items(), less);
    }

    //! Seek in File: return a Block range containing items begin, end of
    //! given type.
    template <typename ItemType>
    std::vector<Block> GetItemRange(size_t begin, size_t end) const;

    //! Output the Block objects contained in this File.
    friend std::ostream& operator << (std::ostream& os, const File& f);

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id) { dia_id_ = dia_id; }

private:
    //! unique file id
    size_t id_;

    //! optionally associated DIANode id
    size_t dia_id_;

    //! container holding Blocks and thus shared pointers to all byte blocks.
    std::deque<Block> blocks_;

    //! inclusive prefixsum of number of elements of blocks, hence
    //! num_items_sum_[i] is the number of items starting in all blocks
    //! preceding and including the i-th block.
    std::deque<size_t> num_items_sum_;

    //! Total size of this file in bytes. Sum of all block sizes.
    size_t size_bytes_ = 0;

    //! Total number of bytes stored in the File by a Writer: for stats, never
    //! decreases.
    size_t stats_bytes_ = 0;

    //! Total number of items stored in the File by a Writer: for stats, never
    //! decreases.
    size_t stats_items_ = 0;

    //! for access to blocks_ and num_items_sum_
    friend class KeepFileBlockSource;
    friend class ConsumeFileBlockSource;
};

using FilePtr = tlx::CountingPtr<File>;

/*!
 * A BlockSource to read Blocks from a File. The KeepFileBlockSource mainly
 * contains an index to the current block, which is incremented when the
 * NextBlock() must be delivered.
 */
class KeepFileBlockSource
{
public:
    //! Start reading a File
    KeepFileBlockSource(
        const File& file, size_t local_worker_id,
        size_t num_prefetch = File::default_prefetch,
        size_t first_block = 0, size_t first_item = keep_first_item);

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader
    PinnedBlock NextBlock();

    //! Perform prefetch
    void Prefetch(size_t prefetch);

protected:
    //! Determine current unpinned Block to deliver via NextBlock()
    Block NextUnpinnedBlock();

private:
    //! sentinel value for not changing the first_item item
    static constexpr size_t keep_first_item = size_t(-1);

    //! file to read blocks from
    const File& file_;

    //! local worker id reading the File
    size_t local_worker_id_;

    //! number of block prefetch operations
    size_t num_prefetch_;

    //! current prefetch operations
    std::deque<PinRequestPtr> fetching_blocks_;

    //! number of the first block
    size_t first_block_;

    //! index of current block.
    size_t current_block_;

    //! offset of first item in first block read
    size_t first_item_;
};

/*!
 * A BlockSource to read and simultaneously consume Blocks from a File. The
 * ConsumeFileBlockSource always returns the first block of the File and removes
 * it, hence, consuming Blocks from the File.
 *
 * \attention The reader consumes the File's contents UNCONDITIONALLY, the File
 * will always be emptied whether all items were read via the Reader or not.
 */
class ConsumeFileBlockSource
{
public:
    //! Start reading a File. Creates a source for the given file and set the
    //! number of blocks that should be prefetched. 0 means that no blocks are
    //! prefetched.
    ConsumeFileBlockSource(
        File* file, size_t local_worker_id,
        size_t num_prefetch = File::default_prefetch);

    //! non-copyable: delete copy-constructor
    ConsumeFileBlockSource(const ConsumeFileBlockSource&) = delete;
    //! non-copyable: delete assignment operator
    ConsumeFileBlockSource& operator = (const ConsumeFileBlockSource&) = delete;
    //! move-constructor: default
    ConsumeFileBlockSource(ConsumeFileBlockSource&& s);

    //! Perform prefetch
    void Prefetch(size_t prefetch);

    //! Get the next block of file.
    PinnedBlock NextBlock();

    //! Consume unread blocks and reset File to zero items.
    ~ConsumeFileBlockSource();

private:
    //! file to consume blocks from (ptr to make moving easier)
    File* file_;

    //! local worker id reading the File
    size_t local_worker_id_;

    //! number of block prefetch operations
    size_t num_prefetch_;

    //! current prefetch operations
    std::deque<PinRequestPtr> fetching_blocks_;
};

//! Get BlockReader seeked to the corresponding item index
template <typename ItemType>
typename File::KeepReader
File::GetReaderAt(size_t index, size_t prefetch) const {
    static constexpr bool debug = false;

    // perform binary search for item block with largest exclusive size
    // prefixsum less or equal to index.
    auto it =
        std::lower_bound(num_items_sum_.begin(), num_items_sum_.end(), index);

    if (it == num_items_sum_.end())
        die("Access beyond end of File?");

    size_t begin_block = it - num_items_sum_.begin();

    sLOG << "File::GetReaderAt()"
         << "item" << index << "in block" << begin_block
         << "psum" << num_items_sum_[begin_block]
         << "first_item" << blocks_[begin_block].first_item_absolute();

    // start Reader at given first valid item in located block
    KeepReader fr(
        KeepFileBlockSource(*this, local_worker_id_, prefetch,
                            begin_block,
                            blocks_[begin_block].first_item_absolute()));

    // skip over extra items in beginning of block
    size_t items_before = it == num_items_sum_.begin() ? 0 : *(it - 1);

    sLOG << "File::GetReaderAt()"
         << "items_before" << items_before << "index" << index
         << "delta" << (index - items_before);
    assert(items_before <= index);

    // use fixed_size information to accelerate jump.
    if (Serialization<KeepReader, ItemType>::is_fixed_size)
    {
        // fetch a Block to get typecode_verify flag
        fr.HasNext();

        const size_t skip_items = index - items_before;
        const size_t bytes_per_item =
            (fr.typecode_verify() ? sizeof(size_t) : 0)
            + Serialization<KeepReader, ItemType>::fixed_size;

        fr.Skip(skip_items, skip_items * bytes_per_item);
    }
    else
    {
        for (size_t i = items_before; i < index; ++i) {
            if (!fr.HasNext())
                die("Underflow in GetItemRange()");
            fr.template Next<ItemType>();
        }
    }

    sLOG << "File::GetReaderAt()"
         << "after seek at" << fr.CopyBlock();

    return fr;
}

template <typename ItemType>
ItemType File::GetItemAt(size_t index) const {
    KeepReader reader = this->GetReaderAt<ItemType>(index, /* prefetch */ 0);
    return reader.Next<ItemType>();
}

template <typename ItemType, typename CompareFunction>
size_t File::GetIndexOf(
    const ItemType& item, size_t tie, size_t left, size_t right,
    const CompareFunction& less) const {

    static constexpr bool debug = false;

    static_assert(
        std::is_convertible<
            bool, typename common::FunctionTraits<CompareFunction>::result_type
            >::value,
        "Comperator must return boolean.");

    LOG << "File::GetIndexOf() looking for item " << item << " tie " << tie
        << " in range [" << left << "," << right << ") ="
        << " size " << right - left;

    assert(left <= right);
    assert(left <= num_items());
    assert(right <= num_items());

    // Use a binary search to find the item.
    while (left < right) {
        size_t mid = (right + left) >> 1;
        LOG << "left: " << left << "right: " << right << "mid: " << mid;
        ItemType cur = GetItemAt<ItemType>(mid);
        LOG << "Item at mid: " << cur;
        if (less(item, cur) ||
            (!less(item, cur) && !less(cur, item) && tie <= mid)) {
            right = mid;
        }
        else {
            left = mid + 1;
        }
    }

    LOG << "found insert position at: " << left;
    return left;
}

//! Seek in File: return a Block range containing items begin, end of
//! given type.
template <typename ItemType>
std::vector<Block> File::GetItemRange(size_t begin, size_t end) const {
    assert(begin <= end);
    // deliver array of remaining blocks
    return GetReaderAt<ItemType>(begin)
           .template GetItemBatch<ItemType>(end - begin);
}

//! Take a vector of Readers and prefetch equally from them
template <typename Reader>
void StartPrefetch(std::vector<Reader>& readers, size_t prefetch) {
    for (size_t p = 1; p <= prefetch; ++p) {
        for (Reader& r : readers)
            r.source().Prefetch(p);
    }
}

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_FILE_HEADER

/******************************************************************************/
