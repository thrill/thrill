/*******************************************************************************
 * thrill/data/file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_FILE_HEADER
#define THRILL_DATA_FILE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/future.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/buffered_block_reader.hpp>
#include <thrill/data/dyn_block_reader.hpp>

#include <cassert>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

class KeepFileBlockSource;
class ConsumeFileBlockSource;
class CachingBlockQueueSource;

/*!
 * A File or generally File<> is an ordered sequence of
 * Block objects for storing items. By using the Block
 * indirection, the File can be composed using existing Block objects (via
 * reference counting), but only contain a subset of the items in those
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
class File : public virtual BlockSink
{
public:
    using Writer = BlockWriter<File>;
    using Reader = DynBlockReader;
    using KeepReader = BlockReader<KeepFileBlockSource>;
    using ConsumeReader = BlockReader<ConsumeFileBlockSource>;
    using DynWriter = DynBlockWriter;

    //! Constructor from BlockPool
    explicit File(BlockPool& block_pool, size_t local_worker_id)
        : BlockSink(block_pool, local_worker_id)
    { }

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(const PinnedBlock& b) final {
        assert(!closed_);
        if (b.size() == 0) return;
        blocks_.push_back(b);
        num_items_sum_.push_back(num_items() + b.num_items());
        size_ += b.size();
    }

    void Close() final {
        assert(!closed_);
        // 2016-02-04: Files are never closed, one can always append -tb.
        // closed_ = true;
    }

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    enum { allocate_can_fail_ = false };

    // returns a string that identifies this string instance
    std::string ToString() {
        return "File@" + std::to_string(reinterpret_cast<uintptr_t>(this));
    }

    bool closed() const {
        return closed_;
    }

    //! Return the number of blocks
    size_t num_blocks() const { return blocks_.size(); }

    //! Return the number of items in the file
    size_t num_items() const {
        return num_items_sum_.size() ? num_items_sum_.back() : 0;
    }

    //! Returns true if the File is empty.
    bool empty() const {
        return blocks_.empty();
    }

    //! Return the number of bytes of user data in this file.
    size_t total_size() const { return size_; }

    //! Return shared pointer to a block
    const Block & block(size_t i) const {
        assert(i < blocks_.size());
        return blocks_[i];
    }

    //! Return number of items starting in block i
    size_t ItemsStartIn(size_t i) const {
        assert(i < blocks_.size());
        return num_items_sum_[i] - (i == 0 ? 0 : num_items_sum_[i - 1]);
    }

    //! Get BlockWriter.
    Writer GetWriter(size_t block_size = default_block_size) {
        return Writer(this, block_size);
    }

    //! Get BlockWriterPtr.
    std::shared_ptr<Writer> GetWriterPtr(size_t block_size = default_block_size) {
        return std::make_shared<Writer>(this, block_size);
    }

    //! Get BlockWriter.
    DynWriter GetDynWriter(size_t block_size = default_block_size) {
        return DynWriter(this, block_size);
    }

    /*!
     * Get BlockReader or a consuming BlockReader for beginning of File
     *
     * \attention If consume is true, the reader consumes the File's contents
     * UNCONDITIONALLY, the File will always be emptied whether all items were
     * read via the Reader or not.
     */
    Reader GetReader(bool consume);

    //! Get BlockReader for beginning of File
    KeepReader GetKeepReader() const;

    /*!
     * Get consuming BlockReader for beginning of File
     *
     * \attention The reader consumes the File's contents UNCONDITIONALLY, the
     * File will always be emptied whether all items were read via the Reader or
     * not.
     */
    ConsumeReader GetConsumeReader();

    //! Get BufferedBlockReader for beginning of File
    template <typename ValueType>
    BufferedBlockReader<ValueType, KeepFileBlockSource> GetBufferedReader() const;

    //! Get BlockReader seeked to the corresponding item index
    template <typename ItemType>
    KeepReader GetReaderAt(size_t index) const;

    //! Get item at the corresponding position. Do not use this
    //! method for reading multiple successive items.
    template <typename ItemType>
    ItemType GetItemAt(size_t index) const;

    //! Get index of the given item, or the next greater item,
    //! in this file. The file has to be ordered according to the
    //! given compare function. The tie value can be used to
    //! make a decision in case of many successive equal elements.
    //! The tie is compared with the local rank of the element.
    //!
    //! WARNING: This method uses GetItemAt combined with a binary search and
    //! is therefore not efficient. The method will be reimplemented in near future.
    template <typename ItemType, typename CompareFunction = std::less<ItemType> >
    size_t GetIndexOf(const ItemType& item, size_t tie,
                      const CompareFunction& func = CompareFunction()) const;

    //! Seek in File: return a Block range containing items begin, end of
    //! given type.
    template <typename ItemType>
    std::vector<Block> GetItemRange(size_t begin, size_t end) const;

    //! Read complete File into a std::string, obviously, this should only be
    //! used for debugging!
    std::string ReadComplete() const {
        std::string output;
        for (const Block& b : blocks_)
            output += b.PinWait(0).ToString();
        return output;
    }

    //! Output the Block objects contained in this File.
    friend std::ostream& operator << (std::ostream& os, const File& f) {
        os << "[File " << std::hex << &f << std::dec
           << " Blocks=[";
        size_t i = 0;
        for (const Block& b : f.blocks_)
            os << "\n    " << i++ << " " << b;
        return os << "]]";
    }

private:
    //! the container holding blocks and thus shared pointers to all byte
    //! blocks.
    std::deque<Block> blocks_;

    //! inclusive prefixsum of number of elements of blocks, hence
    //! num_items_sum_[i] is the number of items starting in all blocks preceding
    //! and including the i-th block.
    std::deque<size_t> num_items_sum_;

    //! Total size of this file in bytes. Sum of all block sizes.
    size_t size_ = 0;

    //! for access to blocks_ and used_
    friend class data::KeepFileBlockSource;
    friend class data::ConsumeFileBlockSource;

    //! Closed files can not be altered
    bool closed_ = false;
};

using FilePtr = std::shared_ptr<File>;

/*!
 * A BlockSource to read Blocks from a File. The KeepFileBlockSource mainly contains
 * an index to the current block, which is incremented when the NextBlock() must
 * be delivered.
 */
class KeepFileBlockSource
{
public:
    static const size_t default_prefetch = 2;

    //! Start reading a File
    KeepFileBlockSource(
        const File& file, size_t num_prefetch = default_prefetch,
        size_t first_block = 0, size_t first_item = keep_first_item)
        : file_(file), num_prefetch_(num_prefetch),
          first_block_(first_block), current_block_(first_block),
          first_item_(first_item) { }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader
    PinnedBlock NextBlock() {

        if (current_block_ >= file_.num_blocks() && fetching_blocks_.empty())
            return PinnedBlock();

        if (num_prefetch_ == 0)
        {
            // operate without prefetching
            return NextUnpinnedBlock().PinWait(file_.local_worker_id());
        }
        else
        {
            // prefetch #desired blocks
            while (fetching_blocks_.size() < num_prefetch_ &&
                   current_block_ < file_.num_blocks())
            {
                fetching_blocks_.emplace_back(
                    NextUnpinnedBlock().Pin(file_.local_worker_id()));
            }

            // this might block if the prefetching is not finished
            fetching_blocks_.front().wait();

            PinnedBlock b = fetching_blocks_.front().get();
            fetching_blocks_.pop_front();
            return b;
        }
    }

protected:
    //! Determine current unpinned Block to deliver via NextBlock()
    Block NextUnpinnedBlock() {
        if (current_block_ == first_block_) {
            // construct first block differently, in case we want to shorten it.
            Block b = file_.block(current_block_++);
            if (first_item_ != keep_first_item)
                b.set_begin(first_item_);
            return b;
        }
        else {
            return file_.block(current_block_++);
        }
    }

private:
    //! sentinel value for not changing the first_item item
    static const size_t keep_first_item = size_t(-1);

    //! file to read blocks from
    const File& file_;

    //! number of block prefetch operations
    size_t num_prefetch_;

    //! current prefetch operations
    std::deque<std::future<PinnedBlock> > fetching_blocks_;

    //! number of the first block
    size_t first_block_;

    //! index of current block.
    size_t current_block_;

    //! offset of first item in first block read
    size_t first_item_;
};

inline
File::KeepReader File::GetKeepReader() const {
    return KeepReader(KeepFileBlockSource(*this));
}

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
    //! Start reading a File
    //! Creates a source for the given file and set the number of blocks
    //! that should be prefetched. 0 means that no blocks are prefetched.
    explicit ConsumeFileBlockSource(File* file, size_t num_prefetch = 2)
        : file_(file), num_prefetch_(num_prefetch) { }

    //! non-copyable: delete copy-constructor
    ConsumeFileBlockSource(const ConsumeFileBlockSource&) = delete;
    //! non-copyable: delete assignment operator
    ConsumeFileBlockSource& operator = (const ConsumeFileBlockSource&) = delete;
    //! move-constructor: default
    ConsumeFileBlockSource(ConsumeFileBlockSource&& s)
        : file_(s.file_), num_prefetch_(s.num_prefetch_),
          fetching_blocks_(std::move(s.fetching_blocks_)) { s.file_ = nullptr; }

    //! Get the next block of file.
    PinnedBlock NextBlock() {
        assert(file_);
        if (file_->blocks_.empty() && fetching_blocks_.empty())
            return PinnedBlock();

        // operate without prefetching
        if (num_prefetch_ == 0) {
            std::future<PinnedBlock> f =
                file_->blocks_.front().Pin(file_->local_worker_id());
            file_->blocks_.pop_front();
            f.wait();
            return f.get();
        }

        // prefetch #desired blocks
        while (fetching_blocks_.size() < num_prefetch_ && !file_->blocks_.empty()) {
            fetching_blocks_.emplace_back(
                file_->blocks_.front().Pin(file_->local_worker_id()));
            file_->blocks_.pop_front();
        }

        // this might block if the prefetching is not finished
        fetching_blocks_.front().wait();

        PinnedBlock b = fetching_blocks_.front().get();
        fetching_blocks_.pop_front();
        return b;
    }

    //! Consume unread blocks and reset File to zero items.
    ~ConsumeFileBlockSource() {
        if (file_) {
            file_->blocks_.clear();
            file_->num_items_sum_.clear();
        }
    }

private:
    //! file to consume blocks from (ptr to make moving easier)
    File* file_;

    //! number of block prefetch operations
    size_t num_prefetch_;

    //! current prefetch operations
    std::deque<std::future<PinnedBlock> > fetching_blocks_;
};

inline
File::ConsumeReader File::GetConsumeReader() {
    return ConsumeReader(ConsumeFileBlockSource(this));
}

template <typename ValueType>
inline
BufferedBlockReader<ValueType, KeepFileBlockSource>
File::GetBufferedReader() const {
    return BufferedBlockReader<ValueType, KeepFileBlockSource>(
        KeepFileBlockSource(*this));
}

inline
File::Reader File::GetReader(bool consume) {
    if (consume)
        return ConstructDynBlockReader<ConsumeFileBlockSource>(this);
    else
        return ConstructDynBlockReader<KeepFileBlockSource>(*this);
}

//! Get BlockReader seeked to the corresponding item index
template <typename ItemType>
typename File::KeepReader
File::GetReaderAt(size_t index) const {
    static const bool debug = false;

    // perform binary search for item block with largest exclusive size
    // prefixsum less or equal to index.
    auto it =
        std::lower_bound(num_items_sum_.begin(), num_items_sum_.end(), index);

    if (it == num_items_sum_.end())
        die("Access beyond end of File?");

    size_t begin_block = it - num_items_sum_.begin();

    sLOG << "item" << index << "in block" << begin_block
         << "psum" << num_items_sum_[begin_block]
         << "first_item" << blocks_[begin_block].first_item_absolute();

    // start Reader at given first valid item in located block
    KeepReader fr(
        KeepFileBlockSource(*this, KeepFileBlockSource::default_prefetch,
                            begin_block,
                            blocks_[begin_block].first_item_absolute()));

    // skip over extra items in beginning of block
    size_t items_before = it == num_items_sum_.begin() ? 0 : *(it - 1);

    sLOG << "items_before" << items_before << "index" << index
         << "delta" << (index - items_before);
    assert(items_before <= index);

    // use fixed_size information to accelerate jump.
    if (Serialization<KeepReader, ItemType>::is_fixed_size)
    {
        const size_t skip_items = index - items_before;
        fr.Skip(skip_items,
                skip_items * ((KeepReader::self_verify ? sizeof(size_t) : 0) +
                              Serialization<KeepReader, ItemType>::fixed_size));
    }
    else
    {
        for (size_t i = items_before; i < index; ++i) {
            if (!fr.HasNext())
                die("Underflow in GetItemRange()");
            fr.template Next<ItemType>();
        }
    }

    sLOG << "after seek at" << fr.CopyBlock();

    return fr;
}

template <typename ItemType>
ItemType File::GetItemAt(size_t index) const {

    KeepReader reader = this->GetReaderAt<ItemType>(index);
    ItemType val = reader.Next<ItemType>();

    return val;
}

template <typename ItemType, typename CompareFunction>
size_t File::GetIndexOf(
    const ItemType& item, size_t tie, const CompareFunction& less) const {

    static const bool debug = false;

    static_assert(
        std::is_convertible<
            bool,
            typename common::FunctionTraits<CompareFunction>::result_type
            >::value,
        "Comperator must return int.");

    LOG << "Looking for item " << item;
    LOG << "Looking for tie " << tie;
    LOG << "Len: " << num_items();

    // Use a binary search to find the item.
    size_t left = 0;
    size_t right = num_items();

    while (left < right) {
        size_t mid = (right + left) >> 1;
        LOG << "Left: " << left;
        LOG << "Right: " << right;
        LOG << "Mid: " << mid;
        ItemType cur = GetItemAt<ItemType>(mid);
        LOG << "Item at mid: " << cur;
        if (less(item, cur) || (!less(item, cur) && !less(cur, item) && tie <= mid)) {
            right = mid;
        }
        else {
            left = mid + 1;
        }
    }

    LOG << "Found element at: " << left;

    return left;
}

//! Seek in File: return a Block range containing items begin, end of
//! given type.
template <typename ItemType>
std::vector<Block>
File::GetItemRange(size_t begin, size_t end) const {
    assert(begin <= end);
    // deliver array of remaining blocks
    return GetReaderAt<ItemType>(begin)
           .template GetItemBatch<ItemType>(end - begin);
}

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_FILE_HEADER

/******************************************************************************/
