/*******************************************************************************
 * thrill/data/file.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_FILE_HEADER
#define THRILL_DATA_FILE_HEADER

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
    explicit File(BlockPool& block_pool)
        : BlockSink(block_pool)
    { }

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void AppendBlock(const Block& b) final {
        assert(!closed_);
        if (b.size() == 0) return;
        blocks_.push_back(b);
        num_items_sum_.push_back(num_items() + b.num_items());
        size_ += b.size();
    }

    void Close() final {
        assert(!closed_);
        closed_ = true;
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
    // method for reading multiple successive items.
    template <typename ItemType>
    ItemType GetItemAt(size_t index) const;

    //! Get index of the given item, or the next greater item,
    // in this file. The file has to be ordered according to the
    // given compare function.
    //
    // WARNING: This method uses GetItemAt combined with a binary search and
    // is therefore not efficient. The method will be reimplemented in near future.
    template <typename ItemType, typename CompareFunction = std::greater<ItemType> >
    size_t GetIndexOf(ItemType item, const CompareFunction func = CompareFunction()) const;

    //! Seek in File: return a Block range containing items begin, end of
    //! given type.
    template <typename ItemType>
    std::vector<Block> GetItemRange(size_t begin, size_t end) const;

    //! Read complete File into a std::string, obviously, this should only be
    //! used for debugging!
    std::string ReadComplete() const {
        std::string output;
        for (const Block& b : blocks_)
            output += b.ToString();
        return output;
    }

    //! Output the Block objects contained in this File.
    friend std::ostream& operator << (std::ostream& os, const File& f) {
        os << "[File " << std::hex << &f << std::dec
           << " Blocks=[";
        for (const Block& b : f.blocks_)
            os << "\n    " << b;
        return os << "]]";
    }

protected:
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

/*!
 * A BlockSource to read Blocks from a File. The KeepFileBlockSource mainly contains
 * an index to the current block, which is incremented when the NextBlock() must
 * be delivered.
 */
class KeepFileBlockSource
{
public:
    //! Start reading a File
    KeepFileBlockSource(const File& file,
                        size_t first_block = 0, size_t first_item = keep_first_item)
        : file_(file), first_block_(first_block), first_item_(first_item) {
        current_block_ = first_block_ - 1;
    }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader
    Block NextBlock() {
        ++current_block_;

        if (current_block_ >= file_.num_blocks())
            return Block();

        if (current_block_ == first_block_) {
            // construct first block differently, in case we want to shorten it.
            Block b = file_.block(current_block_);
            if (first_item_ != keep_first_item)
                b.set_begin(first_item_);
            return b;
        }
        else {
            return file_.block(current_block_);
        }
    }

protected:
    //! sentinel value for not changing the first_item item
    static const size_t keep_first_item = size_t(-1);

    //! file to read blocks from
    const File& file_;

    //! index of current block.
    size_t current_block_ = -1;

    //! number of the first block
    size_t first_block_;

    //! offset of first item in first block read
    size_t first_item_;
};

inline
File::KeepReader File::GetKeepReader() const {
    return KeepReader(KeepFileBlockSource(*this, 0));
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
    explicit ConsumeFileBlockSource(File* file)
        : file_(file) { }

    //! non-copyable: delete copy-constructor
    ConsumeFileBlockSource(const ConsumeFileBlockSource&) = delete;
    //! non-copyable: delete assignment operator
    ConsumeFileBlockSource& operator = (const ConsumeFileBlockSource&) = delete;
    //! move-constructor: default
    ConsumeFileBlockSource(ConsumeFileBlockSource&& s)
        : file_(s.file_) { s.file_ = nullptr; }

    //! Get the next block of file.
    Block NextBlock() {
        assert(file_);
        if (file_->blocks_.empty())
            return Block();

        Block b = file_->blocks_.front();
        file_->blocks_.pop_front();
        return b;
    }

    //! Consume unread blocks and reset File to zero items.
    ~ConsumeFileBlockSource() {
        if (file_) {
            file_->blocks_.clear();
            file_->num_items_sum_.clear();
        }
    }

protected:
    //! file to consume blocks from
    File* file_;
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
        KeepFileBlockSource(*this, 0, 0));
}

inline
File::Reader File::GetReader(bool consume) {
    if (consume)
        return ConstructDynBlockReader<ConsumeFileBlockSource>(this);
    else
        return ConstructDynBlockReader<KeepFileBlockSource>(*this, 0);
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
        KeepFileBlockSource(*this, begin_block,
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

    return fr;
}

template <typename ItemType>
ItemType File::GetItemAt(size_t index) const {

    KeepReader reader = this->GetReaderAt<ItemType>(index);
    ItemType val = reader.Next<ItemType>();

    return val;
}

template <typename ItemType, typename CompareFunction>
size_t File::GetIndexOf(ItemType item, const CompareFunction comperator) const {

    static const bool debug = false;

    LOG << "Looking for item " << item;

    // Use a binary search to find the item.
    size_t left = 0;
    size_t right = num_items();

    while (left < right - 1) {
        size_t mid = (right + left) / 2;
        ItemType cur = this->GetItemAt<ItemType>(mid);
        if (comperator(cur, item)) {
            right = mid;
        }
        else {
            left = mid;
        }
    }

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
