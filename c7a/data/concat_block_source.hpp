/*******************************************************************************
 * c7a/data/block_reader.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_READER_HEADERX
#define C7A_DATA_BLOCK_READER_HEADERX

#include <c7a/data/block.hpp>

#include <utility>

namespace c7a {
namespace data {

/*!
 * ConcatBlockSource is a BlockSource which concatenates all Blocks available
 * from a vector of BlockSources. They are concatenated in order: first all
 * Blocks from source zero, then from source one, etc.
 */
template <typename BlockSource>
class ConcatBlockSource
{
public:
    using Byte = unsigned char;

    using Block = typename BlockSource::Block;
    using BlockCPtr = std::shared_ptr<const Block>;

    //! Construct a BlockSource which concatenates many other BlockSources.
    explicit ConcatBlockSource(const std::vector<BlockSource>& sources)
        : sources_(sources) { }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    bool NextBlock(const Byte** out_current, const Byte** out_end) {
        if (current_ >= sources_.size()) return false;
        do {
            if (sources_[current_].NextBlock(out_current, out_end))
                return true;
            // current source returned false -> empty
        } while (++current_ < sources_.size());
        return false;
    }

protected:
    //! vector containing block sources
    std::vector<BlockSource> sources_;

    //! current source, all sources < current_ are empty.
    size_t current_ = 0;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
