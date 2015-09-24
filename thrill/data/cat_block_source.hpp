/*******************************************************************************
 * thrill/data/cat_block_source.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_CAT_BLOCK_SOURCE_HEADER
#define THRILL_DATA_CAT_BLOCK_SOURCE_HEADER

#include <thrill/data/block.hpp>

#include <utility>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * CatBlockSource is a BlockSource which concatenates all Blocks available from
 * a vector of BlockSources. They are concatenated in order: first all Blocks
 * from source zero, then from source one, etc.
 */
template <typename BlockSource>
class CatBlockSource
{
public:
    //! default constructor
    CatBlockSource() = default;

    //! Construct a BlockSource which catenates many other BlockSources.
    explicit CatBlockSource(std::vector<BlockSource>&& sources)
        : sources_(std::move(sources)) { }

    //! non-copyable: delete copy-constructor
    CatBlockSource(const CatBlockSource&) = delete;
    //! non-copyable: delete assignment operator
    CatBlockSource& operator = (const CatBlockSource&) = delete;
    //! move-constructor: default
    CatBlockSource(CatBlockSource&&) = default;
    //! move-assignment operator: default
    CatBlockSource& operator = (CatBlockSource&&) = default;

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    Block NextBlock() {
        for ( ; current_ < sources_.size(); ++current_) {
            Block b = sources_[current_].NextBlock();
            if (b.IsValid()) return b;
        }
        return Block();
    }

private:
    //! vector containing block sources
    std::vector<BlockSource> sources_;

    //! current source, all sources < current_ are empty.
    size_t current_ = 0;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CAT_BLOCK_SOURCE_HEADER

/******************************************************************************/
