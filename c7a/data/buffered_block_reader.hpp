/*******************************************************************************
 * c7a/data/buffered:block_reader.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BUFFERED_BLOCK_READER_HEADER
#define C7A_DATA_BUFFERED_BLOCK_READER_HEADER

#include <c7a/data/block_reader.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * Simple block reader that allows reading the first value without advancing. 
 */
template <typename ItemType>
class BufferedBlockReader
{
private:
    File::Reader &reader_;
    ItemType current_;
    bool hasCurrent_;
public:
    bool HasValue() {
        return hasCurrent_;
    }

    ItemType Value() {
        assert(HasValue());

        return current_;
    }

    void Next() {
        hasCurrent_ = reader_.HasNext();
        if(hasCurrent_)
            current_ = reader_.Next<ItemType>();
    }

    BufferedBlockReader(File::Reader &reader) : reader_(reader) {
        Next();    
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_READER_HEADER

/******************************************************************************/
