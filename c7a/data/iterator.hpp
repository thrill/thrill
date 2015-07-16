/*******************************************************************************
 * c7a/data/iterator.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_ITERATOR_HEADER
#define C7A_DATA_ITERATOR_HEADER

#include <vector>
#include "serializer.hpp"

namespace c7a {
namespace data {

//! Iterator gives you access to data of a block
template <class T, class BlockSource = FileBlockSource<default_block_size>>
class Iterator
{
public:
    static const bool debug = false;
    //! Creates an instance of iterator that deserializes blobs to T
    explicit Iterator(BlockSource& source)
        : reader_(source) {
    }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const T Next() {
        return reader_.Next();
    }

    //! Seeks the next num_elements elements in the BufferChain
    //! out_data and out_len specify the memory block that contains the elements.
    //! Since the elements are continuous in memory, the number of seeked
    //! elements may be smaller than num_elements. In this case, a subsequent
    //! call is required.
    //! \param num_elements max number of elements to seek
    //! \param out_data base address of result. Is nullptr if return value is 0
    //! \param out_len number of bytes in the memory segment
    //! \returns the number of elements in the seeked block.
    size_t Seek(size_t num_elements, void** out_data, size_t* out_len) {
        //TODO(ts) case for fixed-size elements
        /* TODO(ts) re-implement with new block concept
        if (current_reader_.empty() && current_ != buffer_chain_.End()) {
            MoveToNextBuffer();
        }
        *out_data = nullptr;
        *out_len = 0;
        if (current_reader_.IsNull()) {
            return 0;
        }
        *out_data = (void*)((char*)(current_->buffer.data()) + current_reader_.cursor());
        */
        //return current_reader_.SeekStringElements(num_elements, out_len);
        abort(); // FIXME LATER
    }

    //! returns true if currently at least one element is available
    //! If concurrent read and writes operate on this block, this method might
    //! once return false and then true, if new data arrived.
    inline bool HasNext() {
        return reader_.HasNext();
    }

    //! Indicates whether elements can be appended (not closed) or not (closed).
    //! Returns only true when this iterator instance points after the end of
    //! the according BufferChain
    //! Blocks that are finished once cannot be opened again
    inline bool IsFinished() {
        return reader_.closed();
    }

private:
    BlockReader<BlockSource> reader_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_ITERATOR_HEADER

/******************************************************************************/
