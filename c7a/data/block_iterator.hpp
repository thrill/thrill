/*******************************************************************************
 * c7a/data/block_iterator.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_ITERATOR_HEADER
#define C7A_DATA_BLOCK_ITERATOR_HEADER

#include <vector>
#include "serializer.hpp"
#include "buffer_chain.hpp"
#include "binary_buffer_reader.hpp"

namespace c7a {
namespace data {
//! BlockIterator gives you access to data of a block
template <class T>
class BlockIterator
{
public:
    //! Creates an instance of iterator that deserializes blobs to T
    explicit BlockIterator(const BufferChain& buffers)
        : buffer_chain_(buffers),
          current_(buffers.head),
          current_reader_(nullptr, 0),
          late_init_(false)
    {
        if (current_ != nullptr) {
            current_reader_ = BinaryBufferReader(current_->buffer);
        }
        else {
            late_init_ = true;
        }
    }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const T Next()
    {
        if (current_reader_.empty()) {
            if (current_ != nullptr && !current_->IsEnd()) {
                MoveToNextBuffer();
            }
            else {
                throw "buffer chain element has no follow-up element.";
            }
        }
        return Deserialize<T>(current_reader_.GetString());
    }

    //! returns true if currently at least one element is available
    //! If concurrent read and writes operate on this block, this method might
    //! once return false and then true, if new data arrived.
    inline bool HasNext()
    {
        check_late_init();
        return !current_reader_.empty() || (current_ != nullptr && !current_->IsEnd());
    }

    inline void check_late_init()
    {
        if (late_init_)
        {
            current_ = buffer_chain_.head;
            if (current_ != nullptr) {
                current_reader_ = BinaryBufferReader(current_->buffer);
                late_init_ = false;
            }
        }
    }

    //! Indicates whether elements can be appended (not closed) or not (closed).
    //! Blocks that are closed once cannot be opened again
    inline bool IsClosed() const
    {
        return buffer_chain_.closed;
    }

private:
    const struct BufferChain& buffer_chain_;
    const BufferChainElement* current_;
    BinaryBufferReader current_reader_;
    bool late_init_; //problem when iterator is created before emitter has flushed values

    void MoveToNextBuffer()
    {
        assert(!current_->IsEnd());
        current_ = current_->next;
        current_reader_ = BinaryBufferReader(current_->buffer);
    }
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_ITERATOR_HEADER

/******************************************************************************/
