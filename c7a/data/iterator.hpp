/*******************************************************************************
 * c7a/data/iterator.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_ITERATOR_HEADER
#define C7A_DATA_ITERATOR_HEADER

#include <vector>
#include "serializer.hpp"
#include "buffer_chain.hpp"
#include "binary_buffer_reader.hpp"

namespace c7a {
namespace data {

//! Iterator gives you access to data of a block
template <class T>
class Iterator
{
public:
    static const bool debug = false;
    //! Creates an instance of iterator that deserializes blobs to T
    explicit Iterator(BufferChain& buffers)
        : buffer_chain_(buffers),
          current_(buffer_chain_.Begin()),
          current_reader_(nullptr, 0) {
        if (current_ != buffer_chain_.End()) {
            current_reader_ = BinaryBufferReader(current_->buffer);
            sLOG << "initialized non-empty iterator";
        }
        else {
            sLOG << "initialized empty iterator";
        }
    }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const T Next() {
        if (current_reader_.empty()) {
            if (current_ != buffer_chain_.End()) {
                MoveToNextBuffer();
            }
            else {
                die("buffer chain element has no follow-up element.");
            }
        }
        return Deserialize<T>(current_reader_.GetString());
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

        if (current_reader_.empty() && current_ != buffer_chain_.End()) {
            MoveToNextBuffer();
        }
        *out_data = nullptr;
        *out_len = 0;
        if (current_reader_.IsNull()) {
            return 0;
        }
        *out_data = (void*)((char*)(current_->buffer.data()) + current_reader_.cursor());
        return current_reader_.SeekStringElements(num_elements, out_len);
    }

    //! returns true if currently at least one element is available
    //! If concurrent read and writes operate on this block, this method might
    //! once return false and then true, if new data arrived.
    inline bool HasNext() {
        // current reader not empty --> read along
        // current reader empty     --> do we have follow-up buffer-chain element?
        //                          and when we move to this buffer, is it empty?
        return !current_reader_.empty() || (current_ != buffer_chain_.End() && LookAhead());
    }

    //! Waits until either an element is accessible (HasNext() == true) or the
    //! buffer chain was closed
    //! Does not enter idle state if HasNext() == true || buffer_chain_.Closed() == true
    void WaitForMore() {
        while (!HasNext() && !buffer_chain_.IsClosed())
            buffer_chain_.Wait();
    }

    //! Waits until all elements are available at the iterator and the BufferChain
    //! is closed
    void WaitForAll() {
        buffer_chain_.WaitUntilClosed();
    }

    //! Indicates whether elements can be appended (not closed) or not (closed).
    //! Returns only true when this iterator instance points after the end of
    //! the according BufferChain
    //! Blocks that are finished once cannot be opened again
    inline bool IsFinished() {
        return !HasNext()
               && (buffer_chain_.size() == 0 || current_ == buffer_chain_.End())
               && buffer_chain_.IsClosed();
    }

private:
    struct BufferChain& buffer_chain_;
    BufferChainIterator current_;
    BinaryBufferReader current_reader_;

    void MoveToNextBuffer() {
        assert(current_ != buffer_chain_.End());

        //reader is initialized with size 0 if BufferChain was empty on creation
        //do not traverse, but instead re-load current buffer into reader.

        if (!current_reader_.IsNull())
            current_++;

        if (current_ == buffer_chain_.End())
            current_reader_ = BinaryBufferReader(nullptr, 0);
        else
            current_reader_ = BinaryBufferReader(current_->buffer);
    }

    //! Edge case: iterator has read until end of buffer,
    //! Follow-up buffer exists --> HasNext has to see if that follow-up buffer
    //! is not empty. We do this here.
    bool LookAhead() {
        MoveToNextBuffer();
        return !current_reader_.empty();
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_ITERATOR_HEADER

/******************************************************************************/
