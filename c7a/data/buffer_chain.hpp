/*******************************************************************************
 * c7a/data/buffer_chain.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BUFFER_CHAIN_HEADER
#define C7A_DATA_BUFFER_CHAIN_HEADER

#include <deque>
#include <condition_variable>
#include <mutex> //mutex, unique_lock

#include <c7a/data/binary_buffer.hpp>
#include <c7a/data/emitter_target.hpp>

namespace c7a {
namespace data {

//! Elements of a singly linked list, holding a immuteable buffer
struct BufferChainElement {
    BufferChainElement(BinaryBuffer b, size_t element_count, size_t offset = 0)
        : buffer(b),
          element_count(element_count),
          offset_of_first(offset) {
        //TODO(ts, sl) implement offset logic
        assert(offset_of_first == 0);     //no support for offset right now
    }

    //! BinaryBuffer holds the data
    BinaryBuffer buffer;

    //! prefixsum of number of elements of previous BufferChainElements and
    //! this BufferChainElement
    size_t       element_count;

    //! offset to the first element in the BinaryBuffer
    //! The cut-off element before that offset is not included in the element_count
    size_t       offset_of_first;
};

using BufferChainIterator = std::deque<BufferChainElement>::const_iterator;

//! A Buffer chain holds multiple immuteable buffers.
//! Append in O(1), Delete in O(num_buffers)
struct BufferChain : public EmitterTarget {
    BufferChain() : closed_(false) { }

    //! Appends a BinaryBufferBuffer's content to the chain
    //! This method is thread-safe and runs in O(1)
    //! \param b buffer to append
    void Append(BinaryBufferBuilder& b) {
        std::unique_lock<std::mutex> lock(append_mutex_);
        elements_.emplace_back(BufferChainElement(BinaryBuffer(b), size() + b.elements()));
        b.Detach();
        lock.unlock();
        NotifyWaitingThreads();
    }

    //! Appends an existing element to the chain.
    //! This method is not thread-safe since it is called only when channels
    //! are closed once.
    void Append(BufferChainElement&& element) {
        elements_.emplace_back(std::move(element));
        NotifyWaitingThreads();
    }

    //! Waits until beeing notified
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_variable_.wait(lock);
    }

    //! Waits until beeing notified and closed == true
    void WaitUntilClosed() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_variable_.wait(lock, [=]() { return this->closed_; });
    }

    //! Call buffers' destructors and deconstructs the chain
    void Delete() {
        std::unique_lock<std::mutex> lock(append_mutex_);
        for (auto& elem : elements_)
            elem.buffer.Delete();
    }

    //! Returns the number of elements in this BufferChain at the current
    //! state.
    size_t size() {
        if (elements_.empty())
            return 0;
        return elements_.back().element_count;
    }

    BufferChainIterator Begin() const {
        return elements_.begin();
    }

    BufferChainIterator End() const {
        return elements_.end();
    }

    void Close() {
        closed_ = true;
        NotifyWaitingThreads();
    }

    bool IsClosed() { return closed_; }

    std::deque<BufferChainElement> elements_;

private:
    std::mutex                     mutex_;
    std::mutex                     append_mutex_;
    std::condition_variable        condition_variable_;
    bool                           closed_;

    void NotifyWaitingThreads() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_variable_.notify_all();
    }
};

//! Collects buffers ina map and moves them to a BufferChain in the order of
//! the keys. Buffers with the same key are moved in the order they where
//! appended to the OrderedBufferChain
class OrderedBufferChain
{
public:
    //! Appends data from the BinaryBufferBuilder and detaches it
    //! \param rank of the sender of the data
    //! \param b received data
    void Append(size_t rank, BinaryBufferBuilder& b) {
        std::unique_lock<std::mutex> lock(append_mutex_);
        if (buffers_.find(rank) == buffers_.end())
            buffers_[rank] = std::vector<BufferChainElement>();
        buffers_[rank].emplace_back(BufferChainElement(BinaryBuffer(b), b.elements()));
        b.Detach();
    }

    void MoveTo(std::shared_ptr<data::BufferChain> target) {
        size_t elements = 0;

        //iterate over keys in their order
        for (auto it = buffers_.begin(); it != buffers_.end(); it++) {
            //for each key - iterate over all it's buffers
            for (auto& buf_elem : it->second) {
                //update the elements field
                elements += buf_elem.element_count;
                target->Append(BufferChainElement(buf_elem.buffer, elements));
            }
        }
    }

private:
    std::map<size_t, std::vector<BufferChainElement> > buffers_;
    std::mutex append_mutex_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFER_CHAIN_HEADER

/******************************************************************************/
