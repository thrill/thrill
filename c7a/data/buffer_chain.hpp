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
    BufferChainElement(BinaryBuffer b) : buffer(b) { }
    BinaryBuffer buffer;
};

using BufferChainIterator = std::deque<BufferChainElement>::const_iterator;

//! A Buffer chain holds multiple immuteable buffers.
//! Append in O(1), Delete in O(num_buffers)
struct BufferChain : public EmitterTarget {
    BufferChain() : closed_(false) { }

    //! Appends a BinaryBuffer to this BufferChain.
    //! This method is thread-safe and runs in O(1)
    void Append(BinaryBuffer b) {
        std::unique_lock<std::mutex> lock(append_mutex_);
        elements_.emplace_back(BufferChainElement(b));
        lock.unlock();
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

    BufferChainIterator Begin() {
        return elements_.begin();
    }

    BufferChainIterator End() {
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

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFER_CHAIN_HEADER

/******************************************************************************/
