/*******************************************************************************
 * c7a/data/buffer_chain.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BUFFER_CHAIN_HEADER
#define C7A_DATA_BUFFER_CHAIN_HEADER

#include <vector>
#include <memory> //unique_ptr

#include <c7a/data/binary_buffer.hpp>

namespace c7a {
namespace data {
//! Elements of a singly linked list, holding a immuteable buffer
struct BufferChainElement {
    BufferChainElement(BinaryBuffer b) : next(nullptr), buffer(b) { }

    inline bool              IsEnd() const { return next == nullptr; }

    struct BufferChainElement* next;
    BinaryBuffer             buffer;
};

//! A Buffer chain holds multiple immuteable buffers.
//! Append in O(1), Delete in O(#buffers)
struct BufferChain {
    BufferChain() : head(nullptr), tail(nullptr), closed(false) { }

    void                     Append(BinaryBuffer b)
    {
        if (tail == nullptr) {
            head = new BufferChainElement(b);
            tail = head;
        }
        else {
            tail->next = new BufferChainElement(b);
            tail = tail->next;
        }
    }

    //! Call buffers' destructors and deconstructs the chain
    void                     Delete()
    {
        BufferChainElement* current = head;
        while (current != nullptr) {
            BufferChainElement* next = current->next;
            current->buffer.Delete();
            current = next;
        }
    }

    void                     Close()
    {
        closed = true;
    }

    struct BufferChainElement* head;
    struct BufferChainElement* tail;
    bool                     closed;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFER_CHAIN_HEADER

/******************************************************************************/
