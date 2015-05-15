/*******************************************************************************
 * c7a/data/block_emitter.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_EMITTER_HEADER
#define C7A_DATA_BLOCK_EMITTEr_HEADER

#include <c7a/net/binary_builder.hpp>
#include "buffer_chain.hpp"
#include "serializer.hpp"

namespace c7a {
namespace data {
//! BlockIterator gives you access to data of a block
//TODO specialize the emitter to be more fancy when havein fixe-length elements
template <class T, typename TargetType = BufferChain>
class BlockEmitter
{
public:
    BlockEmitter(TargetType& target)
        : builder_(net::BinaryBuffer::DEFAULT_SIZE),
          target_(target) { }

    void operator () (T x)
    {
        if (builder_.size() + sizeof(T) > builder_.capacity()) { //prevent reallocation
            Flush();
        }
        builder_.PutString(Serialize<T>(x));
    }

    //! Flushes and closes the block (cannot be undone)
    //! No further emitt operations can be done afterwards.
    void Close()
    {
        Flush();
        target_.Close();
    }

    //! Writes the data to the target without closing the emitter
    void Flush()
    {
        target_.Append(net::BinaryBuffer(builder_));
        builder_.Detach();
        builder_.Reserve(net::BinaryBuffer::DEFAULT_SIZE);
    }

private:
    net::BinaryBuilder builder_;
    TargetType& target_;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_EMITTER_HEADER

/******************************************************************************/
