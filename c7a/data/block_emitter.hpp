/*******************************************************************************
 * c7a/data/block_emitter.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_EMITTER_HEADER
#define C7A_DATA_BLOCK_EMITTER_HEADER

#include "binary_buffer_builder.hpp"
#include "buffer_chain.hpp"
#include "serializer.hpp"

namespace c7a {
namespace data {
//! BlockIterator gives you access to data of a block
//TODO specialize the emitter to be more fancy when havein fixe-length elements
template <class T>
class BlockEmitter {
public:
    BlockEmitter(std::shared_ptr<EmitterTarget> target)
        : builder_(BinaryBuffer::DEFAULT_SIZE),
          target_(target) { }

    void operator () (T x) {
        if (builder_.size() + sizeof(T) > builder_.capacity()) { //prevent reallocation
            Flush();
        }
        // TODO(ts): the String content may be a lot bigger than
        // sizeof(T). hence the block may overflow/reallocate.
        builder_.PutString(Serialize<T>(x));
    }

    //! Flushes and closes the block (cannot be undone)
    //! No further emitt operations can be done afterwards.
    void Close() {
        Flush();
        target_->Close();
    }

    //! Writes the data to the target without closing the emitter
    void Flush() {
        target_->Append(BinaryBuffer(builder_));
        builder_.Detach();
        builder_.Reserve(BinaryBuffer::DEFAULT_SIZE);
    }

private:
    BinaryBufferBuilder builder_;
    std::shared_ptr<EmitterTarget> target_;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_EMITTER_HEADER

/******************************************************************************/
