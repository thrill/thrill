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
#define C7A_DATA_BLOCK_EMITTER_HEADER

#include "binary_buffer_builder.hpp"
#include "buffer_chain.hpp"
#include "serializer.hpp"

namespace c7a {
namespace data {
//! BlockEmitter lets you push elements to a downstream operation or network channel.
//! Template parameter specifies the type of element that is accepted.
//! The emitter will serialize the data and put it into the emitter target.
//! Emitters can be flushed to enforce data movement to the sink.
//! Emitters can be closed exacly once.
//! Data sinks can chekc whether all emitters to that sink are closed.
//
// TODO(ts): make special version for fix-length elements
template <class T>
class BlockEmitter
{
public:
    BlockEmitter(std::shared_ptr<EmitterTarget> target)
        : builder_(BinaryBuffer::DEFAULT_SIZE),
          target_(target) { }

    //! Emitts an element
    void operator () (T x) {
        if (builder_.size() + sizeof(T) > builder_.capacity()) { //prevent reallocation
            Flush();
        }
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
