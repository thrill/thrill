/*******************************************************************************
 * c7a/data/emitter_target.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_EMITTER_TARGET_HEADER
#define C7A_DATA_EMITTER_TARGET_HEADER

namespace c7a {
namespace data {

class BinaryBuffer;

//! Emitter Targets specify the behaviour of an emitter when data is flushed.
//!
//! EmitterTargets can append arbitrary binary buffer instances until they are closed.
//! EmitterTargets can be closed exaclty once.
//! Data that has been appended to a target must be visible at the sink ASAP.
class EmitterTarget
{
public:
    virtual void Close() = 0;
    virtual void Append(BinaryBufferBuilder buffer) = 0;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_EMITTER_TARGET_HEADER

/******************************************************************************/
