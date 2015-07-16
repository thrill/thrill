/*******************************************************************************
 * c7a/data/emitter.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_EMITTER_HEADER
#define C7A_DATA_EMITTER_HEADER

#include "dyn_block_writer.hpp"

namespace c7a {
namespace data {

//! Emitter lets you push elements to a downstream operation or network channel.
//! Template parameter specifies the type of element that is accepted.
//! The emitter will serialize the data and put it into the emitter target.
//! Emitters can be flushed to enforce data movement to the sink.
//! Emitters can be closed exacly once.
//! Data sinks can chekc whether all emitters to that sink are closed.
//
// TODO(ts): make special version for fix-length elements
using Emitter = DynBlockWriter<default_block_size>;
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_EMITTER_HEADER

/******************************************************************************/
