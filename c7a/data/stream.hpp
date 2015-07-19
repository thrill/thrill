/*******************************************************************************
 * c7a/data/stream.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_STREAM_HEADER
#define C7A_DATA_STREAM_HEADER

#include <functional>
#include <vector>
#include <memory>

#include <c7a/common/logger.hpp>
#include <c7a/data/file.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

//! Stream is a BlockWriter-Target that triggers callbacks it's observer.
//! There can be multiple observers for each stream, but at least one.
//! Observers cannot be removed once added.
template <size_t BlockSize>
class Stream
{
public:
    using BlockPtr = std::shared_ptr<Block<BlockSize> >;
    using AppendCallbackFunction = std::function<void(const Stream<BlockSize>&, const BlockPtr&, size_t, size_t, size_t)>;
    using CloseCallbackFunction = std::function<void(const Stream<BlockSize>&)>;

    //! Append a block to this file, the block must contain given number of
    //! items after the offset first.
    void Append(const BlockPtr& block, size_t block_used,
                size_t nitems, size_t first) const {
        die_unless(!closed_);
        die_unless(append_observers_.size() > 0);

        for (auto& o : append_observers_) {
            o(*this, block, block_used, nitems, first);
        }
    }

    void OnAppend(AppendCallbackFunction callback_function) {
        append_observers_.push_back(callback_function);
    }

    void OnClose(CloseCallbackFunction callback_function) {
        close_observers_.push_back(callback_function);
    }

    void Close() {
        die_unless(!closed_);
        for (auto& o : close_observers_) {
            o(*this);
        }
    }

private:
    //! closed streams won't fire callbacks nor accept 'Append' / 'Close' calls
    bool closed_ = { false };

    std::vector<AppendCallbackFunction> append_observers_;
    std::vector<CloseCallbackFunction> close_observers_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_STREAM_HEADER

/******************************************************************************/
