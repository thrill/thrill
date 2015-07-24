/*******************************************************************************
 * c7a/data/manager.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_MANAGER_HEADER
#define C7A_DATA_MANAGER_HEADER

#include <c7a/api/input_line_iterator.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/repository.hpp>

#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

//! Manages all kind of memory for data elements
//!
//!
//! Provides Channel creation for sending / receiving data from other workers.
class Manager
{
public:
    explicit Manager(net::DispatcherThread& dispatcher)
        : cmp_(dispatcher) { }

    //! non-copyable: delete copy-constructor
    Manager(const Manager&) = delete;
    //! non-copyable: delete assignment operator
    Manager& operator = (const Manager&) = delete;

    //! Connect net::Group. Forwarded To ChannelMultiplexer.
    void Connect(net::Group* group) {
        cmp_.Connect(group);
    }

#if FIXUP_LATER
    //! Docu see net::ChannelMultiplexer::Scatter()
    template <class T>
    void Scatter(const ChainId& source, const ChainId& target, std::vector<size_t> offsets) {
        assert(source.type == LOCAL);
        assert(target.type == NETWORK);
        assert(dias_.Contains(source));
        cmp_.Scatter<T>(dias_.Chain(source), target, offsets);
    }
#endif      // FIXUP_LATER

    //! Returns a reference to an existing Channel.
    ChannelPtr GetChannel(const ChannelId id) {
        assert(cmp_.HasChannel(id));
        return std::move(cmp_.GetOrCreateChannel(id));
    }

    //! Returns a reference to a new Channel.
    //! This method alters the state of the manager and must be called on all
    //! Workers to ensure correct communication cordination
    ChannelPtr GetNewChannel() {
        return std::move(cmp_.GetOrCreateChannel(cmp_.AllocateNext()));
    }

    //! Returns a new File object containing a sequence of local Blocks.
    File GetFile() {
        return File();
    }
    //TODO(ts) add FileId Persist(File) method

private:
    static const bool debug = false;
    ChannelMultiplexer<default_block_size> cmp_;

    Repository<File> files_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_MANAGER_HEADER

/******************************************************************************/
