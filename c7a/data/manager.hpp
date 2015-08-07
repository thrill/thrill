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

#include <c7a/common/logger.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/repository.hpp>

#include <functional>
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
    explicit Manager(data::ChannelMultiplexer& multiplexer, size_t my_local_worker_id)
        : cmp_(multiplexer), my_local_worker_id_(my_local_worker_id) { }

    //! non-copyable: delete copy-constructor
    Manager(const Manager&) = delete;
    //! non-copyable: delete assignment operator
    Manager& operator = (const Manager&) = delete;
    //! default move constructor
    Manager(Manager&&) = default;

    //! Returns a reference to a new Channel.
    //! This method alters the state of the manager and must be called on all
    //! Workers to ensure correct communication cordination
    ChannelPtr GetNewChannel() {
        return std::move(cmp_.GetOrCreateChannel(cmp_.AllocateNext(my_local_worker_id_), my_local_worker_id_));
    }

    //! Returns a new File object containing a sequence of local Blocks.
    File GetFile() {
        return File();
    }
    //TODO(ts) add FileId Persist(File) method

private:
    static const bool debug = false;
    ChannelMultiplexer& cmp_;
    size_t my_local_worker_id_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_MANAGER_HEADER

/******************************************************************************/
