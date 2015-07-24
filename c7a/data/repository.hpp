/*******************************************************************************
 * c7a/data/repository.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_REPOSITORY_HEADER
#define C7A_DATA_REPOSITORY_HEADER

#include <c7a/common/logger.hpp>

#include <cassert>
#include <map>
#include <memory>
#include <mutex>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

using DataId = size_t;

template <class Target>
class Repository
{
public:
    //!Alllocates the next data target.
    //! Calls to this method alter the internal state -> order of calls is
    //! important and must be deterministic
    DataId AllocateNext() {
        std::lock_guard<std::mutex> lock(allocate_next_mutex_);
        DataId result = next_id_++;
        GetOrAllocate(result);
        return result;
    }

    //! Allocates a data target with the given ID
    //! Use this only for internal purpose.
    //! \param id of the target to retrieve
    //! \exception std::runtime_exception if id is already contained
    DataId Allocate(const DataId& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        sLOG << "allocate" << id;
        if (Contains(id)) {
            throw new std::runtime_error("duplocate data target allocation w/ explicit id");
        }
        data_[id] = std::make_shared<Target>();
        return id;
    }

    //! Indicates if a data target exists with the given id
    bool Contains(const DataId& id) {
        return data_.find(id) != data_.end();
    }

    //! Returns the data target with the given ID
    //! \ param id of the data to retrieve
    //! \exception std::runtime_error if id is not contained
    std::shared_ptr<Target> operator () (const DataId& id) {
        sLOG << "data" << id;
        if (!Contains(id)) {
            throw new std::runtime_error("data id is unknwon");
        }
        return data_[id];
    }

    //! Gets or allocates the data target with the given id in an atomic
    //! fashion.
    //! \returns a shared ptr to the created/retrieved data target
    std::shared_ptr<Target> GetOrAllocate(const DataId& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!Contains(id))
            data_[id] = std::make_shared<Target>();
        return data_[id];
    }

private:
    static const bool debug = false;
    DataId next_id_;
    std::map<DataId, std::shared_ptr<Target> > data_;
    std::mutex mutex_;
    std::mutex allocate_next_mutex_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_REPOSITORY_HEADER

/******************************************************************************/
