/*******************************************************************************
 * c7a/data/repository.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_REPOSITORY_HEADER
#define C7A_DATA_REPOSITORY_HEADER

#include <c7a/common/logger.hpp>
#include <cassert>
#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

template <class Object>
class Repository
{
public:
    using IdPair = std::pair<size_t, size_t>;
    using ObjectPtr = std::shared_ptr<Object>;

    //! construct with initial ids 0.
    explicit Repository(size_t num_workers_per_node)
        : next_id_(num_workers_per_node, 0) { }

    //! Alllocates the next data target.
    //! Calls to this method alter the internal state -> order of calls is
    //! important and must be deterministic
    size_t AllocateId(size_t local_worker_id) {
        assert(local_worker_id < next_id_.size());
        return next_id_[local_worker_id]++;
    }

    //! Get object with given id, if it does not exist, create it.
    //! \param object_id of the object
    //! \param local_worker_id of the local worker who requested the object
    //! \param construction parameters forwards to constructor
    template <typename ... Types>
    ObjectPtr GetOrCreate(size_t object_id, size_t local_worker_id,
                          Types&& ... construction) {
        IdPair id(local_worker_id, object_id);
        auto it = map_.find(id);

        if (it != map_.end())
            return it->second;

        // construct new object
        ObjectPtr value = std::make_shared<Object>(
            std::forward<Types>(construction) ...);

        map_.insert(std::make_pair(id, value));
        return std::move(value);
    }

    //! return mutable reference to map of ids.
    std::map<IdPair, ObjectPtr> & map() { return map_; }

private:
    //! Next ID to generate, one for each local worker.
    std::vector<size_t> next_id_;

    //! map containing value items
    std::map<IdPair, ObjectPtr> map_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_REPOSITORY_HEADER

/******************************************************************************/
