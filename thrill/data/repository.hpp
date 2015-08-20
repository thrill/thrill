/*******************************************************************************
 * thrill/data/repository.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_REPOSITORY_HEADER
#define THRILL_DATA_REPOSITORY_HEADER

#include <cassert>
#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace thrill {
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

    // //! Allocates a data target with the given ID
    // //! Use this only for internal purpose.
    // //! \param id of the target to retrieve
    // //! \exception std::runtime_exception if id is already contained
    // size_t Allocate(const size_t& id) {
    //     sLOG << "allocate" << id;
    //     if (Contains(id)) {
    //         throw new std::runtime_error("duplocate data target allocation w/ explicit id");
    //     }
    //     data_[id] = std::make_shared<Object>();
    //     return id;
    // }

    // //! Indicates if a data target exists with the given id
    // bool Contains(const size_t& id) {
    //     return data_.find(id) != data_.end();
    // }

    // //! Returns the data target with the given ID
    // //! \ param id of the data to retrieve
    // //! \exception std::runtime_error if id is not contained
    // std::shared_ptr<Object> operator () (const size_t& id) {
    //     sLOG << "data" << id;
    //     if (!Contains(id)) {
    //         throw new std::runtime_error("data id is unknwon");
    //     }
    //     return data_[id];
    // }

    // //! Gets or allocates the data target with the given id in an atomic
    // //! fashion.
    // //! \returns a shared ptr to the created/retrieved data target
    // std::shared_ptr<Object> GetOrAllocate(const size_t& id) {
    //     if (!Contains(id))
    //         data_[id] = std::make_shared<Object>();
    //     return data_[id];
    // }

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
} // namespace thrill

#endif // !THRILL_DATA_REPOSITORY_HEADER

/******************************************************************************/
