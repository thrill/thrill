/*******************************************************************************
 * c7a/data/repository.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_REPOSITORY_HEADER
#define C7A_DATA_REPOSITORY_HEADER

#include <cassert>
#include <map>
#include <memory>
#include <mutex>

#include <c7a/common/logger.hpp>

namespace c7a {
namespace data {

enum DataType {
    LOCAL, NETWORK
};

struct DataId {
    DataType type;
    size_t   identifier;

    DataId(DataType type, size_t id) : type(type), identifier(id) { }

    //! Creates local data id
    DataId(size_t id) : type(LOCAL), identifier(id) { }

    //! Post Increment
    DataId operator ++ (int /*dummy*/) {
        auto result = DataId(type, identifier++);
        return result;
    }

    //! Pre Increment
    DataId operator ++ () {
        auto result = DataId(type, ++identifier);
        return result;
    }

    //! Returns a string representation of this DataId
    std::string ToString() const {
        switch (type) {
        case LOCAL:
            return "local-" + std::to_string(identifier);
        case NETWORK:
            return "network-" + std::to_string(identifier);
        default:
            return "unknown-" + std::to_string(identifier);
        }
    }

    //! DataId are equal if theu share the same type and identifier
    bool operator == (const DataId& other) const {
        return other.type == type && identifier == other.identifier;
    }

    bool operator < (const DataId& other) const {
        auto a = other.identifier;
        auto b = identifier;
        if (other.type == NETWORK)
            a = -1 * a;
        if (type == NETWORK)
            b = -1 * b;
        return a < b;
    }
};
//! Stream operator that calls ToString on DataId
static inline
std::ostream& operator << (std::ostream& stream, const DataId& id) {
    stream << id.ToString();
    return stream;
}

template <class Target>
class Repository
{
public:
    //!Creates a data repositpry of the given type
    Repository(DataType type = LOCAL) : next_id_(type, 0) { }

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
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_REPOSITORY_HEADER

/******************************************************************************/
