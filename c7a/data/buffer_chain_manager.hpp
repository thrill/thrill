/*******************************************************************************
 * c7a/data/buffer_chain_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER
#define C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER

#include <map>
#include "buffer_chain.hpp"
#include "chain_id.hpp"

namespace c7a {
namespace data {

//! Holds instances of BlockChains and addresses them with IDs
//!
//! Required for DIAs and for incoming net channels.
//! This cannot be in the DataManger, because that would cause cyclic dependencies
class BufferChainManager
{
public:
    BufferChainManager(ChainType type = LOCAL) : next_id_(type, 0) { }

    //! Allocates the next BufferChain.
    //! Calls to this method alter the internal state -> order of call is
    //! important and must be deterministic
    ChainId AllocateNext() {
        //mutext just to protect ++ op
        std::lock_guard<std::mutex> lock(allocate_next_mutex_);
        ChainId result = next_id_++;
        GetOrAllocate(result);
        return result;
    }

    //! Allocates a BufferChain with the given ID
    //! Use this only for internal stuff.
    //! \param id id of the chain to retrieve
    //! \exception std::runtime_error if id is not contained
    ChainId Allocate(const ChainId& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        sLOG << "allocate" << id;
        if (Contains(id)) {
            throw new std::runtime_error("duplicate chain allocation with explicit id");
        }
        chains_[id] = std::make_shared<BufferChain>();
        return id;
    }

    //! Indicates if a Bufferchain exists with the given ID
    bool Contains(const ChainId& id) {
        auto result = chains_.find(id) != chains_.end();
        sLOG << "contains" << id << "=" << result;
        return result;
    }

    //! Returns the BufferChain with the given ID
    //! \param id id of the chain to retrieve
    //! \exception std::runtime_error if id is not contained
    std::shared_ptr<BufferChain> Chain(const ChainId& id) {
        sLOG << "chain" << id;
        if (!Contains(id)) {
            throw new std::runtime_error("chain id is unknown");
        }

        return chains_[id];
    }

    //! Gets or allocates the BufferChain with the given ID in an atomic fashion
    //! Returns BufferchainPtr of the created or retrieved chain.
    std::shared_ptr<BufferChain> GetOrAllocate(const ChainId& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!Contains(id))
            chains_[id] = std::make_shared<BufferChain>();
        return chains_[id];
    }

private:
    static const bool debug = false;
    ChainId next_id_;
    std::map<ChainId, std::shared_ptr<BufferChain> > chains_;
    std::mutex mutex_;
    std::mutex allocate_next_mutex_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER

/******************************************************************************/
