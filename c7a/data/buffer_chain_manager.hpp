/*******************************************************************************
 * c7a/data/buffer_chain_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER
#define C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER

#include <map>
#include "buffer_chain.hpp"

namespace c7a {
namespace data {

//! Identification for buffer Chains
typedef size_t ChainId;

//! Holds instances of BlockChains and addresses them with IDs
//!
//! Required for DIAs and for incoming net channels.
//! This cannot be in the DataManger, because that would cause cyclic dependencies
class BufferChainManager
{
public:

    BufferChainManager() : next_id_(0) { }

    //! Allocates the next BufferChain.
    //! Calls to this method alter the internal state -> order of call is
    //! important and must be deterministic
    ChainId AllocateNext() {
        ChainId result = next_id_;
        if (!Contains(next_id_))
            Allocate(next_id_++);
        return result;
    }

    //! Allocates a BufferChain with the given ID
    //! Use this only for internal stuff.
    //! \param id id of the chain to retrieve
    //! \exception std::runtime_error if id is not contained
    ChainId Allocate(ChainId id) {
        if (Contains(id)) {
            throw new std::runtime_error("duplicate chain allocation with explicit id");
        }
        chains_[id] = std::make_shared<BufferChain>();
        return id;
    }

    //! Indicates if a Bufferchain exists with the given ID
    bool Contains(ChainId id) {
        return chains_.find(id) != chains_.end();
    }

    //! Returns the BufferChain with the given ID
    //! \param id id of the chain to retrieve
    //! \exception std::runtime_error if id is not contained
    std::shared_ptr<BufferChain> Chain(ChainId id) {
        if (!Contains(id)) {
            throw new std::runtime_error("chain id is unknown");
        }

        return chains_[id];
    }

private:
    ChainId next_id_;
    std::map<ChainId, std::shared_ptr<BufferChain> > chains_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER

/******************************************************************************/
