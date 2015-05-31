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

namespace c7a {
namespace data {

//! Distinguishes different ChainID types
enum ChainType { LOCAL, NETWORK };

//! Identification for buffer Chains
//! ChainIDs carry a \ref ChainType "type" and an identifier.
//! ChainIDs can generate their successors by using the ++ operators.
struct ChainId {
    ChainType type;
    size_t    identifier;

    ChainId(ChainType type, size_t id) : type(type), identifier(id) { }

    //! Creates a local ChainID
    ChainId(size_t id) : type(LOCAL), identifier(id) { }

    //post increment
    ChainId operator ++ (int /*dummy*/) {
        auto result = ChainId(type, identifier++);
        return result;
    }

    //pre increment
    ChainId operator ++ () {
        auto result = ChainId(type, ++identifier);
        return result;
    }

    //! Returns a string representation of this ChainID
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

    //! ChainIDs are equal if theu share the same type and identifier
    bool operator == (const ChainId& other) const {
        return other.type == type && identifier == other.identifier;
    }

    bool operator < (const ChainId& other) const {
        auto a = other.identifier;
        auto b = identifier;
        if (other.type == NETWORK)
            a = -1 * a;
        if (type == NETWORK)
            b = -1 * b;
        return a < b;
    }
};

//! Stream operator that calls ToString on ChainID
static std::ostream& operator << (std::ostream& stream, const ChainId& id) {
    stream << id.ToString();
    return stream;
}

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
        sLOG << "allocate" << id;
        if (Contains(id)) {
            throw new std::runtime_error("duplicate chain allocation with explicit id");
        }
        chains_[id] = std::make_shared<BufferChain>();
        return id;
    }

    //! Indicates if a Bufferchain exists with the given ID
    bool Contains(ChainId id) {
        auto result = chains_.find(id) != chains_.end();
        sLOG << "contains" << id << "=" << result;
        return result;
    }

    //! Returns the BufferChain with the given ID
    //! \param id id of the chain to retrieve
    //! \exception std::runtime_error if id is not contained
    std::shared_ptr<BufferChain> Chain(ChainId id) {
        sLOG << "chain" << id;
        if (!Contains(id)) {
            throw new std::runtime_error("chain id is unknown");
        }

        return chains_[id];
    }

private:
    static const bool debug = true;
    ChainId next_id_;
    std::map<ChainId, std::shared_ptr<BufferChain> > chains_;
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BUFFER_CHAIN_MANAGER_HEADER

/******************************************************************************/
