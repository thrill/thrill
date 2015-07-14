/*******************************************************************************
 * c7a/data/chain_id.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHAIN_ID_HEADER
#define C7A_DATA_CHAIN_ID_HEADER

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
static inline
std::ostream& operator << (std::ostream& stream, const ChainId& id) {
    stream << id.ToString();
    return stream;
}

typedef c7a::data::ChainId ChannelId;

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHAIN_ID_HEADER

/******************************************************************************/
