/*******************************************************************************
 * c7a/net/net-exception.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_NET_EXCEPTION_HEADER
#define C7A_NET_NET_EXCEPTION_HEADER
#pragma once

namespace c7a {

//! \addtogroup net Network Communication
//! \{
/*!
 * A NetException is thrown by NetConnection on all errors instead of returning
 * error codes. If ever we manage to recover from network errors, we probably
 * have to rebuild most of the network objects anyway.
 */

class NetException : public std::runtime_error
{
public:
    NetException(const std::string& what)
        : std::runtime_error(what)
    { }

    NetException(const std::string& what, int _errno)
        : std::runtime_error(what + ": " + strerror(_errno))
    { }
};
} //namespace c7a

// \}

#endif // !C7A_NET_NET_EXCEPTION_HEADER

/******************************************************************************/
