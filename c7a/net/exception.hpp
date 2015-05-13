/*******************************************************************************
 * c7a/net/net-exception.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_NET_EXCEPTION_HEADER
#define C7A_NET_NET_EXCEPTION_HEADER

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * A Exception is thrown by Connection on all errors instead of returning
 * error codes. If ever we manage to recover from network errors, we probably
 * have to rebuild most of the network objects anyway.
 */
class Exception : public std::runtime_error
{
public:
    explicit Exception(const std::string& what)
        : std::runtime_error(what)
    { }

    Exception(const std::string& what, int _errno)
        : std::runtime_error(what + ": " + strerror(_errno))
    { }
};

// \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_EXCEPTION_HEADER

/******************************************************************************/
