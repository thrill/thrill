#pragma once

//! \addtogroup net Network Communication
//! \{
/*!
 * A NetException is thrown by NetConnection on all errors instead of returning
 * error codes. If ever we manage to recover from network errors, we probably
 * have to rebuild most of the network objects anyway.
 */
namespace c7a {
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
}//namespace c7a
// \}
