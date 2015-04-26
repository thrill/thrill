#pragma once

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
}
