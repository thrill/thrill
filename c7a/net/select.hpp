/*******************************************************************************
 * c7a/net/select.hpp
 *
 * Lightweight wrapper around select()
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_SELECT_HEADER
#define C7A_NET_SELECT_HEADER

#include <c7a/net/socket.hpp>

#include <vector>
#include <sys/select.h>

namespace c7a {

//! \addtogroup netsock Low Level Socket API
//! \{

/**
 * Select is an object-oriented wrapper for select(). It takes care of the
 * socket list, bit-fields, etc.
 */
class Select
{
    static const bool debug = true;

public:
    //! Create an empty select() object
    Select()
    { }

    //! Create a select() object and add the first socket
    Select(Socket& s1)
    { Add(s1); }

    //! Create a select() object and add the first two sockets
    Select(Socket& s1, Socket& s2)
    { Add(s1), Add(s2); }

private:
    //! typedef of the list used for the sockets
    typedef std::vector<Socket*> socketlist_type;

    //! list of sockets in this select() group
    std::vector<Socket*> socketlist_;

    //! read bit-field
    fd_set readset_;

    //! write bit-field
    fd_set writeset_;

    //! exception bit-field
    fd_set exceptset_;

    //! time elapsed during last select() call
    unsigned int elapsed_ = 0;

public:
    //! Add a socket to the selection set
    void Add(Socket& socket)
    { socketlist_.push_back(&socket); }

    //! Do a select() and return the first pending socket, or NULL if a timeout
    //! occurred. Wait for maximum msec milliseconds. Only select() the readable
    //! or writable sockets if set.
    Socket * SelectOne(int msec, bool readable, bool writable);

    //! Return the time elapsed during the last select() call on this object
    //! @return time in msec
    unsigned int elapsed()
    { return elapsed_; }
};

//! \}

} // namespace c7a

#endif // !C7A_NET_SELECT_HEADER

/******************************************************************************/
