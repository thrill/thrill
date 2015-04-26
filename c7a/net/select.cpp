/*******************************************************************************
 * c7a/communication/select.cpp
 *
 * Lightweight wrapper around select()
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/select.hpp>

#include <sys/time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

namespace c7a {

Socket* Select::SelectOne(const int msec,
                          const bool readable, const bool writeable)
{
    int maxfd = 0;

    FD_ZERO(&readset_);
    FD_ZERO(&writeset_);
    FD_ZERO(&exceptset_);

    for (Socket* s : socketlist_)
    {
        // maybe add to readable fd set
        if (readable) FD_SET(s->GetFileDescriptor(), &readset_);
        // maybe add to writable fd set
        if (writeable) FD_SET(s->GetFileDescriptor(), &writeset_);
        // but always add to exception fd set
        FD_SET(s->GetFileDescriptor(), &exceptset_);

        maxfd = std::max(maxfd, s->GetFileDescriptor() + 1);
    }

    struct timeval timeout;

    timeout.tv_usec = (msec % 1000) * 1000;
    timeout.tv_sec = msec / 1000;

#ifdef __linux__
    // linux supports reading elapsed time from timeout

    int r = ::select(maxfd, &readset_, &writeset_, &exceptset_, &timeout);

    elapsed_ = msec - (timeout.tv_sec * 1000 + timeout.tv_usec / 1000);

#else
    // else we have to do the gettimeofday() calls ourselves

    struct timeval selstart, selfinish;

    gettimeofday(&selstart, NULL);
    int r = ::select(maxfd, &readset_, &writeset_, &exceptset_, &timeout);
    gettimeofday(&selfinish, NULL);

    elapsed_ = (selfinish.tv_sec - selstart.tv_sec) * 1000;
    elapsed_ += (selfinish.tv_usec - selstart.tv_usec) / 1000;

    // int selsays = msec - (timeout.tv_sec * 1000 + timeout.tv_usec / 1000);

    LOG << "Select::SelectOne() spent " << elapsed_ << " msec in select."
        << "select() says " << r;

#endif

    if (r < 0) {
        LOG << "Select::SelectOne() failed!"
            << " error=" << strerror(errno);
        return NULL;
    }
    else if (r == 0) {
        return NULL;
    }

    for (Socket* s : socketlist_)
    {
        if (readable && FD_ISSET(s->GetFileDescriptor(), &readset_)) return s;
        if (writeable && FD_ISSET(s->GetFileDescriptor(), &writeset_)) return s;

        if (FD_ISSET(s->GetFileDescriptor(), &exceptset_)) return s;
    }

    return NULL;
}

} // namespace c7a

/******************************************************************************/
