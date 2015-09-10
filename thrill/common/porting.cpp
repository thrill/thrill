/*******************************************************************************
 * thrill/common/porting.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/porting.hpp>
#include <thrill/common/system_exception.hpp>

#include <fcntl.h>
#include <unistd.h>

namespace thrill {
namespace common {

void make_pipe(int out_pipefds[2]) {
#if THRILL_HAVE_PIPE2
    if (pipe2(out_pipefds, O_CLOEXEC) != 0)
        throw common::ErrnoException("Error creating pipe");
#else
    if (pipe(out_pipefds) != 0)
        throw common::ErrnoException("Error creating pipe");

    if (fcntl(out_pipefds[0], F_SETFD, FD_CLOEXEC) != 0) {
        throw common::ErrnoException("Error setting FD_CLOEXEC on pipe");
    }
    if (fcntl(out_pipefds[1], F_SETFD, FD_CLOEXEC) != 0) {
        throw common::ErrnoException("Error setting FD_CLOEXEC on pipe");
    }
#endif
}

} // namespace common
} // namespace thrill

/******************************************************************************/
