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

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

namespace thrill {
namespace common {

void make_pipe(int out_pipefds[2]) {
#if THRILL_HAVE_PIPE2
    if (pipe2(out_pipefds, O_CLOEXEC) != 0)
        throw common::SystemException("Error creating pipe", errno);
#else
    if (pipe(out_pipefds) != 0)
        throw common::SystemException("Error creating pipe", errno);

    if (fcntl(out_pipefds[0], F_SETFD, FD_CLOEXEC) != 0) {
        throw common::SystemException(
                  "Error setting FD_CLOEXEC on pipe", errno);
    }
    if (fcntl(out_pipefds[1], F_SETFD, FD_CLOEXEC) != 0) {
        throw common::SystemException(
                  "Error setting FD_CLOEXEC on pipe", errno);
    }
#endif
}

} // namespace common
} // namespace thrill

/******************************************************************************/
