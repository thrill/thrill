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

#if !defined(_MSC_VER)

#include <unistd.h>

#else

#include <io.h>

#endif

namespace thrill {
namespace common {

void PortSetCloseOnExec(int fd) {
#if !defined(_MSC_VER)
    if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
        throw ErrnoException("Error setting FD_CLOEXEC on file descriptor");
    }
#else
    (void)fd;
#endif
}

void MakePipe(int out_pipefds[2]) {
#if THRILL_HAVE_PIPE2
    if (pipe2(out_pipefds, O_CLOEXEC) != 0)
        throw ErrnoException("Error creating pipe");
#elif defined(_MSC_VER)
    if (_pipe(out_pipefds, 256, O_BINARY) != 0)
        throw ErrnoException("Error creating pipe");
#else
    if (pipe(out_pipefds) != 0)
        throw ErrnoException("Error creating pipe");

    PortSetCloseOnExec(out_pipefds[0]);
    PortSetCloseOnExec(out_pipefds[1]);
#endif
}

} // namespace common
} // namespace thrill

/******************************************************************************/
