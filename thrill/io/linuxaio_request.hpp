/*******************************************************************************
 * thrill/io/linuxaio_request.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2011 Johannes Singler <singler@kit.edu>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_LINUXAIO_REQUEST_HEADER
#define THRILL_IO_LINUXAIO_REQUEST_HEADER

#include <thrill/io/linuxaio_file.hpp>

#if THRILL_HAVE_LINUXAIO_FILE

#include <linux/aio_abi.h>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Request for an linuxaio_file.
class LinuxaioRequest final : public Request
{
    //! control block of async request
    iocb cb_;

    void fill_control_block();

public:
    LinuxaioRequest(
        const CompletionHandler& on_complete,
        FileBase* file, void* buffer, offset_type offset, size_type bytes,
        ReadOrWriteType type)
        : Request(on_complete, file, buffer, offset, bytes, type) {
        assert(dynamic_cast<LinuxaioFile*>(file));
        LOG << "LinuxaioRequest[" << this << "]" << " LinuxaioRequest"
            << "(file=" << file << " buffer=" << buffer
            << " offset=" << offset << " bytes=" << bytes
            << " type=" << type << ")";
    }

    bool post();
    bool cancel() final;
    bool cancel_aio();
    void completed(bool posted, bool canceled);
    void completed(bool canceled) final { completed(true, canceled); }
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_LINUXAIO_FILE

#endif // !THRILL_IO_LINUXAIO_REQUEST_HEADER

/******************************************************************************/
