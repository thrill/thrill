/*******************************************************************************
 * thrill/io/request.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <ostream>

#include <thrill/io/file.hpp>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

request::request(
    const completion_handler& on_compl,
    io::file* file,
    void* buffer,
    offset_type offset,
    size_type bytes,
    ReadOrWriteType type)
    : on_complete_(on_compl),
      file_(file),
      buffer_(buffer),
      offset_(offset),
      bytes_(bytes),
      type_(type) {
    LOG << "request::(...), ref_cnt=" << reference_count();
    file_->add_request_ref();
}

request::~request() {
    LOG << "request::~request(), ref_cnt=" << reference_count();
}

void request::check_alignment() const {
    if (offset_ % STXXL_BLOCK_ALIGN != 0)
        LOG1 << "Offset is not aligned: modulo "
             << STXXL_BLOCK_ALIGN << " = " << offset_ % STXXL_BLOCK_ALIGN;

    if (bytes_ % STXXL_BLOCK_ALIGN != 0)
        LOG1 << "Size is not a multiple of "
             << STXXL_BLOCK_ALIGN << ", = " << bytes_ % STXXL_BLOCK_ALIGN;

    if (size_t(buffer_) % STXXL_BLOCK_ALIGN != 0)
        LOG1 << "Buffer is not aligned: modulo "
             << STXXL_BLOCK_ALIGN << " = " << size_t(buffer_) % STXXL_BLOCK_ALIGN
             << " (" << buffer_ << ")";
}

void request::check_nref_failed(bool after) {
    LOG1 << "WARNING: serious error, reference to the request is lost "
         << (after ? "after" : "before") << " serve()"
         << " nref=" << reference_count()
         << " this=" << this
         << " offset=" << offset_
         << " buffer=" << buffer_
         << " bytes=" << bytes_
         << " type=" << ((type_ == READ) ? "READ" : "WRITE")
         << " file=" << file_
         << " iotype=" << file_->io_type();
}

const char* request::io_type() const {
    return file_->io_type();
}

std::ostream& request::print(std::ostream& out) const {
    out << "File object address: " << static_cast<void*>(file_);
    out << " Buffer address: " << static_cast<void*>(buffer_);
    out << " File offset: " << offset_;
    out << " Transfer size: " << bytes_ << " bytes";
    out << " Type of transfer: " << ((type_ == READ) ? "READ" : "WRITE");
    return out;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
