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
    file* file,
    void* buffer,
    offset_type offset,
    size_type bytes,
    request_type type)
    : m_on_complete(on_compl),
      m_file(file),
      m_buffer(buffer),
      m_offset(offset),
      m_bytes(bytes),
      m_type(type) {
    LOG << "request::(...), ref_cnt=" << reference_count();
    m_file->add_request_ref();
}

request::~request() {
    LOG << "request::~request(), ref_cnt=" << reference_count();
}

void request::check_alignment() const {
    if (m_offset % STXXL_BLOCK_ALIGN != 0)
        LOG1 << "Offset is not aligned: modulo "
             << STXXL_BLOCK_ALIGN << " = " << m_offset % STXXL_BLOCK_ALIGN;

    if (m_bytes % STXXL_BLOCK_ALIGN != 0)
        LOG1 << "Size is not a multiple of "
             << STXXL_BLOCK_ALIGN << ", = " << m_bytes % STXXL_BLOCK_ALIGN;

    if (size_t(m_buffer) % STXXL_BLOCK_ALIGN != 0)
        LOG1 << "Buffer is not aligned: modulo "
             << STXXL_BLOCK_ALIGN << " = " << size_t(m_buffer) % STXXL_BLOCK_ALIGN
             << " (" << m_buffer << ")";
}

void request::check_nref_failed(bool after) {
    LOG1 << "WARNING: serious error, reference to the request is lost "
         << (after ? "after" : "before") << " serve()"
         << " nref=" << reference_count()
         << " this=" << this
         << " offset=" << m_offset
         << " buffer=" << m_buffer
         << " bytes=" << m_bytes
         << " type=" << ((m_type == READ) ? "READ" : "WRITE")
         << " file=" << m_file
         << " iotype=" << m_file->io_type();
}

const char* request::io_type() const {
    return m_file->io_type();
}

std::ostream& request::print(std::ostream& out) const {
    out << "File object address: " << static_cast<void*>(m_file);
    out << " Buffer address: " << static_cast<void*>(m_buffer);
    out << " File offset: " << m_offset;
    out << " Transfer size: " << m_bytes << " bytes";
    out << " Type of transfer: " << ((m_type == READ) ? "READ" : "WRITE");
    return out;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
