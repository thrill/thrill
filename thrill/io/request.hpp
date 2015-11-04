/*******************************************************************************
 * thrill/io/request.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_HEADER
#define THRILL_IO_REQUEST_HEADER

#include <thrill/common/counting_ptr.hpp>
#include <thrill/io/completion_handler.hpp>
#include <thrill/io/request_interface.hpp>

#include <cassert>
#include <memory>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

#define STXXL_BLOCK_ALIGN 4096

class file;

class io_error : public std::ios_base::failure
{
public:
    io_error() throw ()
        : std::ios_base::failure("")
    { }

    io_error(const std::string& message) throw ()
        : std::ios_base::failure(message)
    { }
};

//! Request object encapsulating basic properties like file and offset.
class request : virtual public request_interface, public common::ReferenceCount
{
    friend class linuxaio_queue;

protected:
    completion_handler m_on_complete;
    std::unique_ptr<io_error> m_error;

    static const bool debug = false;

protected:
    file* m_file;
    void* m_buffer;
    offset_type m_offset;
    size_type m_bytes;
    request_type m_type;

public:
    request(const completion_handler& on_compl,
            file* file,
            void* buffer,
            offset_type offset,
            size_type bytes,
            request_type type);

    virtual ~request();

    file * get_file() const { return m_file; }
    void * get_buffer() const { return m_buffer; }
    offset_type get_offset() const { return m_offset; }
    size_type get_size() const { return m_bytes; }
    request_type get_type() const { return m_type; }

    void check_alignment() const;

    std::ostream & print(std::ostream& out) const;

    //! Inform the request object that an error occurred during the I/O
    //! execution.
    void error_occured(const char* msg) {
        m_error.reset(new io_error(msg));
    }

    //! Inform the request object that an error occurred during the I/O
    //! execution.
    void error_occured(const std::string& msg) {
        m_error.reset(new io_error(msg));
    }

    //! Rises an exception if there were error with the I/O.
    void check_errors() {
        if (m_error.get())
            throw *(m_error.get());
    }

    virtual const char * io_type() const;

protected:
    void check_nref(bool after = false) {
        if (reference_count() < 2)
            check_nref_failed(after);
    }

private:
    void check_nref_failed(bool after);
};

inline std::ostream& operator << (std::ostream& out, const request& req) {
    return req.print(out);
}

//! A reference counting pointer for \c request.
using request_ptr = common::CountingPtr<request>;

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_HEADER

/******************************************************************************/
