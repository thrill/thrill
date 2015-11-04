/*******************************************************************************
 * thrill/io/disk_queues.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_DISK_QUEUES_HEADER
#define THRILL_IO_DISK_QUEUES_HEADER

#include <map>

#include <thrill/io/iostats.hpp>
#include <thrill/io/linuxaio_queue.hpp>
#include <thrill/io/linuxaio_request.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_queue_impl_qwqr.hpp>
#include <thrill/io/serving_request.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Encapsulates disk queues.
//! \remark is a singleton
class disk_queues : public singleton<disk_queues>
{
    friend class singleton<disk_queues>;

    using DISKID = int64_t;
    using request_queue_map = std::map<DISKID, request_queue*>;

protected:
    request_queue_map queues;
    disk_queues() {
        stats::get_instance(); // initialize stats before ourselves
    }

public:
    void add_request(request_ptr& req, DISKID disk) {
#ifdef STXXL_HACK_SINGLE_IO_THREAD
        disk = 42;
#endif
        request_queue_map::iterator qi = queues.find(disk);
        request_queue* q;
        if (qi == queues.end())
        {
            // create new request queue
#if STXXL_HAVE_LINUXAIO_FILE
            if (dynamic_cast<linuxaio_request*>(req.get()))
                q = queues[disk] = new linuxaio_queue(
                        dynamic_cast<linuxaio_file*>(req->file())->get_desired_queue_length()
                        );
            else
#endif
            q = queues[disk] = new request_queue_impl_qwqr();
        }
        else
            q = qi->second;

        q->add_request(req);
    }

    //! Cancel a request.
    //! The specified request is canceled unless already being processed.
    //! However, cancelation cannot be guaranteed.
    //! Cancelled requests must still be waited for in order to ensure correct
    //! operation.
    //! \param req request to cancel
    //! \param disk disk number for disk that \c req was scheduled on
    //! \return \c true iff the request was canceled successfully
    bool cancel_request(request_ptr& req, DISKID disk) {
#ifdef STXXL_HACK_SINGLE_IO_THREAD
        disk = 42;
#endif
        if (queues.find(disk) != queues.end())
            return queues[disk]->cancel_request(req);
        else
            return false;
    }

    request_queue * get_queue(DISKID disk) {
        if (queues.find(disk) != queues.end())
            return queues[disk];
        else
            return nullptr;
    }

    ~disk_queues() {
        // deallocate all queues
        for (request_queue_map::iterator i = queues.begin(); i != queues.end(); i++)
            delete (*i).second;
    }

    //! Changes requests priorities.
    //! \param op one of:
    //!                 - READ, read requests are served before write requests within a disk queue
    //!                 - WRITE, write requests are served before read requests within a disk queue
    //!                 - NONE, read and write requests are served by turns, alternately
    void set_priority_op(request_queue::priority_op op) {
        for (request_queue_map::iterator i = queues.begin(); i != queues.end(); i++)
            i->second->set_priority_op(op);
    }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_QUEUES_HEADER

/******************************************************************************/
