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

#include <thrill/io/file_base.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/linuxaio_file.hpp>
#include <thrill/io/linuxaio_queue.hpp>
#include <thrill/io/linuxaio_request.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_queue_impl_qw_qr.hpp>
#include <thrill/io/serving_request.hpp>

#include <map>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Encapsulates disk queues.
//! \remark is a singleton
class DiskQueues : public common::Singleton<DiskQueues>
{
    friend class common::Singleton<DiskQueues>;

    using DiskId = int64_t;
    using RequestQueueMap = std::map<DiskId, RequestQueue*>;

protected:
    RequestQueueMap queues;
    DiskQueues() {
        Stats::get_instance(); // initialize stats before ourselves
    }

public:
    void make_queue(const FileBasePtr& file) {
        int queue_id = file->get_queue_id();

        RequestQueueMap::iterator qi = queues.find(queue_id);
        if (qi != queues.end())
            return;

        // create new request queue
#if THRILL_HAVE_LINUXAIO_FILE
        if (const LinuxaioFile* af =
                dynamic_cast<const LinuxaioFile*>(file.get())) {
            queues[queue_id] = new LinuxaioQueue(af->desired_queue_length());
            return;
        }
#endif
        queues[queue_id] = new RequestQueueImplQwQr();
    }

    void add_request(RequestPtr& req, DiskId disk) {
#ifdef THRILL_HACK_SINGLE_IO_THREAD
        disk = 42;
#endif
        RequestQueueMap::iterator qi = queues.find(disk);
        RequestQueue* q = nullptr;
        if (qi == queues.end())
        {
            // create new request queue
#if THRILL_HAVE_LINUXAIO_FILE
            if (dynamic_cast<LinuxaioRequest*>(req.get()))
                q = queues[disk] = new LinuxaioQueue(
                        dynamic_cast<LinuxaioFile*>(req->file().get())
                        ->desired_queue_length());
            else
#endif
            q = queues[disk] = new RequestQueueImplQwQr();
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
    bool cancel_request(Request* req, DiskId disk) {
#ifdef THRILL_HACK_SINGLE_IO_THREAD
        disk = 42;
#endif
        if (queues.find(disk) != queues.end())
            return queues[disk]->cancel_request(req);
        else
            return false;
    }

    RequestQueue * get_queue(DiskId disk) {
        if (queues.find(disk) != queues.end())
            return queues[disk];
        else
            return nullptr;
    }

    ~DiskQueues() {
        // deallocate all queues
        for (RequestQueueMap::iterator i = queues.begin(); i != queues.end(); i++)
            delete (*i).second;
    }

    //! Changes requests priorities.
    //! \param op one of:
    //!                 - READ, read requests are served before write requests within a disk queue
    //!                 - WRITE, write requests are served before read requests within a disk queue
    //!                 - NONE, read and write requests are served by turns, alternately
    void set_priority_op(RequestQueue::PriorityOp op) {
        for (RequestQueueMap::iterator i = queues.begin(); i != queues.end(); i++)
            i->second->set_priority_op(op);
    }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_QUEUES_HEADER

/******************************************************************************/
