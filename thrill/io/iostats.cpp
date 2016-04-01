/*******************************************************************************
 * thrill/io/iostats.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009, 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/iostats.hpp>

#include <thrill/common/string.hpp>

#include <iomanip>
#include <sstream>
#include <string>

namespace thrill {
namespace io {

Stats::Stats()
    : last_reset_time_(timestamp()) { }

void Stats::write_started(size_t size, double now) {
#if THRILL_IO_STATS_TIMING
    if (now == 0.0)
        now = timestamp();
#else
    common::UNUSED(now);
#endif
    {
        std::unique_lock<std::mutex> write_lock(write_mutex_);

        ++write_ops_;
        write_volume_ += size;
#if THRILL_IO_STATS_TIMING
        double diff = now - parallel_write_begin_;
        write_time_ += static_cast<double>(acc_writes_) * diff;
        parallel_write_begin_ = now;
        parallel_write_time_ += (acc_writes_++) ? diff : 0.0;
#endif
    }
#if THRILL_IO_STATS_TIMING
    {
        std::unique_lock<std::mutex> io_lock(io_mutex_);

        double diff = now - parallel_io_begin_;
        parallel_io_time_ += (acc_ios_++) ? diff : 0.0;
        parallel_io_begin_ = now;
    }
#endif
}

void Stats::write_canceled(size_t size) {
    {
        std::unique_lock<std::mutex> write_lock(write_mutex_);

        --write_ops_;
        write_volume_ -= size;
    }
    write_finished();
}

void Stats::write_finished() {
#if THRILL_IO_STATS_TIMING
    double now = timestamp();
    {
        std::unique_lock<std::mutex> write_lock(write_mutex_);

        double diff = now - parallel_write_begin_;
        write_time_ += static_cast<double>(acc_writes_) * diff;
        parallel_write_begin_ = now;
        parallel_write_time_ += (acc_writes_--) ? diff : 0.0;
    }
    {
        std::unique_lock<std::mutex> io_lock(io_mutex_);

        double diff = now - parallel_io_begin_;
        parallel_io_time_ += (acc_ios_--) ? diff : 0.0;
        parallel_io_begin_ = now;
    }
#endif
}

void Stats::write_cached(size_t size) {
    std::unique_lock<std::mutex> write_lock(write_mutex_);

    ++cached_write_ops_;
    cached_write_volume_ += size;
}

void Stats::read_started(size_t size, double now) {
#if THRILL_IO_STATS_TIMING
    if (now == 0.0)
        now = timestamp();
#else
    common::UNUSED(now);
#endif
    {
        std::unique_lock<std::mutex> read_lock(read_mutex_);

        ++read_ops_;
        read_volume_ += size;
#if THRILL_IO_STATS_TIMING
        double diff = now - parallel_read_begin_;
        read_time_ += static_cast<double>(acc_reads_) * diff;
        parallel_read_begin_ = now;
        parallel_read_time_ += (acc_reads_++) ? diff : 0.0;
 #endif
    }
 #if THRILL_IO_STATS_TIMING
    {
        std::unique_lock<std::mutex> io_lock(io_mutex_);

        double diff = now - parallel_io_begin_;
        parallel_io_time_ += (acc_ios_++) ? diff : 0.0;
        parallel_io_begin_ = now;
    }
 #endif
}

void Stats::read_canceled(size_t size) {
    {
        std::unique_lock<std::mutex> read_lock(read_mutex_);

        --read_ops_;
        read_volume_ -= size;
    }
    read_finished();
}

void Stats::read_finished() {
#if THRILL_IO_STATS_TIMING
    double now = timestamp();
    {
        std::unique_lock<std::mutex> read_lock(read_mutex_);

        double diff = now - parallel_read_begin_;
        read_time_ += static_cast<double>(acc_reads_) * diff;
        parallel_read_begin_ = now;
        parallel_read_time_ += (acc_reads_--) ? diff : 0.0;
    }
    {
        std::unique_lock<std::mutex> io_lock(io_mutex_);

        double diff = now - parallel_io_begin_;
        parallel_io_time_ += (acc_ios_--) ? diff : 0.0;
        parallel_io_begin_ = now;
    }
#endif
}

void Stats::read_cached(size_t size) {
    std::unique_lock<std::mutex> read_lock(read_mutex_);

    ++cached_read_ops_;
    cached_read_volume_ += size;
}

#if !THRILL_DO_NOT_COUNT_WAIT_TIME
void Stats::wait_started(WaitOp wait_op) {
    double now = timestamp();
    {
        std::unique_lock<std::mutex> wait_lock(wait_mutex_);

        double diff = now - parallel_wait_begin_;
        io_wait_time_ += static_cast<double>(acc_waits_) * diff;
        parallel_wait_begin_ = now;
        parallel_wait_time_ += (acc_waits_++) ? diff : 0.0;

        if (wait_op == WaitOp::READ) {
            diff = now - parallel_wait_read_begin_;
            read_wait_time_ += static_cast<double>(acc_wait_read_) * diff;
            parallel_wait_read_begin_ = now;
            parallel_wait_read_time_ += (acc_wait_read_++) ? diff : 0.0;
        }
        else /* if (wait_op == WaitOp::WRITE) */ {
            // wait_any() is only used from write_pool and buffered_writer, so account WAIT_OP_ANY for WaitOp::WRITE, too
            diff = now - parallel_wait_write_begin_;
            write_wait_time_ += static_cast<double>(acc_wait_write_) * diff;
            parallel_wait_write_begin_ = now;
            parallel_wait_write_time_ += (acc_wait_write_++) ? diff : 0.0;
        }
    }
}

void Stats::wait_finished(WaitOp wait_op) {
    double now = timestamp();
    {
        std::unique_lock<std::mutex> wait_lock(wait_mutex_);

        double diff = now - parallel_wait_begin_;
        io_wait_time_ += static_cast<double>(acc_waits_) * diff;
        parallel_wait_begin_ = now;
        parallel_wait_time_ += (acc_waits_--) ? diff : 0.0;

        if (wait_op == WaitOp::READ) {
            double diff_read = now - parallel_wait_read_begin_;
            read_wait_time_ += static_cast<double>(acc_wait_read_) * diff_read;
            parallel_wait_read_begin_ = now;
            parallel_wait_read_time_ += (acc_wait_read_--) ? diff_read : 0.0;
        }
        else /* if (wait_op == WaitOp::WRITE) */ {
            double diff_write = now - parallel_wait_write_begin_;
            write_wait_time_ += static_cast<double>(acc_wait_write_) * diff_write;
            parallel_wait_write_begin_ = now;
            parallel_wait_write_time_ += (acc_wait_write_--) ? diff_write : 0.0;
        }
#if THRILL_WAIT_LOG_ENABLED
        std::ofstream* waitlog = stxxl::logger::GetInstance()->waitlog_stream();
        if (waitlog)
            *waitlog << (now - last_reset_time_) << "\t"
                     << ((wait_op == WaitOp::READ) ? diff : 0.0) << "\t"
                     << ((wait_op != WaitOp::READ) ? diff : 0.0) << "\t"
                     << read_wait_time_ << "\t" << write_wait_time_ << std::endl << std::flush;
#endif
    }
}
#endif

std::ostream& operator << (std::ostream& o, const StatsData& s) {
#define hr common::FormatIecUnits
    o << "Thrill I/O statistics" << std::endl;
    o << " total number of reads                      : "
      << hr(s.read_ops()) << std::endl;
    o << " average block size (read)                  : "
      << hr(s.read_ops() ? s.read_volume() / s.read_ops() : 0) << std::endl;
    o << " number of bytes read from disks            : "
      << hr(s.read_volume()) << std::endl;
#if THRILL_IO_STATS_TIMING
    o << " time spent in serving all read requests    : "
      << s.read_time() << " s"
      << " @ " << (static_cast<double>(s.read_volume()) / 1048576.0 / s.read_time()) << " MiB/s"
      << std::endl;
    o << " time spent in reading (parallel read time) : "
      << s.parallel_read_time() << " s"
      << " @ " << (static_cast<double>(s.read_volume()) / 1048576.0 / s.parallel_read_time()) << " MiB/s"
      << std::endl;
    if (s.cached_read_ops()) {
        o << " total number of cached reads               : "
          << hr(s.cached_read_ops()) << std::endl;
        o << " average block size (cached read)           : "
          << hr(s.cached_read_volume() / s.cached_read_ops()) << std::endl;
        o << " number of bytes read from cache            : "
          << hr(s.cached_read_volume()) << std::endl;
    }
    if (s.cached_write_ops()) {
        o << " total number of cached writes              : "
          << hr(s.cached_write_ops()) << std::endl;
        o << " average block size (cached write)          : "
          << hr(s.cached_write_volume() / s.cached_write_ops()) << std::endl;
        o << " number of bytes written to cache           : "
          << hr(s.cached_write_volume()) << std::endl;
    }
#endif
    o << " total number of writes                     : "
      << hr(s.write_ops()) << std::endl;
    o << " average block size (write)                 : "
      << hr(s.write_ops() ? s.write_volume() / s.write_ops() : 0) << std::endl;
    o << " number of bytes written to disks           : "
      << hr(s.write_volume()) << std::endl;
#if THRILL_IO_STATS_TIMING
    o << " time spent in serving all write requests   : "
      << s.write_time() << " s"
      << " @ " << (static_cast<double>(s.write_volume()) / 1048576.0 / s.write_time()) << " MiB/s"
      << std::endl;
    o << " time spent in writing (parallel write time): "
      << s.parallel_write_time() << " s"
      << " @ " << (static_cast<double>(s.write_volume()) / 1048576.0 / s.parallel_write_time()) << " MiB/s"
      << std::endl;
    o << " time spent in I/O (parallel I/O time)      : "
      << s.parallel_io_time() << " s"
      << " @ " << ((static_cast<double>(s.read_volume() + s.write_volume())) / 1048576.0 / s.parallel_io_time()) << " MiB/s"
      << std::endl;
#if !THRILL_DO_NOT_COUNT_WAIT_TIME
    o << " I/O wait time                              : "
      << s.io_wait_time() << " s" << std::endl;
    if (s.read_wait_time() != 0.0)
        o << " I/O wait4read time                         : "
          << s.read_wait_time() << " s" << std::endl;
    if (s.write_wait_time() != 0.0)
        o << " I/O wait4write time                        : "
          << s.write_wait_time() << " s" << std::endl;
#endif
    o << " Time since the last reset                  : "
      << s.elapsed_time() << " s" << std::endl;
#endif
#undef hr
    return o;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
