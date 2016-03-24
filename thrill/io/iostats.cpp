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

#include <iomanip>
#include <sstream>
#include <string>

namespace thrill {
namespace io {

Stats::Stats()
    : reads(0),
      writes(0),
      volume_read(0),
      volume_written(0),
      c_reads(0),
      c_writes(0),
      c_volume_read(0),
      c_volume_written(0),
      t_reads(0.0),
      t_writes(0.0),
      p_reads(0.0),
      p_writes(0.0),
      p_begin_read(0.0),
      p_begin_write(0.0),
      p_ios(0.0),
      p_begin_io(0.0),
      t_waits(0.0),
      p_waits(0.0),
      p_begin_wait(0.0),
      t_wait_read(0.0),
      p_wait_read(0.0),
      p_begin_wait_read(0.0),
      t_wait_write(0.0),
      p_wait_write(0.0),
      p_begin_wait_write(0.0),
      acc_reads(0), acc_writes(0),
      acc_ios(0),
      acc_waits(0),
      acc_wait_read(0), acc_wait_write(0),
      last_reset(timestamp())
{ }

#if THRILL_IO_STATS
void Stats::write_started(size_t size_, double now) {
    if (now == 0.0)
        now = timestamp();
    {
        std::unique_lock<std::mutex> write_lock(write_mutex);

        ++writes;
        volume_written += size_;
        double diff = now - p_begin_write;
        t_writes += static_cast<double>(acc_writes) * diff;
        p_begin_write = now;
        p_writes += (acc_writes++) ? diff : 0.0;
    }
    {
        std::unique_lock<std::mutex> io_lock(io_mutex);

        double diff = now - p_begin_io;
        p_ios += (acc_ios++) ? diff : 0.0;
        p_begin_io = now;
    }
}

void Stats::write_canceled(size_t size_) {
    {
        std::unique_lock<std::mutex> write_lock(write_mutex);

        --writes;
        volume_written -= size_;
    }
    write_finished();
}

void Stats::write_finished() {
    double now = timestamp();
    {
        std::unique_lock<std::mutex> write_lock(write_mutex);

        double diff = now - p_begin_write;
        t_writes += static_cast<double>(acc_writes) * diff;
        p_begin_write = now;
        p_writes += (acc_writes--) ? diff : 0.0;
    }
    {
        std::unique_lock<std::mutex> io_lock(io_mutex);

        double diff = now - p_begin_io;
        p_ios += (acc_ios--) ? diff : 0.0;
        p_begin_io = now;
    }
}

void Stats::write_cached(size_t size_) {
    std::unique_lock<std::mutex> write_lock(write_mutex);

    ++c_writes;
    c_volume_written += size_;
}

void Stats::read_started(size_t size_, double now) {
    if (now == 0.0)
        now = timestamp();
    {
        std::unique_lock<std::mutex> read_lock(read_mutex);

        ++reads;
        volume_read += size_;
        double diff = now - p_begin_read;
        t_reads += static_cast<double>(acc_reads) * diff;
        p_begin_read = now;
        p_reads += (acc_reads++) ? diff : 0.0;
    }
    {
        std::unique_lock<std::mutex> io_lock(io_mutex);

        double diff = now - p_begin_io;
        p_ios += (acc_ios++) ? diff : 0.0;
        p_begin_io = now;
    }
}

void Stats::read_canceled(size_t size_) {
    {
        std::unique_lock<std::mutex> read_lock(read_mutex);

        --reads;
        volume_read -= size_;
    }
    read_finished();
}

void Stats::read_finished() {
    double now = timestamp();
    {
        std::unique_lock<std::mutex> read_lock(read_mutex);

        double diff = now - p_begin_read;
        t_reads += static_cast<double>(acc_reads) * diff;
        p_begin_read = now;
        p_reads += (acc_reads--) ? diff : 0.0;
    }
    {
        std::unique_lock<std::mutex> io_lock(io_mutex);

        double diff = now - p_begin_io;
        p_ios += (acc_ios--) ? diff : 0.0;
        p_begin_io = now;
    }
}

void Stats::read_cached(size_t size_) {
    std::unique_lock<std::mutex> read_lock(read_mutex);

    ++c_reads;
    c_volume_read += size_;
}
#endif

#ifndef THRILL_DO_NOT_COUNT_WAIT_TIME
void Stats::wait_started(wait_op_type wait_op) {
    double now = timestamp();
    {
        std::unique_lock<std::mutex> wait_lock(wait_mutex);

        double diff = now - p_begin_wait;
        t_waits += static_cast<double>(acc_waits) * diff;
        p_begin_wait = now;
        p_waits += (acc_waits++) ? diff : 0.0;

        if (wait_op == WAIT_OP_READ) {
            diff = now - p_begin_wait_read;
            t_wait_read += static_cast<double>(acc_wait_read) * diff;
            p_begin_wait_read = now;
            p_wait_read += (acc_wait_read++) ? diff : 0.0;
        }
        else /* if (wait_op == WAIT_OP_WRITE) */ {
            // wait_any() is only used from write_pool and buffered_writer, so account WAIT_OP_ANY for WAIT_OP_WRITE, too
            diff = now - p_begin_wait_write;
            t_wait_write += static_cast<double>(acc_wait_write) * diff;
            p_begin_wait_write = now;
            p_wait_write += (acc_wait_write++) ? diff : 0.0;
        }
    }
}

void Stats::wait_finished(wait_op_type wait_op) {
    double now = timestamp();
    {
        std::unique_lock<std::mutex> wait_lock(wait_mutex);

        double diff = now - p_begin_wait;
        t_waits += static_cast<double>(acc_waits) * diff;
        p_begin_wait = now;
        p_waits += (acc_waits--) ? diff : 0.0;

        if (wait_op == WAIT_OP_READ) {
            double diff_read = now - p_begin_wait_read;
            t_wait_read += static_cast<double>(acc_wait_read) * diff_read;
            p_begin_wait_read = now;
            p_wait_read += (acc_wait_read--) ? diff_read : 0.0;
        }
        else /* if (wait_op == WAIT_OP_WRITE) */ {
            double diff_write = now - p_begin_wait_write;
            t_wait_write += static_cast<double>(acc_wait_write) * diff_write;
            p_begin_wait_write = now;
            p_wait_write += (acc_wait_write--) ? diff_write : 0.0;
        }
#ifdef THRILL_WAIT_LOG_ENABLED
        std::ofstream* waitlog = stxxl::logger::get_instance()->waitlog_stream();
        if (waitlog)
            *waitlog << (now - last_reset) << "\t"
                     << ((wait_op == WAIT_OP_READ) ? diff : 0.0) << "\t"
                     << ((wait_op != WAIT_OP_READ) ? diff : 0.0) << "\t"
                     << t_wait_read << "\t" << t_wait_write << std::endl << std::flush;
#endif
    }
}
#endif

std::string format_with_SI_IEC_unit_multiplier(uint64_t number, const char* unit, int multiplier) {
    // may not overflow, std::numeric_limits<uint64>::max() == 16 EB
    static const char* endings[] = { "", "k", "M", "G", "T", "P", "E" };
    static const char* binary_endings[] = { "", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei" };
    std::ostringstream out;
    out << number << ' ';
    int scale = 0;
    double number_d = static_cast<double>(number);
    double multiplier_d = multiplier;
    while (number_d >= multiplier_d)
    {
        number_d /= multiplier_d;
        ++scale;
    }
    if (scale > 0)
        out << '(' << std::fixed << std::setprecision(3) << number_d << ' '
            << (multiplier == 1024 ? binary_endings[scale] : endings[scale])
            << (unit ? unit : "") << ") ";
    else if (unit && *unit)
        out << unit << ' ';
    return out.str();
}

std::ostream& operator << (std::ostream& o, const StatsData& s) {
#define hr add_IEC_binary_multiplier
    o << "Thrill I/O statistics" << std::endl;
#if THRILL_IO_STATS
    o << " total number of reads                      : " << hr(s.get_reads()) << std::endl;
    o << " average block size (read)                  : "
      << hr(s.get_reads() ? s.get_read_volume() / s.get_reads() : 0, "B") << std::endl;
    o << " number of bytes read from disks            : " << hr(s.get_read_volume(), "B") << std::endl;
    o << " time spent in serving all read requests    : " << s.get_read_time() << " s"
      << " @ " << (static_cast<double>(s.get_read_volume()) / 1048576.0 / s.get_read_time()) << " MiB/s"
      << std::endl;
    o << " time spent in reading (parallel read time) : " << s.get_pread_time() << " s"
      << " @ " << (static_cast<double>(s.get_read_volume()) / 1048576.0 / s.get_pread_time()) << " MiB/s"
      << std::endl;
    if (s.get_cached_reads()) {
        o << " total number of cached reads               : " << hr(s.get_cached_reads()) << std::endl;
        o << " average block size (cached read)           : " << hr(s.get_cached_read_volume() / s.get_cached_reads(), "B") << std::endl;
        o << " number of bytes read from cache            : " << hr(s.get_cached_read_volume(), "B") << std::endl;
    }
    if (s.get_cached_writes()) {
        o << " total number of cached writes              : " << hr(s.get_cached_writes()) << std::endl;
        o << " average block size (cached write)          : " << hr(s.get_cached_written_volume() / s.get_cached_writes(), "B") << std::endl;
        o << " number of bytes written to cache           : " << hr(s.get_cached_written_volume(), "B") << std::endl;
    }
    o << " total number of writes                     : " << hr(s.get_writes()) << std::endl;
    o << " average block size (write)                 : "
      << hr(s.get_writes() ? s.get_written_volume() / s.get_writes() : 0, "B") << std::endl;
    o << " number of bytes written to disks           : " << hr(s.get_written_volume(), "B") << std::endl;
    o << " time spent in serving all write requests   : " << s.get_write_time() << " s"
      << " @ " << (static_cast<double>(s.get_written_volume()) / 1048576.0 / s.get_write_time()) << " MiB/s"
      << std::endl;
    o << " time spent in writing (parallel write time): " << s.get_pwrite_time() << " s"
      << " @ " << (static_cast<double>(s.get_written_volume()) / 1048576.0 / s.get_pwrite_time()) << " MiB/s"
      << std::endl;
    o << " time spent in I/O (parallel I/O time)      : " << s.get_pio_time() << " s"
      << " @ " << ((static_cast<double>(s.get_read_volume() + s.get_written_volume())) / 1048576.0 / s.get_pio_time()) << " MiB/s"
      << std::endl;
#else
    o << " n/a" << std::endl;
#endif
#ifndef THRILL_DO_NOT_COUNT_WAIT_TIME
    o << " I/O wait time                              : " << s.get_io_wait_time() << " s" << std::endl;
    if (s.get_wait_read_time() != 0.0)
        o << " I/O wait4read time                         : " << s.get_wait_read_time() << " s" << std::endl;
    if (s.get_wait_write_time() != 0.0)
        o << " I/O wait4write time                        : " << s.get_wait_write_time() << " s" << std::endl;
#endif
    o << " Time since the last reset                  : " << s.get_elapsed_time() << " s" << std::endl;
    return o;
#undef hr
}

} // namespace io
} // namespace thrill

/******************************************************************************/
