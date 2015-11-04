/*******************************************************************************
 * thrill/io/iostats.hpp
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

#pragma once
#ifndef THRILL_IO_IOSTATS_HEADER
#define THRILL_IO_IOSTATS_HEADER

#ifndef STXXL_IO_STATS
 #define STXXL_IO_STATS 1
#endif

// #include <stxxl/bits/common/timer.h>
// #include <stxxl/bits/common/types.h>
// #include <stxxl/bits/common/utils.h>
#include <thrill/common/defines.hpp>

#include <iostream>
#include <mutex>
#include <string>

#include <sys/time.h>

namespace thrill {
namespace io {

//! \addtogroup iolayer
//!
//! \{

/******************************************************************************/

template <typename INSTANCE, bool destroy_on_exit = true>
class singleton
{
    using instance_type = INSTANCE;
    using instance_pointer = instance_type *;
    using volatile_instance_pointer = volatile instance_pointer;

    static volatile_instance_pointer instance;

    static instance_pointer create_instance();
    static void destroy_instance();

public:
    singleton() = default;

    //! non-copyable: delete copy-constructor
    singleton(const singleton&) = delete;
    //! non-copyable: delete assignment operator
    singleton& operator = (const singleton&) = delete;
    //! move-constructor: default
    singleton(singleton&&) = default;
    //! move-assignment operator: default
    singleton& operator = (singleton&&) = default;

    inline static instance_pointer get_instance() {
        if (!instance)
            return create_instance();

        return instance;
    }
};

template <typename INSTANCE, bool destroy_on_exit>
typename singleton<INSTANCE, destroy_on_exit>::instance_pointer
singleton<INSTANCE, destroy_on_exit>::create_instance() {
    static std::mutex create_mutex;
    std::unique_lock<std::mutex> lock(create_mutex);
    if (!instance) {
        instance = new instance_type();
        if (destroy_on_exit)
            atexit(destroy_instance);
    }
    return instance;
}

template <typename INSTANCE, bool destroy_on_exit>
void singleton<INSTANCE, destroy_on_exit>::destroy_instance() {
    instance_pointer inst = instance;
    // instance = nullptr;
    instance = reinterpret_cast<instance_pointer>(size_t(-1));     // bomb if used again
    delete inst;
}

template <typename INSTANCE, bool destroy_on_exit>
typename singleton<INSTANCE, destroy_on_exit>::volatile_instance_pointer
singleton<INSTANCE, destroy_on_exit>::instance = nullptr;

//! Returns number of seconds since the epoch, high resolution.
static inline double
timestamp() {
#if STXXL_BOOST_TIMESTAMP
    boost::posix_time::ptime MyTime = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration Duration =
        MyTime - boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
    double sec = double(Duration.hours()) * 3600. +
                 double(Duration.minutes()) * 60. +
                 double(Duration.seconds()) +
                 double(Duration.fractional_seconds()) / (pow(10., Duration.num_fractional_digits()));
    return sec;
#elif STXXL_WINDOWS
    return GetTickCount() / 1000.0;
#else
    struct timeval tp;
    gettimeofday(&tp, nullptr);
    return double(tp.tv_sec) + double(tp.tv_usec) / 1000000.;
#endif
}

/******************************************************************************/

//! Collects various I/O statistics.
class stats : public singleton<stats>
{
    friend class singleton;

    unsigned reads, writes;                     // number of operations
    int64_t volume_read, volume_written;        // number of bytes read/written
    unsigned c_reads, c_writes;                 // number of cached operations
    int64_t c_volume_read, c_volume_written;    // number of bytes read/written from/to cache
    double t_reads, t_writes;                   // seconds spent in operations
    double p_reads, p_writes;                   // seconds spent in parallel operations
    double p_begin_read, p_begin_write;         // start time of parallel operation
    double p_ios;                               // seconds spent in all parallel I/O operations (read and write)
    double p_begin_io;
    double t_waits, p_waits;                    // seconds spent waiting for completion of I/O operations
    double p_begin_wait;
    double t_wait_read, p_wait_read;
    double p_begin_wait_read;
    double t_wait_write, p_wait_write;
    double p_begin_wait_write;
    int acc_reads, acc_writes;                  // number of requests, participating in parallel operation
    int acc_ios;
    int acc_waits;
    int acc_wait_read, acc_wait_write;
    double last_reset;
    std::mutex read_mutex, write_mutex, io_mutex, wait_mutex;

    stats();

public:
    enum wait_op_type {
        WAIT_OP_ANY,
        WAIT_OP_READ,
        WAIT_OP_WRITE
    };

    class scoped_read_write_timer
    {
        using size_type = size_t;

        bool is_write;
#if STXXL_IO_STATS
        bool running;
#endif

    public:
        scoped_read_write_timer(size_type size, bool is_write = false)
            : is_write(is_write)
#if STXXL_IO_STATS
              , running(false)
#endif
        {
            start(size);
        }

        ~scoped_read_write_timer() {
            stop();
        }

        void start(size_type size) {
#if STXXL_IO_STATS
            if (!running) {
                running = true;
                if (is_write)
                    stats::get_instance()->write_started(size);
                else
                    stats::get_instance()->read_started(size);
            }
#else
            common::THRILL_UNUSED(size);
#endif
        }

        void stop() {
#if STXXL_IO_STATS
            if (running) {
                if (is_write)
                    stats::get_instance()->write_finished();
                else
                    stats::get_instance()->read_finished();
                running = false;
            }
#endif
        }
    };

    class scoped_write_timer
    {
        using size_type = size_t;

#if STXXL_IO_STATS
        bool running;
#endif

    public:
        scoped_write_timer(size_type size)
#if STXXL_IO_STATS
            : running(false)
#endif
        {
            start(size);
        }

        ~scoped_write_timer() {
            stop();
        }

        void start(size_type size) {
#if STXXL_IO_STATS
            if (!running) {
                running = true;
                stats::get_instance()->write_started(size);
            }
#else
            common::THRILL_UNUSED(size);
#endif
        }

        void stop() {
#if STXXL_IO_STATS
            if (running) {
                stats::get_instance()->write_finished();
                running = false;
            }
#endif
        }
    };

    class scoped_read_timer
    {
        using size_type = size_t;

#if STXXL_IO_STATS
        bool running;
#endif

    public:
        scoped_read_timer(size_type size)
#if STXXL_IO_STATS
            : running(false)
#endif
        {
            start(size);
        }

        ~scoped_read_timer() {
            stop();
        }

        void start(size_type size) {
#if STXXL_IO_STATS
            if (!running) {
                running = true;
                stats::get_instance()->read_started(size);
            }
#else
            common::THRILL_UNUSED(size);
#endif
        }

        void stop() {
#if STXXL_IO_STATS
            if (running) {
                stats::get_instance()->read_finished();
                running = false;
            }
#endif
        }
    };

    class scoped_wait_timer
    {
#ifndef STXXL_DO_NOT_COUNT_WAIT_TIME
        bool running;
        wait_op_type wait_op;
#endif

    public:
        scoped_wait_timer(wait_op_type wait_op, bool measure_time = true)
#ifndef STXXL_DO_NOT_COUNT_WAIT_TIME
            : running(false), wait_op(wait_op)
#endif
        {
            if (measure_time)
                start();
        }

        ~scoped_wait_timer() {
            stop();
        }

        void start() {
#ifndef STXXL_DO_NOT_COUNT_WAIT_TIME
            if (!running) {
                running = true;
                stats::get_instance()->wait_started(wait_op);
            }
#endif
        }

        void stop() {
#ifndef STXXL_DO_NOT_COUNT_WAIT_TIME
            if (running) {
                stats::get_instance()->wait_finished(wait_op);
                running = false;
            }
#endif
        }
    };

public:
    //! Returns total number of reads.
    //! \return total number of reads
    unsigned get_reads() const {
        return reads;
    }

    //! Returns total number of writes.
    //! \return total number of writes
    unsigned get_writes() const {
        return writes;
    }

    //! Returns number of bytes read from disks.
    //! \return number of bytes read
    int64_t get_read_volume() const {
        return volume_read;
    }

    //! Returns number of bytes written to the disks.
    //! \return number of bytes written
    int64_t get_written_volume() const {
        return volume_written;
    }

    //! Returns total number of reads served from cache.
    //! \return total number of cached reads
    unsigned get_cached_reads() const {
        return c_reads;
    }

    //! Returns total number of cached writes.
    //! \return total number of cached writes
    unsigned get_cached_writes() const {
        return c_writes;
    }

    //! Returns number of bytes read from cache.
    //! \return number of bytes read from cache
    int64_t get_cached_read_volume() const {
        return c_volume_read;
    }

    //! Returns number of bytes written to the cache.
    //! \return number of bytes written to cache
    int64_t get_cached_written_volume() const {
        return c_volume_written;
    }

    //! Time that would be spent in read syscalls if all parallel reads were serialized.
    //! \return seconds spent in reading
    double get_read_time() const {
        return t_reads;
    }

    //! Time that would be spent in write syscalls if all parallel writes were serialized.
    //! \return seconds spent in writing
    double get_write_time() const {
        return t_writes;
    }

    //! Period of time when at least one I/O thread was executing a read.
    //! \return seconds spent in reading
    double get_pread_time() const {
        return p_reads;
    }

    //! Period of time when at least one I/O thread was executing a write.
    //! \return seconds spent in writing
    double get_pwrite_time() const {
        return p_writes;
    }

    //! Period of time when at least one I/O thread was executing a read or a write.
    //! \return seconds spent in I/O
    double get_pio_time() const {
        return p_ios;
    }

    //! I/O wait time counter.
    //! \return number of seconds spent in I/O waiting functions \link
    //! request::wait request::wait \endlink, \c wait_any and \c wait_all
    double get_io_wait_time() const {
        return t_waits;
    }

    double get_wait_read_time() const {
        return t_wait_read;
    }

    double get_wait_write_time() const {
        return t_wait_write;
    }

    //! Return time of the last reset.
    //! \return seconds passed from the last reset()
    double get_last_reset_time() const {
        return last_reset;
    }

    // for library use
    void write_started(size_t size_, double now = 0.0);
    void write_canceled(size_t size_);
    void write_finished();
    void write_cached(size_t size_);
    void read_started(size_t size_, double now = 0.0);
    void read_canceled(size_t size_);
    void read_finished();
    void read_cached(size_t size_);
    void wait_started(wait_op_type wait_op);
    void wait_finished(wait_op_type wait_op);
};

#if !STXXL_IO_STATS
inline void stats::write_started(size_t size_, double now) {
    common::THRILL_UNUSED(size_);
    common::THRILL_UNUSED(now);
}
inline void stats::write_cached(size_t size_) {
    common::THRILL_UNUSED(size_);
}
inline void stats::write_finished() { }
inline void stats::read_started(size_t size_, double now) {
    common::THRILL_UNUSED(size_);
    common::THRILL_UNUSED(now);
}
inline void stats::read_cached(size_t size_) {
    common::THRILL_UNUSED(size_);
}
inline void stats::read_finished() { }
#endif
#ifdef STXXL_DO_NOT_COUNT_WAIT_TIME
inline void stats::wait_started(wait_op_type) { }
inline void stats::wait_finished(wait_op_type) { }
#endif

class stats_data
{
    //! number of operations
    unsigned reads, writes;
    //! number of bytes read/written
    int64_t volume_read, volume_written;
    //! number of cached operations
    unsigned c_reads, c_writes;
    //! number of bytes read/written from/to cache
    int64_t c_volume_read, c_volume_written;
    //! seconds spent in operations
    double t_reads, t_writes;
    //! seconds spent in parallel operations
    double p_reads, p_writes;
    //! seconds spent in all parallel I/O operations (read and write)
    double p_ios;
    //! seconds spent waiting for completion of I/O operations
    double t_wait;
    double t_wait_read, t_wait_write;
    double elapsed;

public:
    stats_data()
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
          p_ios(0.0),
          t_wait(0.0),
          t_wait_read(0.0),
          t_wait_write(0.0),
          elapsed(0.0)
    { }

    stats_data(const stats& s)
        : reads(s.get_reads()),
          writes(s.get_writes()),
          volume_read(s.get_read_volume()),
          volume_written(s.get_written_volume()),
          c_reads(s.get_cached_reads()),
          c_writes(s.get_cached_writes()),
          c_volume_read(s.get_cached_read_volume()),
          c_volume_written(s.get_cached_written_volume()),
          t_reads(s.get_read_time()),
          t_writes(s.get_write_time()),
          p_reads(s.get_pread_time()),
          p_writes(s.get_pwrite_time()),
          p_ios(s.get_pio_time()),
          t_wait(s.get_io_wait_time()),
          t_wait_read(s.get_wait_read_time()),
          t_wait_write(s.get_wait_write_time()),
          elapsed(timestamp() - s.get_last_reset_time())
    { }

    stats_data operator + (const stats_data& a) const {
        stats_data s;
        s.reads = reads + a.reads;
        s.writes = writes + a.writes;
        s.volume_read = volume_read + a.volume_read;
        s.volume_written = volume_written + a.volume_written;
        s.c_reads = c_reads + a.c_reads;
        s.c_writes = c_writes + a.c_writes;
        s.c_volume_read = c_volume_read + a.c_volume_read;
        s.c_volume_written = c_volume_written + a.c_volume_written;
        s.t_reads = t_reads + a.t_reads;
        s.t_writes = t_writes + a.t_writes;
        s.p_reads = p_reads + a.p_reads;
        s.p_writes = p_writes + a.p_writes;
        s.p_ios = p_ios + a.p_ios;
        s.t_wait = t_wait + a.t_wait;
        s.t_wait_read = t_wait_read + a.t_wait_read;
        s.t_wait_write = t_wait_write + a.t_wait_write;
        s.elapsed = elapsed + a.elapsed;
        return s;
    }

    stats_data operator - (const stats_data& a) const {
        stats_data s;
        s.reads = reads - a.reads;
        s.writes = writes - a.writes;
        s.volume_read = volume_read - a.volume_read;
        s.volume_written = volume_written - a.volume_written;
        s.c_reads = c_reads - a.c_reads;
        s.c_writes = c_writes - a.c_writes;
        s.c_volume_read = c_volume_read - a.c_volume_read;
        s.c_volume_written = c_volume_written - a.c_volume_written;
        s.t_reads = t_reads - a.t_reads;
        s.t_writes = t_writes - a.t_writes;
        s.p_reads = p_reads - a.p_reads;
        s.p_writes = p_writes - a.p_writes;
        s.p_ios = p_ios - a.p_ios;
        s.t_wait = t_wait - a.t_wait;
        s.t_wait_read = t_wait_read - a.t_wait_read;
        s.t_wait_write = t_wait_write - a.t_wait_write;
        s.elapsed = elapsed - a.elapsed;
        return s;
    }

    unsigned get_reads() const {
        return reads;
    }

    unsigned get_writes() const {
        return writes;
    }

    int64_t get_read_volume() const {
        return volume_read;
    }

    int64_t get_written_volume() const {
        return volume_written;
    }

    unsigned get_cached_reads() const {
        return c_reads;
    }

    unsigned get_cached_writes() const {
        return c_writes;
    }

    int64_t get_cached_read_volume() const {
        return c_volume_read;
    }

    int64_t get_cached_written_volume() const {
        return c_volume_written;
    }

    double get_read_time() const {
        return t_reads;
    }

    double get_write_time() const {
        return t_writes;
    }

    double get_pread_time() const {
        return p_reads;
    }

    double get_pwrite_time() const {
        return p_writes;
    }

    double get_pio_time() const {
        return p_ios;
    }

    double get_elapsed_time() const {
        return elapsed;
    }

    double get_io_wait_time() const {
        return t_wait;
    }

    double get_wait_read_time() const {
        return t_wait_read;
    }

    double get_wait_write_time() const {
        return t_wait_write;
    }
};

std::ostream& operator << (std::ostream& o, const stats_data& s);

inline std::ostream& operator << (std::ostream& o, const stats& s) {
    o << stats_data(s);
    return o;
}

std::string format_with_SI_IEC_unit_multiplier(uint64_t number, const char* unit = "", int multiplier = 1000);

inline std::string add_IEC_binary_multiplier(uint64_t number, const char* unit = "") {
    return format_with_SI_IEC_unit_multiplier(number, unit, 1024);
}

inline std::string add_SI_multiplier(uint64_t number, const char* unit = "") {
    return format_with_SI_IEC_unit_multiplier(number, unit, 1000);
}

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_IOSTATS_HEADER

/******************************************************************************/
