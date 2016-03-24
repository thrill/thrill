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

#ifndef THRILL_IO_STATS_TIMING
 #define THRILL_IO_STATS_TIMING 0
 #define THRILL_DO_NOT_COUNT_WAIT_TIME 1
#endif

#include <thrill/common/config.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/singleton.hpp>

#include <chrono>
#include <mutex>
#include <ostream>
#include <string>

namespace thrill {
namespace io {

//! \addtogroup io_layer
//!
//! \{

//! Returns number of seconds since the epoch, high resolution.
static inline double
timestamp() {
    return static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count()) / 1e6;
}

//! Collects various I/O statistics.
class Stats : public common::Singleton<Stats>
{
    friend class common::Singleton<Stats>;

    //! number of operations
    size_t read_ops_ = 0, write_ops_ = 0;
    //! number of bytes read/written
    int64_t read_volume_ = 0, write_volume_ = 0;
    //! number of cached operations
    size_t cached_read_ops_ = 0, cached_write_ops_ = 0;
    //! number of bytes read/written from/to cache
    int64_t cached_read_volume_ = 0, cached_write_volume_ = 0;
    //! seconds spent in operations
    double read_time_ = 0.0, write_time_ = 0.0;
    //! seconds spent in parallel operations
    double parallel_read_time_ = 0.0, parallel_write_time_ = 0.0;
    //! seconds spent in all parallel I/O operations (read and write)
    double parallel_io_time_ = 0.0;
    //! seconds spent waiting for completion of I/O operations
    double io_wait_time_ = 0.0;
    double read_wait_time_ = 0.0, write_wait_time_ = 0.0;
#if THRILL_IO_STATS_TIMING
    //! start time of parallel operation
    double parallel_read_begin_ = 0.0, parallel_write_begin_ = 0.0;
    double parallel_io_begin_ = 0.0;
    double parallel_wait_begin_ = 0.0;
    double parallel_wait_read_begin_ = 0.0;
    double parallel_wait_write_begin_ = 0.0;
    double parallel_wait_time_ = 0.0;
    double parallel_wait_read_time_ = 0.0;
    double parallel_wait_write_time_ = 0.0;
    //! number of requests, participating in parallel operation
    int acc_reads_ = 0, acc_writes_ = 0;
    int acc_ios_ = 0;
    int acc_waits_ = 0;
    int acc_wait_read_ = 0, acc_wait_write_ = 0;
#endif
    double last_reset_time_ = 0.0;

    std::mutex read_mutex_, write_mutex_, io_mutex_, wait_mutex_;

    Stats();

public:
    enum class WaitOp {
        ANY, READ, WRITE
    };

    class ScopedReadWriteTimer
    {
        using size_type = size_t;

        bool is_write_;
        bool running_ = false;

    public:
        explicit ScopedReadWriteTimer(size_type size, bool is_write = false)
            : is_write_(is_write) {
            Start(size);
        }

        ~ScopedReadWriteTimer() {
            Stop();
        }

        void Start(size_type size) {
            if (!running_) {
                running_ = true;
                if (is_write_)
                    Stats::GetInstance()->write_started(size);
                else
                    Stats::GetInstance()->read_started(size);
            }
        }

        void Stop() {
            if (running_) {
                if (is_write_)
                    Stats::GetInstance()->write_finished();
                else
                    Stats::GetInstance()->read_finished();
                running_ = false;
            }
        }
    };

    class ScopedWriteTimer
    {
        using size_type = size_t;
        bool running_ = false;

    public:
        explicit ScopedWriteTimer(size_type size) {
            Start(size);
        }

        ~ScopedWriteTimer() {
            Stop();
        }

        void Start(size_type size) {
            if (!running_) {
                running_ = true;
                Stats::GetInstance()->write_started(size);
            }
        }

        void Stop() {
            if (running_) {
                Stats::GetInstance()->write_finished();
                running_ = false;
            }
        }
    };

    class ScopedReadTimer
    {
        using size_type = size_t;

        bool running_ = false;

    public:
        explicit ScopedReadTimer(size_type size) {
            Start(size);
        }

        ~ScopedReadTimer() {
            Stop();
        }

        void Start(size_type size) {
            if (!running_) {
                running_ = true;
                Stats::GetInstance()->read_started(size);
            }
        }

        void Stop() {
            if (running_) {
                Stats::GetInstance()->read_finished();
                running_ = false;
            }
        }
    };

    class ScopedWaitTimer
    {
#if !THRILL_DO_NOT_COUNT_WAIT_TIME
        bool running_ = false;
        WaitOp wait_op_;
#endif

    public:
        explicit ScopedWaitTimer(WaitOp wait_op, bool measure_time = true)
#if !THRILL_DO_NOT_COUNT_WAIT_TIME
            : wait_op_(wait_op)
#endif
        {
            if (measure_time)
                Start();
#if THRILL_DO_NOT_COUNT_WAIT_TIME
            common::THRILL_UNUSED(wait_op);
#endif
        }

        ~ScopedWaitTimer() {
            Stop();
        }

        void Start() {
#if !THRILL_DO_NOT_COUNT_WAIT_TIME
            if (!running_) {
                running_ = true;
                Stats::GetInstance()->wait_started(wait_op_);
            }
#endif
        }

        void Stop() {
#if !THRILL_DO_NOT_COUNT_WAIT_TIME
            if (running_) {
                Stats::GetInstance()->wait_finished(wait_op_);
                running_ = false;
            }
#endif
        }
    };

public:
    //! Returns total number of read_ops.
    size_t read_ops() const { return read_ops_; }

    //! Returns total number of write_ops.
    size_t write_ops() const { return write_ops_; }

    //! Returns number of bytes read from disks.
    int64_t read_volume() const { return read_volume_; }

    //! Returns number of bytes written to the disks.
    int64_t write_volume() const { return write_volume_; }

    //! Returns total number of reads served from cache.
    size_t cached_read_ops() const { return cached_read_ops_; }

    //! Returns total number of cached write_ops.
    size_t cached_write_ops() const { return cached_write_ops_; }

    //! Returns number of bytes read from cache.
    int64_t cached_read_volume() const { return cached_read_volume_; }

    //! Returns number of bytes written to the cache.
    int64_t cached_write_volume() const { return cached_write_volume_; }

    //! Time that would be spent in read syscalls if all parallel read_ops were
    //! serialized.
    double read_time() const { return read_time_; }

    //! Time that would be spent in write syscalls if all parallel write_ops were
    //! serialized.
    double write_time() const { return write_time_; }

    //! Period of time when at least one I/O thread was executing a read.
    double parallel_read_time() const { return parallel_read_time_; }

    //! Period of time when at least one I/O thread was executing a write.
    double parallel_write_time() const { return parallel_write_time_; }

    //! Period of time when at least one I/O thread was executing a read or a
    //! write.
    double parallel_io_time() const { return parallel_io_time_; }

    //! I/O wait time counter.
    double io_wait_time() const { return io_wait_time_; }

    double read_wait_time() const { return read_wait_time_; }

    double write_wait_time() const { return write_wait_time_; }

    //! Return time of the last reset.
    double last_reset_time() const { return last_reset_time_; }

    // for library use
    void write_started(size_t size, double now = 0.0);
    void write_canceled(size_t size);
    void write_finished();
    void write_cached(size_t size);
    void read_started(size_t size, double now = 0.0);
    void read_canceled(size_t size);
    void read_finished();
    void read_cached(size_t size);
    void wait_started(WaitOp wait_op);
    void wait_finished(WaitOp wait_op);
};

#ifdef THRILL_DO_NOT_COUNT_WAIT_TIME
inline void Stats::wait_started(WaitOp) { }
inline void Stats::wait_finished(WaitOp) { }
#endif

class StatsData
{
    //! number of operations
    size_t read_ops_ = 0, write_ops_ = 0;
    //! number of bytes read/written
    int64_t read_volume_ = 0, write_volume_ = 0;
    //! number of cached operations
    size_t cached_read_ops_ = 0, cached_write_ops_ = 0;
    //! number of bytes read/written from/to cache
    int64_t cached_read_volume_ = 0, cached_write_volume_ = 0;
    //! seconds spent in operations
    double read_time_ = 0.0, write_time_ = 0.0;
    //! seconds spent in parallel operations
    double parallel_read_time_ = 0.0, parallel_write_time_ = 0.0;
    //! seconds spent in all parallel I/O operations (read and write)
    double parallel_io_time_ = 0.0;
    //! seconds spent waiting for completion of I/O operations
    double io_wait_time_ = 0.0;
    double read_wait_time_ = 0.0, write_wait_time_ = 0.0;
    double elapsed_time_ = 0.0;

public:
    StatsData() = default;

    explicit StatsData(const Stats& s)
        : read_ops_(s.read_ops()),
          write_ops_(s.write_ops()),
          read_volume_(s.read_volume()),
          write_volume_(s.write_volume()),
          cached_read_ops_(s.cached_read_ops()),
          cached_write_ops_(s.cached_write_ops()),
          cached_read_volume_(s.cached_read_volume()),
          cached_write_volume_(s.cached_write_volume()),
          read_time_(s.read_time()),
          write_time_(s.write_time()),
          parallel_read_time_(s.parallel_read_time()),
          parallel_write_time_(s.parallel_write_time()),
          parallel_io_time_(s.parallel_io_time()),
          io_wait_time_(s.io_wait_time()),
          read_wait_time_(s.read_wait_time()),
          write_wait_time_(s.write_wait_time()),
          elapsed_time_(timestamp() - s.last_reset_time())
    { }

    StatsData operator + (const StatsData& a) const {
        StatsData s;
        s.read_ops_ = read_ops_ + a.read_ops_;
        s.write_ops_ = write_ops_ + a.write_ops_;
        s.read_volume_ = read_volume_ + a.read_volume_;
        s.write_volume_ = write_volume_ + a.write_volume_;
        s.cached_read_ops_ = cached_read_ops_ + a.cached_read_ops_;
        s.cached_write_ops_ = cached_write_ops_ + a.cached_write_ops_;
        s.cached_read_volume_ = cached_read_volume_ + a.cached_read_volume_;
        s.cached_write_volume_ = cached_write_volume_ + a.cached_write_volume_;
        s.read_time_ = read_time_ + a.read_time_;
        s.write_time_ = write_time_ + a.write_time_;
        s.parallel_read_time_ = parallel_read_time_ + a.parallel_read_time_;
        s.parallel_write_time_ = parallel_write_time_ + a.parallel_write_time_;
        s.parallel_io_time_ = parallel_io_time_ + a.parallel_io_time_;
        s.io_wait_time_ = io_wait_time_ + a.io_wait_time_;
        s.read_wait_time_ = read_wait_time_ + a.read_wait_time_;
        s.write_wait_time_ = write_wait_time_ + a.write_wait_time_;
        s.elapsed_time_ = elapsed_time_ + a.elapsed_time_;
        return s;
    }

    StatsData operator - (const StatsData& a) const {
        StatsData s;
        s.read_ops_ = read_ops_ - a.read_ops_;
        s.write_ops_ = write_ops_ - a.write_ops_;
        s.read_volume_ = read_volume_ - a.read_volume_;
        s.write_volume_ = write_volume_ - a.write_volume_;
        s.cached_read_ops_ = cached_read_ops_ - a.cached_read_ops_;
        s.cached_write_ops_ = cached_write_ops_ - a.cached_write_ops_;
        s.cached_read_volume_ = cached_read_volume_ - a.cached_read_volume_;
        s.cached_write_volume_ = cached_write_volume_ - a.cached_write_volume_;
        s.read_time_ = read_time_ - a.read_time_;
        s.write_time_ = write_time_ - a.write_time_;
        s.parallel_read_time_ = parallel_read_time_ - a.parallel_read_time_;
        s.parallel_write_time_ = parallel_write_time_ - a.parallel_write_time_;
        s.parallel_io_time_ = parallel_io_time_ - a.parallel_io_time_;
        s.io_wait_time_ = io_wait_time_ - a.io_wait_time_;
        s.read_wait_time_ = read_wait_time_ - a.read_wait_time_;
        s.write_wait_time_ = write_wait_time_ - a.write_wait_time_;
        s.elapsed_time_ = elapsed_time_ - a.elapsed_time_;
        return s;
    }

    size_t read_ops() const {
        return read_ops_;
    }

    size_t write_ops() const {
        return write_ops_;
    }

    int64_t read_volume() const {
        return read_volume_;
    }

    int64_t write_volume() const {
        return write_volume_;
    }

    size_t cached_read_ops() const {
        return cached_read_ops_;
    }

    size_t cached_write_ops() const {
        return cached_write_ops_;
    }

    int64_t cached_read_volume() const {
        return cached_read_volume_;
    }

    int64_t cached_write_volume() const {
        return cached_write_volume_;
    }

    double read_time() const {
        return read_time_;
    }

    double write_time() const {
        return write_time_;
    }

    double parallel_read_time() const {
        return parallel_read_time_;
    }

    double parallel_write_time() const {
        return parallel_write_time_;
    }

    double parallel_io_time() const {
        return parallel_io_time_;
    }

    double elapsed_time() const {
        return elapsed_time_;
    }

    double io_wait_time() const {
        return io_wait_time_;
    }

    double read_wait_time() const {
        return read_wait_time_;
    }

    double write_wait_time() const {
        return write_wait_time_;
    }
};

std::ostream& operator << (std::ostream& o, const StatsData& s);

inline std::ostream& operator << (std::ostream& o, const Stats& s) {
    o << StatsData(s);
    return o;
}

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_IOSTATS_HEADER

/******************************************************************************/
