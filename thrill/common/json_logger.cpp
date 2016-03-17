/*******************************************************************************
 * thrill/common/json_logger.cpp
 *
 * Logger for statistical output in JSON format for post-processing.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/json_logger.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace thrill {
namespace common {

using steady_clock = std::chrono::steady_clock;

/******************************************************************************/
// LinuxSystemStats

class LinuxProcStats
{
    static constexpr bool debug = true;

public:
    explicit LinuxProcStats(JsonLogger& logger) : logger_(logger) {

        file_stat_.open("/proc/stat");
        file_net_dev_.open("/proc/net/dev");

        pid_t mypid = getpid();
        file_pid_stat_.open("/proc/" + std::to_string(mypid) + "/stat");
        file_pid_io_.open("/proc/" + std::to_string(mypid) + "/io");
    }

    //! calculate percentage of change relative to base.
    static double perc(unsigned long long prev, unsigned long long curr,
                       unsigned long long base) {
        if (curr < prev)
            return 0.0;
        else
            return static_cast<double>(curr - prev) / base * 100.0;
    }

    //! method to prepare JsonLine
    void common_out(JsonLine& out);

    //! read /proc/stat
    void read_stat(JsonLine& out);

    //! read /proc/<pid>/stat
    void read_pid_stat(JsonLine& out);

    //! read /proc/net/dev
    void read_net_dev(const steady_clock::time_point& tp, JsonLine& out);

    //! read /proc/<pid>/io
    void read_pid_io(const steady_clock::time_point& tp, JsonLine& out);

    void tick(const steady_clock::time_point& tp) {

        // JsonLine to construct
        JsonLine out = logger_.line();

        read_stat(out);
        read_pid_stat(out);
        read_net_dev(tp, out);
        read_pid_io(tp, out);

        tp_last_ = tp;
    }

private:
    //! reference to JsonLogger for output
    JsonLogger& logger_;

    //! open file handle to /proc/stat
    std::ifstream file_stat_;
    //! open file handle to /proc/net/dev
    std::ifstream file_net_dev_;
    //! open file handle to /proc/<our-pid>/stat
    std::ifstream file_pid_stat_;
    //! open file handle to /proc/<our-pid>/io
    std::ifstream file_pid_io_;

    //! last time point called
    steady_clock::time_point tp_last_;

    struct CpuStat {
        unsigned long long user = 0;
        unsigned long long nice = 0;
        unsigned long long sys = 0;
        unsigned long long idle = 0;
        unsigned long long iowait = 0;
        unsigned long long steal = 0;
        unsigned long long hardirq = 0;
        unsigned long long softirq = 0;
        unsigned long long guest = 0;
        unsigned long long guest_nice = 0;

        //! total uptime across all modes
        unsigned long long uptime() const {
            return user + nice + sys + idle
                   + iowait + hardirq + steal + softirq;
        }
        //! return pure user mode time excluding virtual guests
        unsigned long long user_plain() const { return user - guest; }
        //! return pure nice mode time excluding virtual guests
        unsigned long long nice_plain() const { return nice - guest_nice; }
    };

    struct PidStat {
        unsigned long long check_pid = 0;
        unsigned long long utime = 0;
        unsigned long long stime = 0;
        unsigned long long cutime = 0;
        unsigned long long cstime = 0;
        unsigned long long num_threads = 0;
        unsigned long long vsize = 0;
        unsigned long long rss = 0;
    };

    struct NetDevStat {
        std::string        if_name;
        unsigned long long rx_pkts = 0;
        unsigned long long tx_pkts = 0;
        unsigned long long rx_bytes = 0;
        unsigned long long tx_bytes = 0;
    };

    struct PidIoStat {
        unsigned long long read_bytes = 0;
        unsigned long long write_bytes = 0;
    };

    //! delta jiffies since the last iteration (read from uptime() of the cpu
    //! summary)
    unsigned long long jiffies_delta_ = 0;

    //! previous summary cpu reading
    CpuStat cpu_prev_;

    //! previous cpu core reading
    std::vector<CpuStat> cpu_core_prev_;

    //! previous reading from pid's stat file
    PidStat pid_stat_prev_;

    //! previous reading from network stats
    std::vector<NetDevStat> net_dev_prev_;

    //! previous summarized reading from network stats
    NetDevStat net_dev_sum_prev_;

    //! find or create entry for net_dev
    NetDevStat& find_net_dev(const std::string& if_name);

    //! previous reading of pid's io file
    PidIoStat pid_io_prev_;
};

void LinuxProcStats::common_out(JsonLine& out) {
    if (out.items() == 2) {
        out << "class" << "LinuxProcStats";
    }
}

void LinuxProcStats::read_stat(JsonLine& out) {
    if (!file_stat_.is_open()) return;

    file_stat_.clear();
    file_stat_.seekg(0);
    if (!file_stat_.good()) return;

    // read the number of jiffies spent in the various modes since the
    // last tick.

    std::vector<double> cores_user, cores_nice, cores_sys, cores_idle,
        cores_iowait, cores_hardirq, cores_softirq,
        cores_steal, cores_guest, cores_guest_nice;

    std::string line;
    while (std::getline(file_stat_, line)) {
        if (common::StartsWith(line, "cpu  ")) {

            CpuStat curr;
            sscanf(line.data() + 5,
                   "%llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                   &curr.user,
                   &curr.nice,
                   &curr.sys,
                   &curr.idle,
                   &curr.iowait,
                   &curr.hardirq,
                   &curr.softirq,
                   &curr.steal,
                   &curr.guest,
                   &curr.guest_nice);

            CpuStat& prev = cpu_prev_;

            if (!prev.user) {
                // just store the first reading
                prev = curr;
                continue;
            }

            jiffies_delta_ = curr.uptime() - prev.uptime();
            unsigned long long base = jiffies_delta_;

            sLOG << "cpu"
                 << "delta" << jiffies_delta_
                 << "user" << perc(prev.user, curr.user, base)
                 << "nice" << perc(prev.nice, curr.nice, base)
                 << "sys" << perc(prev.sys, curr.sys, base)
                 << "iowait" << perc(prev.iowait, curr.iowait, base)
                 << "hardirq" << perc(prev.hardirq, curr.hardirq, base)
                 << "softirq" << perc(prev.softirq, curr.softirq, base)
                 << "steal" << perc(prev.steal, curr.steal, base)
                 << "guest" << perc(prev.guest, curr.guest, base)
                 << "guest_nice" << perc(prev.guest_nice, curr.guest_nice, base)
                 << "idle" << perc(prev.idle, curr.idle, base);

            common_out(out);
            out << "cpu_user" << perc(prev.user, curr.user, base)
                << "cpu_nice" << perc(prev.nice, curr.nice, base)
                << "cpu_sys" << perc(prev.sys, curr.sys, base)
                << "cpu_idle" << perc(prev.idle, curr.idle, base)
                << "cpu_iowait" << perc(prev.iowait, curr.iowait, base)
                << "cpu_hardirq" << perc(prev.hardirq, curr.hardirq, base)
                << "cpu_softirq" << perc(prev.softirq, curr.softirq, base)
                << "cpu_steal" << perc(prev.steal, curr.steal, base)
                << "cpu_guest" << perc(prev.guest, curr.guest, base)
                << "cpu_guest_nice" << perc(prev.guest_nice, curr.guest_nice, base);

            prev = curr;
        }
        else if (common::StartsWith(line, "cpu")) {

            unsigned core_id;
            CpuStat curr;
            sscanf(line.data() + 3,
                   "%u %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                   &core_id,
                   &curr.user,
                   &curr.nice,
                   &curr.sys,
                   &curr.idle,
                   &curr.iowait,
                   &curr.hardirq,
                   &curr.softirq,
                   &curr.steal,
                   &curr.guest,
                   &curr.guest_nice);

            if (cpu_core_prev_.size() < core_id + 1)
                cpu_core_prev_.resize(core_id + 1);

            CpuStat& prev = cpu_core_prev_[core_id];

            if (!prev.user) {
                // just store the first reading
                prev = curr;
                continue;
            }

            jiffies_delta_ = curr.uptime() - prev.uptime();
            unsigned long long base = jiffies_delta_;

            sLOG << "core" << core_id
                 << "delta" << jiffies_delta_
                 << "user" << perc(prev.user, curr.user, base)
                 << "nice" << perc(prev.nice, curr.nice, base)
                 << "sys" << perc(prev.sys, curr.sys, base)
                 << "iowait" << perc(prev.iowait, curr.iowait, base)
                 << "hardirq" << perc(prev.hardirq, curr.hardirq, base)
                 << "softirq" << perc(prev.softirq, curr.softirq, base)
                 << "steal" << perc(prev.steal, curr.steal, base)
                 << "guest" << perc(prev.guest, curr.guest, base)
                 << "guest_nice" << perc(prev.guest_nice, curr.guest_nice, base)
                 << "idle" << perc(prev.idle, curr.idle, base);

            cores_user.emplace_back(perc(prev.user, curr.user, base));
            cores_nice.emplace_back(perc(prev.nice, curr.nice, base));
            cores_sys.emplace_back(perc(prev.sys, curr.sys, base));
            cores_idle.emplace_back(perc(prev.idle, curr.idle, base));
            cores_iowait.emplace_back(perc(prev.iowait, curr.iowait, base));
            cores_hardirq.emplace_back(perc(prev.hardirq, curr.hardirq, base));
            cores_softirq.emplace_back(perc(prev.softirq, curr.softirq, base));
            cores_steal.emplace_back(perc(prev.steal, curr.steal, base));
            cores_guest.emplace_back(perc(prev.guest, curr.guest, base));
            cores_guest_nice.emplace_back(perc(prev.guest_nice, curr.guest_nice, base));

            prev = curr;
        }
    }

    if (cores_user.size()) {
        out << "cores_user" << cores_user
            << "cores_nice" << cores_nice
            << "cores_sys" << cores_sys
            << "cores_idle" << cores_idle
            << "cores_iowait" << cores_iowait
            << "cores_hardirq" << cores_hardirq
            << "cores_softirq" << cores_softirq
            << "cores_steal" << cores_steal
            << "cores_guest" << cores_guest
            << "cores_guest_nice" << cores_guest_nice;
    }
}

void LinuxProcStats::read_pid_stat(JsonLine& out) {
    if (!file_pid_stat_.is_open()) return;

    file_pid_stat_.clear();
    file_pid_stat_.seekg(0);
    if (!file_pid_stat_.good()) return;

    std::string line;
    std::getline(file_pid_stat_, line);

    PidStat curr;

    /* Field          Content */
    /*  pid           process id */
    /*  tcomm         filename of the executable */
    /*  state         state (R is running, S is sleeping, D is sleeping in an */
    /*                uninterruptible wait, Z is zombie, T is traced or stopped) */
    /*  ppid          process id of the parent process */
    /*  pgrp          pgrp of the process */
    /*  sid           session id */
    /*  tty_nr        tty the process uses */
    /*  tty_pgrp      pgrp of the tty */
    /*  flags         task flags */
    /*  min_flt       number of minor faults */
    /*  cmin_flt      number of minor faults with child's */
    /*  maj_flt       number of major faults */
    /*  cmaj_flt      number of major faults with child's */
    /*  utime         user mode jiffies */
    /*  stime         kernel mode jiffies */
    /*  cutime        user mode jiffies with child's */
    /*  cstime        kernel mode jiffies with child's */
    /*  priority      priority level */
    /*  nice          nice level */
    /*  num_threads   number of threads */
    /*  it_real_value (obsolete, always 0) */
    /*  start_time    time the process started after system boot */
    /*  vsize         virtual memory size */
    /*  rss           resident set memory size */
    /*  rsslim        current limit in bytes on the rss */
    sscanf(line.data(),
           /* pid tcomm state ppid pgrp sid tty_nr tty_pgrp flags */
           /* 19162 (firefox) R 1 19162 19162 0 -1 4218880 */
           "%llu %*s %*s %*u %*u %*u %*u %*u %*u "
           /* min_flt cmin_flt maj_flt cmaj_flt utime stime cutime cstime priority nice */
           /* 340405 6560 3 0 7855 526 3 2 20 0 */
           "%*u %*u %*u %*u %llu %llu %llu %llu %*u %*u "
           /* num_threads it_real_value start_time vsize rss rsslim */
           /* 44 0 130881921 1347448832 99481 18446744073709551615 */
           "%llu %*u %*u %llu %llu",
           /* (firefox) more: 4194304 4515388 140732862948048 140732862941536 246430093205 0 0 4096 33572015 18446744073709551615 0 0 17 0 0 0 0 0 0 8721489 8726954 14176256 140732862948868 140732862948876 140732862948876 140732862951399 0 */
           &curr.check_pid,
           &curr.utime, &curr.stime, &curr.cutime, &curr.cstime,
           &curr.num_threads, &curr.vsize, &curr.rss);

    if (!pid_stat_prev_.check_pid) {
        pid_stat_prev_ = curr;
        return;
    }
    unsigned long long base = jiffies_delta_;

    sLOG << "pid_stat"
         << "utime" << perc(pid_stat_prev_.utime, curr.utime, base)
         << "stime" << perc(pid_stat_prev_.stime, curr.stime, base)
         << "cutime" << perc(pid_stat_prev_.cutime, curr.cutime, base)
         << "cstime" << perc(pid_stat_prev_.cstime, curr.cstime, base)
         << "num_threads" << curr.num_threads
         << "vsize" << curr.vsize
         << "rss" << curr.rss;

    out << "pr_user" << perc(pid_stat_prev_.utime, curr.utime, base)
        << "pr_sys" << perc(pid_stat_prev_.stime, curr.stime, base)
        << "pr_nthreads" << curr.num_threads
        << "pr_vsize" << curr.vsize
        << "pr_rss" << curr.rss;

    pid_stat_prev_ = curr;
}

LinuxProcStats::NetDevStat&
LinuxProcStats::find_net_dev(const std::string& if_name) {
    for (NetDevStat& i : net_dev_prev_) {
        if (i.if_name == if_name) return i;
    }
    net_dev_prev_.emplace_back();
    net_dev_prev_.back().if_name = if_name;
    return net_dev_prev_.back();
}

void LinuxProcStats::read_net_dev(const steady_clock::time_point& tp, JsonLine& out) {
    if (!file_net_dev_.is_open()) return;

    file_net_dev_.clear();
    file_net_dev_.seekg(0);
    if (!file_net_dev_.good()) return;

    unsigned long long elapsed
        = std::chrono::duration_cast<std::chrono::microseconds>(
        tp - tp_last_).count();

    NetDevStat sum;

    std::string line;
    while (std::getline(file_net_dev_, line)) {
        std::string::size_type colonpos = line.find(':');
        if (colonpos == std::string::npos) continue;

        std::string if_name = line.substr(0, colonpos);
        common::Trim(if_name);

        NetDevStat curr;
        sscanf(line.data() + colonpos + 1,
               "%llu %llu %*u %*u %*u %*u %*u %*u %llu %llu",
               &curr.rx_bytes, &curr.rx_pkts,
               &curr.tx_bytes, &curr.tx_pkts);

        sum.rx_bytes += curr.rx_bytes;
        sum.tx_bytes += curr.tx_bytes;
        sum.rx_pkts += curr.rx_pkts;
        sum.tx_pkts += curr.tx_pkts;

        curr.if_name = if_name;
        NetDevStat& prev = find_net_dev(if_name);

        if (prev.rx_bytes == 0) {
            // just store the first reading
            prev = curr;
            continue;
        }

        sLOG << "net" << if_name
             << "rx_bytes" << curr.rx_bytes - prev.rx_bytes
             << "tx_bytes" << curr.tx_bytes - prev.tx_bytes
             << "rx_pkts" << curr.rx_pkts - prev.rx_pkts
             << "tx_pkts" << curr.tx_pkts - prev.tx_pkts
             << "rx_speed"
             << static_cast<double>(curr.rx_bytes - prev.rx_bytes) / elapsed * 1e6
             << "tx_speed"
             << static_cast<double>(curr.tx_bytes - prev.tx_bytes) / elapsed * 1e6;

        prev = curr;
    }

    // summarizes interfaces
    {
        NetDevStat& prev = net_dev_sum_prev_;

        if (prev.rx_bytes == 0) {
            prev = sum;
            return;
        }

        sLOG << "net" << "(all)"
             << "rx_bytes" << sum.rx_bytes - prev.rx_bytes
             << "tx_bytes" << sum.tx_bytes - prev.tx_bytes
             << "rx_pkts" << sum.rx_pkts - prev.rx_pkts
             << "tx_pkts" << sum.tx_pkts - prev.tx_pkts
             << "rx_speed"
             << static_cast<double>(sum.rx_bytes - prev.rx_bytes) / elapsed * 1e6
             << "tx_speed"
             << static_cast<double>(sum.tx_bytes - prev.tx_bytes) / elapsed * 1e6;

        common_out(out);
        out << "net_rx_bytes" << sum.rx_bytes - prev.rx_bytes
            << "net_tx_bytes" << sum.tx_bytes - prev.tx_bytes
            << "net_rx_pkts" << sum.rx_pkts - prev.rx_pkts
            << "net_tx_pkts" << sum.tx_pkts - prev.tx_pkts
            << "net_rx_speed"
            << static_cast<double>(sum.rx_bytes - prev.rx_bytes) / elapsed * 1e6
            << "net_tx_speed"
            << static_cast<double>(sum.tx_bytes - prev.tx_bytes) / elapsed * 1e6;

        prev = sum;
    }
}

void LinuxProcStats::read_pid_io(const steady_clock::time_point& tp, JsonLine& out) {
    if (!file_pid_io_.is_open()) return;

    file_pid_io_.clear();
    file_pid_io_.seekg(0);
    if (!file_pid_io_.good()) return;

    PidIoStat curr;

    std::string line;
    while (std::getline(file_stat_, line)) {
        if (common::StartsWith(line, "read_bytes: ")) {
            sscanf(line.data() + 12, "%llu", &curr.read_bytes);
        }
        else if (common::StartsWith(line, "write_bytes: ")) {
            sscanf(line.data() + 13, "%llu", &curr.write_bytes);
        }
    }

    if (!pid_io_prev_.read_bytes) {
        // just store the first reading
        pid_io_prev_ = curr;
        return;
    }

    unsigned long long elapsed
        = std::chrono::duration_cast<std::chrono::microseconds>(
        tp - tp_last_).count();

    PidIoStat& prev = pid_io_prev_;

    sLOG << "pid_io"
         << "read_bytes" << curr.read_bytes - prev.read_bytes
         << "write_bytes" << curr.write_bytes - prev.write_bytes
         << "read_speed"
         << static_cast<double>(curr.read_bytes - prev.read_bytes) / elapsed * 1e6
         << "write_speed"
         << static_cast<double>(curr.write_bytes - prev.write_bytes) / elapsed * 1e6;

    common_out(out);
    out << "pr_io_read_bytes" << curr.read_bytes - prev.read_bytes
        << "pr_io_write_bytes" << curr.read_bytes - prev.read_bytes
        << "pr_io_read_speed"
        << static_cast<double>(curr.read_bytes - prev.read_bytes) / elapsed * 1e6
        << "pr_io_write_speed"
        << static_cast<double>(curr.write_bytes - prev.write_bytes) / elapsed * 1e6;

    prev = curr;
}

/******************************************************************************/
// JsonProfiler

class JsonProfiler
{
public:
    explicit JsonProfiler(JsonLogger& logger) : logger_(logger) {
        thread_ = std::thread([this]() { return worker(); });
    }

    //! non-copyable: delete copy-constructor
    JsonProfiler(const JsonProfiler&) = delete;
    //! non-copyable: delete assignment operator
    JsonProfiler& operator = (const JsonProfiler&) = delete;

    ~JsonProfiler() {
        if (thread_.get_id() == std::thread::id()) return;

        terminate_ = true;
        cv_.notify_one();
        thread_.join();
    }

private:
    //! reference to JsonLogger
    JsonLogger& logger_;

    //! thread for profiling (only run on top-level loggers)
    std::thread thread_;

    //! flag to terminate profiling thread
    std::atomic<bool> terminate_ { false };

    //! cv/mutex pair to signal thread to terminate
    std::timed_mutex mutex_;

    //! cv/mutex pair to signal thread to terminate
    std::condition_variable_any cv_;

    //! profiling worker function
    void worker();

    LinuxProcStats linux_proc_stats_ { logger_ };
};

void JsonProfiler::worker() {
    std::unique_lock<std::timed_mutex> lock(mutex_);

    steady_clock::time_point tm = steady_clock::now();

    while (!terminate_)
    {
        LOG0 << "hello "
             << std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        linux_proc_stats_.tick(tm);

        cv_.wait_until(mutex_, (tm += std::chrono::seconds(1)));
    }
}

/******************************************************************************/
// JsonLogger

JsonLogger::JsonLogger(const std::string& path) {
    if (!path.size()) return;

    os_.open(path.c_str());
    if (!os_.good()) {
        die("Could not open json log output: "
            << path << " : " << strerror(errno));
    }

    profiler_ = std::make_unique<JsonProfiler>(*this);
}

JsonLogger::JsonLogger(JsonLogger* super)
    : super_(super) { }

JsonLogger::~JsonLogger() { }

/******************************************************************************/
// JsonLine

JsonLine JsonLogger::line() {
    if (super_) {
        JsonLine out = super_->line();

        // append common key:value pairs
        if (common_.str_.size())
            out << common_;

        return out;
    }

    os_ << '{';

    JsonLine out(this, os_);

    // output timestamp in microseconds
    out << "ts"
        << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // append common key:value pairs
    if (common_.str_.size())
        out << common_;

    return out;
}

} // namespace common
} // namespace thrill

/******************************************************************************/
