/*******************************************************************************
 * thrill/common/linux_proc_stats.cpp
 *
 * Profiling Task which reads CPU, network, I/O loads, and more from Linux's
 * /proc filesystem.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/linux_proc_stats.hpp>

#include <thrill/common/die.hpp>
#include <thrill/common/json_logger.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/profile_task.hpp>
#include <thrill/common/profile_thread.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/string_view.hpp>

#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#if __linux__

#include <dirent.h>
#include <unistd.h>

#endif

namespace thrill {
namespace common {

#if __linux__

using steady_clock = std::chrono::steady_clock;

class LinuxProcStats final : public ProfileTask
{
    static constexpr bool debug = false;

public:
    explicit LinuxProcStats(JsonLogger& logger) : logger_(logger) {

        sc_pagesize_ = sysconf(_SC_PAGESIZE);

        file_stat_.open("/proc/stat");
        file_net_dev_.open("/proc/net/dev");
        file_diskstats_.open("/proc/diskstats");
        file_meminfo_.open("/proc/meminfo");

        pid_t mypid = getpid();
        file_pid_stat_.open("/proc/" + std::to_string(mypid) + "/stat");
        file_pid_io_.open("/proc/" + std::to_string(mypid) + "/io");

        read_sys_block_devices();
    }

    //! read /sys/block to find block devices
    void read_sys_block_devices();

    //! calculate percentage of change relative to base.
    static double perc(unsigned long long prev, unsigned long long curr,
                       unsigned long long base) {
        if (curr < prev)
            return 0.0;
        else
            return static_cast<double>(curr - prev)
                   / static_cast<double>(base) * 100.0;
    }

    //! method to prepare JsonLine
    JsonLine& prepare_out(JsonLine& out);

    //! read /proc/stat
    void read_stat(JsonLine& out);

    //! read /proc/<pid>/stat
    void read_pid_stat(JsonLine& out);

    //! read /proc/net/dev
    void read_net_dev(const steady_clock::time_point& tp, JsonLine& out);

    //! read /proc/<pid>/io
    void read_pid_io(const steady_clock::time_point& tp, JsonLine& out);

    //! read /proc/diskstats
    void read_diskstats(JsonLine& out);

    //! read /proc/meminfo
    void read_meminfo(JsonLine& out);

    void RunTask(const steady_clock::time_point& tp) final {

        // JsonLine to construct
        JsonLine out = logger_.line();

        read_stat(out);
        read_pid_stat(out);
        read_net_dev(tp, out);
        read_pid_io(tp, out);
        read_diskstats(out);
        read_meminfo(out);

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
    //! open file handle to /proc/diskstats
    std::ifstream file_diskstats_;
    //! open file handle to /proc/meminfo
    std::ifstream file_meminfo_;

    //! last time point called
    steady_clock::time_point tp_last_;

    //! sysconf(_SC_PAGESIZE)
    size_t sc_pagesize_;

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

    struct DiskStats {
        std::string        dev_name;
        //! number of read operations issued to the device
        unsigned long long rd_ios = 0;
        //! number of read requests merged
        unsigned long long rd_merged = 0;
        //! number of sectors read (512b sectors)
        unsigned long long rd_sectors = 0;
        //! time of read requests in queue (ms)
        unsigned long long rd_time = 0;

        //! number of write operations issued to the device
        unsigned long long wr_ios = 0;
        //! number of write requests merged
        unsigned long long wr_merged = 0;
        //! number of sectors written (512b sectors)
        unsigned long long wr_sectors = 0;
        //! Time of write requests in queue (ms)
        unsigned long long wr_time = 0;

        //! number of I/Os in progress
        unsigned long long ios_progr = 0;
        //! number of time total (for this device) for I/O (ms)
        unsigned long long total_time = 0;
        //! number of time requests spent in queue (ms)
        unsigned long long rq_time = 0;
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

    //! find or create entry for net_dev
    NetDevStat& find_net_dev(const std::string& if_name);

    //! previous reading of pid's io file
    PidIoStat pid_io_prev_;

    //! find or create entry for net_dev
    DiskStats * find_diskstats(const char* dev_name);

    //! previous reading from diskstats
    std::vector<DiskStats> diskstats_prev_;

    //! helper method to parse size lines from /proc/meminfo
    static bool parse_meminfo(const char* str, size_t& size);
};

JsonLine& LinuxProcStats::prepare_out(JsonLine& out) {
    if (out.items() == 2) {
        out << "class" << "LinuxProcStats"
            << "event" << "profile";
    }
    return out;
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
            int ret = sscanf(
                line.data() + 5,
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

            die_unequal(10, ret);

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

            prepare_out(out)
                << "cpu_user" << perc(prev.user, curr.user, base)
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
            int ret = sscanf(
                line.data() + 3,
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

            die_unequal(11, ret);

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
        prepare_out(out)
            << "cores_user" << cores_user
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
    /*  rss           resident set memory size in SC_PAGESIZE units */
    /*  rsslim        current limit in bytes on the rss */
    int ret = sscanf(
        line.data(),
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

    die_unequal(8, ret);

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
         << "rss" << curr.rss * sc_pagesize_;

    prepare_out(out)
        << "pr_user" << perc(pid_stat_prev_.utime, curr.utime, base)
        << "pr_sys" << perc(pid_stat_prev_.stime, curr.stime, base)
        << "pr_nthreads" << curr.num_threads
        << "pr_vsize" << curr.vsize
        << "pr_rss" << curr.rss * sc_pagesize_;

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

void LinuxProcStats::read_net_dev(
    const steady_clock::time_point& tp, JsonLine& out) {
    if (!file_net_dev_.is_open()) return;

    file_net_dev_.clear();
    file_net_dev_.seekg(0);
    if (!file_net_dev_.good()) return;

    double elapsed = static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            tp - tp_last_).count()) / 1e6;

    NetDevStat sum;
    bool sum_output = false;

    std::string line;
    while (std::getline(file_net_dev_, line)) {
        std::string::size_type colonpos = line.find(':');
        if (colonpos == std::string::npos) continue;

        std::string if_name = line.substr(0, colonpos);
        common::Trim(if_name);

        NetDevStat curr;
        int ret = sscanf(line.data() + colonpos + 1,
                         "%llu %llu %*u %*u %*u %*u %*u %*u %llu %llu",
                         &curr.rx_bytes, &curr.rx_pkts,
                         &curr.tx_bytes, &curr.tx_pkts);
        die_unequal(4, ret);

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
             << static_cast<double>(curr.rx_bytes - prev.rx_bytes) / elapsed
             << "tx_speed"
             << static_cast<double>(curr.tx_bytes - prev.tx_bytes) / elapsed;

        sum.rx_bytes += curr.rx_bytes - prev.rx_bytes;
        sum.tx_bytes += curr.tx_bytes - prev.tx_bytes;
        sum.rx_pkts += curr.rx_pkts - prev.rx_pkts;
        sum.tx_pkts += curr.tx_pkts - prev.tx_pkts;
        sum_output = true;

        prev = curr;
    }

    // summarizes interfaces
    if (sum_output)
    {
        sLOG << "net" << "(all)"
             << "rx_bytes" << sum.rx_bytes
             << "tx_bytes" << sum.tx_bytes
             << "rx_pkts" << sum.rx_pkts
             << "tx_pkts" << sum.tx_pkts
             << "rx_speed" << static_cast<double>(sum.rx_bytes) / elapsed
             << "tx_speed" << static_cast<double>(sum.tx_bytes) / elapsed;

        prepare_out(out)
            << "net_rx_bytes" << sum.rx_bytes
            << "net_tx_bytes" << sum.tx_bytes
            << "net_rx_pkts" << sum.rx_pkts
            << "net_tx_pkts" << sum.tx_pkts
            << "net_rx_speed" << static_cast<double>(sum.rx_bytes) / elapsed
            << "net_tx_speed" << static_cast<double>(sum.tx_bytes) / elapsed;
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
            int ret = sscanf(line.data() + 12, "%llu", &curr.read_bytes);
            die_unequal(1, ret);
        }
        else if (common::StartsWith(line, "write_bytes: ")) {
            int ret = sscanf(line.data() + 13, "%llu", &curr.write_bytes);
            die_unequal(1, ret);
        }
    }

    if (!pid_io_prev_.read_bytes) {
        // just store the first reading
        pid_io_prev_ = curr;
        return;
    }

    double elapsed = static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            tp - tp_last_).count()) / 1e6;

    PidIoStat& prev = pid_io_prev_;

    sLOG << "pid_io"
         << "read_bytes" << curr.read_bytes - prev.read_bytes
         << "write_bytes" << curr.write_bytes - prev.write_bytes
         << "read_speed"
         << static_cast<double>(curr.read_bytes - prev.read_bytes) / elapsed
         << "write_speed"
         << static_cast<double>(curr.write_bytes - prev.write_bytes) / elapsed;

    prepare_out(out)
        << "pr_io_read_bytes" << curr.read_bytes - prev.read_bytes
        << "pr_io_write_bytes" << curr.read_bytes - prev.read_bytes
        << "pr_io_read_speed"
        << static_cast<double>(curr.read_bytes - prev.read_bytes) / elapsed
        << "pr_io_write_speed"
        << static_cast<double>(curr.write_bytes - prev.write_bytes) / elapsed;

    prev = curr;
}

LinuxProcStats::DiskStats*
LinuxProcStats::find_diskstats(const char* dev_name) {
    for (DiskStats& i : diskstats_prev_) {
        if (strcmp(i.dev_name.c_str(), dev_name) == 0) return &i;
    }
    return nullptr;
}

void LinuxProcStats::read_sys_block_devices() {
    DIR* dirp = opendir("/sys/block");
    if (!dirp) return;

    struct dirent de_prev, * de;
    while (readdir_r(dirp, &de_prev, &de) == 0 && de != nullptr) {
        if (de->d_name[0] == '.') continue;
        // push into diskstats vector
        diskstats_prev_.emplace_back();
        diskstats_prev_.back().dev_name = de->d_name;
    }
    closedir(dirp);
}

void LinuxProcStats::read_diskstats(JsonLine& out) {
    if (!file_diskstats_.is_open()) return;

    file_diskstats_.clear();
    file_diskstats_.seekg(0);
    if (!file_diskstats_.good()) return;

    DiskStats sum;
    bool sum_valid = false;
    JsonLine disks = prepare_out(out).sub("disks");

    std::string line;
    while (std::getline(file_diskstats_, line)) {

        char dev_name[32];
        DiskStats curr;
        int ret = sscanf(
            line.data(),
            "%*u %*u %31s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
            dev_name,
            &curr.rd_ios, &curr.rd_merged, &curr.rd_sectors, &curr.rd_time,
            &curr.wr_ios, &curr.wr_merged, &curr.wr_sectors, &curr.wr_time,
            &curr.ios_progr, &curr.total_time, &curr.rq_time);
        die_unequal(12, ret);

        DiskStats* ptr_prev = find_diskstats(dev_name);
        if (!ptr_prev) continue;

        DiskStats& prev = *ptr_prev;
        curr.dev_name = dev_name;

        if (!prev.rd_ios && !prev.wr_ios && !prev.ios_progr) {
            // just store the first reading, also: skipped entries that remain
            // zero.
            prev = curr;
            continue;
        }

        sLOG << "diskstats"
             << "dev" << dev_name
             << "rd_ios" << curr.rd_ios - prev.rd_ios
             << "rd_merged" << curr.rd_merged - prev.rd_merged
             << "rd_bytes" << (curr.rd_sectors - prev.rd_sectors) * 512
             << "rd_time" << double(curr.rd_time - prev.rd_time) / 1e3
             << "wr_ios" << curr.wr_ios - prev.wr_ios
             << "wr_merged" << curr.wr_merged - prev.wr_merged
             << "wr_bytes" << (curr.wr_sectors - prev.wr_sectors) * 512
             << "wr_time" << double(curr.wr_time - prev.wr_time) / 1e3
             << "ios_progr" << curr.ios_progr
             << "total_time" << double(curr.total_time - prev.total_time) / 1e3
             << "rq_time" << double(curr.rq_time - prev.rq_time) / 1e3;

        disks.sub(dev_name)
            << "rd_ios" << curr.rd_ios - prev.rd_ios
            << "rd_merged" << curr.rd_merged - prev.rd_merged
            << "rd_bytes" << (curr.rd_sectors - prev.rd_sectors) * 512
            << "rd_time" << double(curr.rd_time - prev.rd_time) / 1e3
            << "wr_ios" << curr.wr_ios - prev.wr_ios
            << "wr_merged" << curr.wr_merged - prev.wr_merged
            << "wr_bytes" << (curr.wr_sectors - prev.wr_sectors) * 512
            << "wr_time" << double(curr.wr_time - prev.wr_time) / 1e3
            << "ios_progr" << curr.ios_progr
            << "total_time" << double(curr.total_time - prev.total_time) / 1e3
            << "rq_time" << double(curr.rq_time - prev.rq_time) / 1e3;

        sum.rd_ios += curr.rd_ios - prev.rd_ios;
        sum.rd_merged += curr.rd_merged - prev.rd_merged;
        sum.rd_sectors += curr.rd_sectors - prev.rd_sectors;
        sum.rd_time += curr.rd_time - prev.rd_time;
        sum.wr_ios += curr.wr_ios - prev.wr_ios;
        sum.wr_merged += curr.wr_merged - prev.wr_merged;
        sum.wr_sectors += curr.wr_sectors - prev.wr_sectors;
        sum.wr_time += curr.wr_time - prev.wr_time;
        sum.ios_progr += curr.ios_progr;
        sum.total_time += curr.total_time - prev.total_time;
        sum.rq_time += curr.rq_time - prev.rq_time;
        sum_valid = true;

        prev = curr;
    }

    disks.Close();

    if (sum_valid) {
        prepare_out(out).sub("diskstats")
            << "rd_ios" << sum.rd_ios
            << "rd_merged" << sum.rd_merged
            << "rd_bytes" << sum.rd_sectors * 512
            << "rd_time" << double(sum.rd_time) / 1e3
            << "wr_ios" << sum.wr_ios
            << "wr_merged" << sum.wr_merged
            << "wr_bytes" << sum.wr_sectors * 512
            << "wr_time" << double(sum.wr_time) / 1e3
            << "ios_progr" << sum.ios_progr
            << "total_time" << double(sum.total_time) / 1e3
            << "rq_time" << double(sum.rq_time) / 1e3;
    }
}

//! helper method to parse size lines from /proc/meminfo
bool LinuxProcStats::parse_meminfo(const char* str, size_t& size) {
    char* endptr;
    size = strtoul(str, &endptr, 10);
    // parse failed, no number
    if (!endptr) return false;

    // skip over spaces
    while (*endptr == ' ') ++endptr;

    // multiply with 2^power
    if (*endptr == 'k' || *endptr == 'K')
        size *= 1024, ++endptr;
    else if (*endptr == 'm' || *endptr == 'M')
        size *= 1024 * 1024, ++endptr;
    else if (*endptr == 'g' || *endptr == 'G')
        size *= 1024 * 1024 * 1024llu, ++endptr;

    // byte indicator
    if (*endptr == 'b' || *endptr == 'B') {
        ++endptr;
    }

    // skip over spaces
    while (*endptr == ' ') ++endptr;

    return (*endptr == 0);
}

void LinuxProcStats::read_meminfo(JsonLine& out) {
    if (!file_meminfo_.is_open()) return;

    file_meminfo_.clear();
    file_meminfo_.seekg(0);
    if (!file_meminfo_.good()) return;

    JsonLine mem = prepare_out(out).sub("meminfo");

    size_t swap_total = 0, swap_free = 0;

    std::string line;
    while (std::getline(file_meminfo_, line)) {
        std::string::size_type colonpos = line.find(':');
        if (colonpos == std::string::npos) continue;

        common::StringView key(line.begin(), line.begin() + colonpos);

        size_t size;

        if (key == "MemTotal") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "total" << size;
        }
        else if (key == "MemFree") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "free" << size;
        }
        else if (key == "MemAvailable") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "available" << size;
        }
        else if (key == "Buffers") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "buffers" << size;
        }
        else if (key == "Cached") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "cached" << size;
        }
        else if (key == "Mapped") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "mapped" << size;
        }
        else if (key == "Shmem") {
            if (parse_meminfo(line.data() + colonpos + 1, size))
                mem << "shmem" << size;
        }
        else if (key == "SwapTotal") {
            if (parse_meminfo(line.data() + colonpos + 1, size)) {
                mem << "swap_total" << size;
                swap_total = size;
                if (swap_total && swap_free) {
                    mem << "swap_used" << swap_total - swap_free;
                    swap_total = swap_free = 0;
                }
            }
        }
        else if (key == "SwapFree") {
            if (parse_meminfo(line.data() + colonpos + 1, size)) {
                mem << "swap_free" << size;
                swap_free = size;
                if (swap_total && swap_free) {
                    mem << "swap_used" << swap_total - swap_free;
                    swap_total = swap_free = 0;
                }
            }
        }
    }
}

void StartLinuxProcStatsProfiler(ProfileThread& sched, JsonLogger& logger) {
    sched.Add(std::chrono::seconds(1),
              new LinuxProcStats(logger), /* own_task */ true);
}

#else

void StartLinuxProcStatsProfiler(ProfileThread&, JsonLogger&)
{ }

#endif  // __linux__

} // namespace common
} // namespace thrill

/******************************************************************************/
