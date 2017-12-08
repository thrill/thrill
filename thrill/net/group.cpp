/*******************************************************************************
 * thrill/net/group.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/collective.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/manager.hpp>
#include <thrill/net/mock/group.hpp>

#if THRILL_HAVE_NET_TCP
#include <thrill/net/tcp/group.hpp>
#endif

#include <functional>
#include <utility>
#include <vector>

namespace thrill {
namespace net {

void RunLoopbackGroupTest(
    size_t num_hosts,
    const std::function<void(Group*)>& thread_function) {
#if THRILL_HAVE_NET_TCP
    // construct local tcp network mesh and run threads
    ExecuteGroupThreads(
        tcp::Group::ConstructLoopbackMesh(num_hosts),
        thread_function);
#else
    // construct mock network mesh and run threads
    ExecuteGroupThreads(
        mock::Group::ConstructLoopbackMesh(num_hosts),
        thread_function);
#endif
}

/******************************************************************************/
// Manager

Manager::Manager(std::array<GroupPtr, kGroupCount>&& groups,
                 common::JsonLogger& logger) noexcept
    : groups_(std::move(groups)), logger_(logger) { }

Manager::Manager(std::vector<GroupPtr>&& groups,
                 common::JsonLogger& logger) noexcept
    : logger_(logger) {
    assert(groups.size() == kGroupCount);
    std::move(groups.begin(), groups.end(), groups_.begin());
}

void Manager::Close() {
    for (size_t i = 0; i < kGroupCount; i++) {
        groups_[i]->Close();
    }
}

std::pair<size_t, size_t> Manager::Traffic() const {
    size_t total_tx = 0, total_rx = 0;

    for (size_t g = 0; g < kGroupCount; ++g) {
        Group& group = *groups_[g];

        for (size_t h = 0; h < group.num_hosts(); ++h) {
            if (h == group.my_host_rank()) continue;

            total_tx += group.connection(h).tx_bytes_;
            total_rx += group.connection(h).rx_bytes_;
        }
    }

    return std::make_pair(total_tx, total_rx);
}

void Manager::RunTask(const std::chrono::steady_clock::time_point& tp) {

    common::JsonLine line = logger_.line();
    line << "class" << "NetManager"
         << "event" << "profile";

    double elapsed = static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            tp - tp_last_).count()) / 1e6;

    size_t total_tx = 0, total_rx = 0;
    size_t prev_total_tx = 0, prev_total_rx = 0;

    for (size_t g = 0; g < kGroupCount; ++g) {
        Group& group = *groups_[g];

        size_t group_tx = 0, group_rx = 0;
        size_t prev_group_tx = 0, prev_group_rx = 0;
        std::vector<size_t> tx_per_host(group.num_hosts());
        std::vector<size_t> rx_per_host(group.num_hosts());

        for (size_t h = 0; h < group.num_hosts(); ++h) {
            if (h == group.my_host_rank()) continue;

            Connection& conn = group.connection(h);

            size_t tx = conn.tx_bytes_.load(std::memory_order_relaxed);
            size_t rx = conn.rx_bytes_.load(std::memory_order_relaxed);
            size_t prev_tx = conn.prev_tx_bytes_;
            size_t prev_rx = conn.prev_rx_bytes_;

            group_tx += tx;
            prev_group_tx += prev_tx;
            group.connection(h).prev_tx_bytes_ = tx;

            group_rx += rx;
            prev_group_rx += prev_rx;
            group.connection(h).prev_rx_bytes_ = rx;

            tx_per_host[h] = tx;
            rx_per_host[h] = rx;
        }

        line.sub(g == 0 ? "flow" : g == 1 ? "data" : "???")
            << "tx_bytes" << group_tx
            << "rx_bytes" << group_rx
            << "tx_speed"
            << static_cast<double>(group_tx - prev_group_tx) / elapsed
            << "rx_speed"
            << static_cast<double>(group_rx - prev_group_rx) / elapsed
            << "tx_per_host" << tx_per_host
            << "rx_per_host" << rx_per_host;

        total_tx += group_tx;
        total_rx += group_rx;
        prev_total_tx += prev_group_tx;
        prev_total_rx += prev_group_rx;

        tp_last_ = tp;
    }

    // write out totals
    line
        << "tx_bytes" << total_tx
        << "rx_bytes" << total_rx
        << "tx_speed"
        << static_cast<double>(total_tx - prev_total_tx) / elapsed
        << "rx_speed"
        << static_cast<double>(total_rx - prev_total_rx) / elapsed;
}

/******************************************************************************/
// Group

size_t Group::num_parallel_async() const {
    return 0;
}

/*[[[perl
  for my $e (
    ["int", "Int"], ["unsigned int", "UnsignedInt"],
    ["long", "Long"], ["unsigned long", "UnsignedLong"],
    ["long long", "LongLong"], ["unsigned long long", "UnsignedLongLong"])
  {
    print "void Group::PrefixSumPlus$$e[1]($$e[0]& value) {\n";
    print "    return PrefixSumSelect(value, std::plus<$$e[0]>(), true);\n";
    print "}\n";

    print "void Group::ExPrefixSumPlus$$e[1]($$e[0]& value) {\n";
    print "    return PrefixSumSelect(value, std::plus<$$e[0]>(), false);\n";
    print "}\n";

    print "void Group::Broadcast$$e[1]($$e[0]& value, size_t origin) {\n";
    print "    return BroadcastSelect(value, origin);\n";
    print "}\n";

    print "void Group::AllReducePlus$$e[1]($$e[0]& value) {\n";
    print "    return AllReduceSelect(value, std::plus<$$e[0]>());\n";
    print "}\n";

    print "void Group::AllReduceMinimum$$e[1]($$e[0]& value) {\n";
    print "    return AllReduceSelect(value, common::minimum<$$e[0]>());\n";
    print "}\n";

    print "void Group::AllReduceMaximum$$e[1]($$e[0]& value) {\n";
    print "    return AllReduceSelect(value, common::maximum<$$e[0]>());\n";
    print "}\n";
  }
]]]*/
void Group::PrefixSumPlusInt(int& value) {
    return PrefixSumSelect(value, std::plus<int>(), true);
}
void Group::ExPrefixSumPlusInt(int& value) {
    return PrefixSumSelect(value, std::plus<int>(), false);
}
void Group::BroadcastInt(int& value, size_t origin) {
    return BroadcastSelect(value, origin);
}
void Group::AllReducePlusInt(int& value) {
    return AllReduceSelect(value, std::plus<int>());
}
void Group::AllReduceMinimumInt(int& value) {
    return AllReduceSelect(value, common::minimum<int>());
}
void Group::AllReduceMaximumInt(int& value) {
    return AllReduceSelect(value, common::maximum<int>());
}
void Group::PrefixSumPlusUnsignedInt(unsigned int& value) {
    return PrefixSumSelect(value, std::plus<unsigned int>(), true);
}
void Group::ExPrefixSumPlusUnsignedInt(unsigned int& value) {
    return PrefixSumSelect(value, std::plus<unsigned int>(), false);
}
void Group::BroadcastUnsignedInt(unsigned int& value, size_t origin) {
    return BroadcastSelect(value, origin);
}
void Group::AllReducePlusUnsignedInt(unsigned int& value) {
    return AllReduceSelect(value, std::plus<unsigned int>());
}
void Group::AllReduceMinimumUnsignedInt(unsigned int& value) {
    return AllReduceSelect(value, common::minimum<unsigned int>());
}
void Group::AllReduceMaximumUnsignedInt(unsigned int& value) {
    return AllReduceSelect(value, common::maximum<unsigned int>());
}
void Group::PrefixSumPlusLong(long& value) {
    return PrefixSumSelect(value, std::plus<long>(), true);
}
void Group::ExPrefixSumPlusLong(long& value) {
    return PrefixSumSelect(value, std::plus<long>(), false);
}
void Group::BroadcastLong(long& value, size_t origin) {
    return BroadcastSelect(value, origin);
}
void Group::AllReducePlusLong(long& value) {
    return AllReduceSelect(value, std::plus<long>());
}
void Group::AllReduceMinimumLong(long& value) {
    return AllReduceSelect(value, common::minimum<long>());
}
void Group::AllReduceMaximumLong(long& value) {
    return AllReduceSelect(value, common::maximum<long>());
}
void Group::PrefixSumPlusUnsignedLong(unsigned long& value) {
    return PrefixSumSelect(value, std::plus<unsigned long>(), true);
}
void Group::ExPrefixSumPlusUnsignedLong(unsigned long& value) {
    return PrefixSumSelect(value, std::plus<unsigned long>(), false);
}
void Group::BroadcastUnsignedLong(unsigned long& value, size_t origin) {
    return BroadcastSelect(value, origin);
}
void Group::AllReducePlusUnsignedLong(unsigned long& value) {
    return AllReduceSelect(value, std::plus<unsigned long>());
}
void Group::AllReduceMinimumUnsignedLong(unsigned long& value) {
    return AllReduceSelect(value, common::minimum<unsigned long>());
}
void Group::AllReduceMaximumUnsignedLong(unsigned long& value) {
    return AllReduceSelect(value, common::maximum<unsigned long>());
}
void Group::PrefixSumPlusLongLong(long long& value) {
    return PrefixSumSelect(value, std::plus<long long>(), true);
}
void Group::ExPrefixSumPlusLongLong(long long& value) {
    return PrefixSumSelect(value, std::plus<long long>(), false);
}
void Group::BroadcastLongLong(long long& value, size_t origin) {
    return BroadcastSelect(value, origin);
}
void Group::AllReducePlusLongLong(long long& value) {
    return AllReduceSelect(value, std::plus<long long>());
}
void Group::AllReduceMinimumLongLong(long long& value) {
    return AllReduceSelect(value, common::minimum<long long>());
}
void Group::AllReduceMaximumLongLong(long long& value) {
    return AllReduceSelect(value, common::maximum<long long>());
}
void Group::PrefixSumPlusUnsignedLongLong(unsigned long long& value) {
    return PrefixSumSelect(value, std::plus<unsigned long long>(), true);
}
void Group::ExPrefixSumPlusUnsignedLongLong(unsigned long long& value) {
    return PrefixSumSelect(value, std::plus<unsigned long long>(), false);
}
void Group::BroadcastUnsignedLongLong(unsigned long long& value, size_t origin) {
    return BroadcastSelect(value, origin);
}
void Group::AllReducePlusUnsignedLongLong(unsigned long long& value) {
    return AllReduceSelect(value, std::plus<unsigned long long>());
}
void Group::AllReduceMinimumUnsignedLongLong(unsigned long long& value) {
    return AllReduceSelect(value, common::minimum<unsigned long long>());
}
void Group::AllReduceMaximumUnsignedLongLong(unsigned long long& value) {
    return AllReduceSelect(value, common::maximum<unsigned long long>());
}
// [[[end]]]

} // namespace net
} // namespace thrill

/******************************************************************************/
