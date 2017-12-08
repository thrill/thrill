/*******************************************************************************
 * thrill/net/mpi/group.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/mpi/dispatcher.hpp>
#include <thrill/net/mpi/group.hpp>

#include <mpi.h>

#include <limits>
#include <mutex>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! The Grand MPI Library Invocation Mutex (The GMLIM)
std::mutex g_mutex;

/******************************************************************************/
// mpi::Exception

std::string Exception::GetErrorString(int error_code) {
    char string[MPI_MAX_ERROR_STRING];
    int resultlen;
    MPI_Error_string(error_code, string, &resultlen);
    return std::string(string, resultlen);
}

/******************************************************************************/
//! mpi::Connection

void Connection::SyncSend(
    const void* data, size_t size, Flags /* flags */) {
    std::unique_lock<std::mutex> lock(g_mutex);

    LOG << "MPI_Send()"
        << " data=" << data
        << " size=" << size
        << " peer_=" << peer_
        << " group_tag_=" << group_tag_;

    assert(size <= std::numeric_limits<int>::max());

    int r = MPI_Send(const_cast<void*>(data), static_cast<int>(size), MPI_BYTE,
                     peer_, group_tag_, MPI_COMM_WORLD);

    if (r != MPI_SUCCESS)
        throw Exception("Error during SyncSend", r);

    tx_bytes_ += size;
}

void Connection::SyncRecv(void* out_data, size_t size) {
    std::unique_lock<std::mutex> lock(g_mutex);

    LOG << "MPI_Recv()"
        << " out_data=" << out_data
        << " size=" << size
        << " peer_=" << peer_
        << " group_tag_=" << group_tag_;

    assert(size <= std::numeric_limits<int>::max());

    MPI_Status status;
    int r = MPI_Recv(out_data, static_cast<int>(size), MPI_BYTE,
                     peer_, group_tag_, MPI_COMM_WORLD, &status);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Recv()", r);

    int count;
    r = MPI_Get_count(&status, MPI_BYTE, &count);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Get_count()", r);

    if (static_cast<size_t>(count) != size)
        throw Exception("Error during SyncRecv: message truncated?");

    rx_bytes_ += size;
}

void Connection::SyncSendRecv(const void* send_data, size_t send_size,
                              void* recv_data, size_t recv_size) {
    std::unique_lock<std::mutex> lock(g_mutex);

    LOG << "MPI_Sendrecv()"
        << " send_data=" << send_data
        << " send_size=" << send_size
        << " recv_data=" << recv_data
        << " recv_size=" << recv_size
        << " peer_=" << peer_
        << " group_tag_=" << group_tag_;

    assert(send_size <= std::numeric_limits<int>::max());
    assert(recv_size <= std::numeric_limits<int>::max());

    MPI_Status status;
    int r = MPI_Sendrecv(const_cast<void*>(send_data),
                         static_cast<int>(send_size), MPI_BYTE,
                         peer_, group_tag_,
                         recv_data, static_cast<int>(recv_size), MPI_BYTE,
                         peer_, group_tag_, MPI_COMM_WORLD, &status);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Sendrecv()", r);

    int count;
    r = MPI_Get_count(&status, MPI_BYTE, &count);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Get_count()", r);

    if (static_cast<size_t>(count) != recv_size)
        throw Exception("Error during SyncSendRecv: message truncated?");

    tx_bytes_ += send_size;
    rx_bytes_ += recv_size;
}

void Connection::SyncRecvSend(const void* send_data, size_t send_size,
                              void* recv_data, size_t recv_size) {
    SyncSendRecv(send_data, send_size, recv_data, recv_size);
}

/******************************************************************************/
// mpi::Group

size_t Group::num_parallel_async() const {
    return 16;
}

std::unique_ptr<net::Dispatcher> Group::ConstructDispatcher(
    mem::Manager& mem_manager) const {
    // construct mpi::Dispatcher
    return std::make_unique<Dispatcher>(mem_manager, num_hosts());
}

void Group::Barrier() {
    std::unique_lock<std::mutex> lock(g_mutex);
    int r = MPI_Barrier(MPI_COMM_WORLD);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Barrier()", r);
}

/******************************************************************************/
// mpi::Group Collective Overrides

/*[[[perl
  for my $e (
    ["int", "Int", "INT"], ["unsigned int", "UnsignedInt", "UNSIGNED"],
    ["long", "Long", "LONG"], ["unsigned long", "UnsignedLong", "UNSIGNED_LONG"],
    ["long long", "LongLong", "LONG_LONG"],
    ["unsigned long long", "UnsignedLongLong", "UNSIGNED_LONG_LONG"])
  {
    print "void Group::PrefixSumPlus$$e[1]($$e[0]& value) {\n";
    print "    std::unique_lock<std::mutex> lock(g_mutex);\n";
    print "    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_$$e[2], MPI_SUM, MPI_COMM_WORLD);\n";
    print "}\n";

    print "void Group::ExPrefixSumPlus$$e[1]($$e[0]& value) {\n";
    print "    std::unique_lock<std::mutex> lock(g_mutex);\n";
    print "    $$e[0] temp = value;\n";
    print "    MPI_Exscan(&temp, &value, 1, MPI_$$e[2], MPI_SUM, MPI_COMM_WORLD);\n";
    print "}\n";

    print "void Group::Broadcast$$e[1]($$e[0]& value, size_t origin) {\n";
    print "    std::unique_lock<std::mutex> lock(g_mutex);\n";
    print "    MPI_Bcast(&value, 1, MPI_$$e[2], origin, MPI_COMM_WORLD);\n";
    print "}\n";

    print "void Group::AllReducePlus$$e[1]($$e[0]& value) {\n";
    print "    std::unique_lock<std::mutex> lock(g_mutex);\n";
    print "    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_$$e[2], MPI_SUM, MPI_COMM_WORLD);\n";
    print "}\n";

    print "void Group::AllReduceMinimum$$e[1]($$e[0]& value) {\n";
    print "    std::unique_lock<std::mutex> lock(g_mutex);\n";
    print "    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_$$e[2], MPI_MIN, MPI_COMM_WORLD);\n";
    print "}\n";

    print "void Group::AllReduceMaximum$$e[1]($$e[0]& value) {\n";
    print "    std::unique_lock<std::mutex> lock(g_mutex);\n";
    print "    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_$$e[2], MPI_MAX, MPI_COMM_WORLD);\n";
    print "}\n";
  }
]]]*/
void Group::PrefixSumPlusInt(int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
}
void Group::ExPrefixSumPlusInt(int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    int temp = value;
    MPI_Exscan(&temp, &value, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
}
void Group::BroadcastInt(int& value, size_t origin) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Bcast(&value, 1, MPI_INT, origin, MPI_COMM_WORLD);
}
void Group::AllReducePlusInt(int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
}
void Group::AllReduceMinimumInt(int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
}
void Group::AllReduceMaximumInt(int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
}
void Group::PrefixSumPlusUnsignedInt(unsigned int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED, MPI_SUM, MPI_COMM_WORLD);
}
void Group::ExPrefixSumPlusUnsignedInt(unsigned int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    unsigned int temp = value;
    MPI_Exscan(&temp, &value, 1, MPI_UNSIGNED, MPI_SUM, MPI_COMM_WORLD);
}
void Group::BroadcastUnsignedInt(unsigned int& value, size_t origin) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Bcast(&value, 1, MPI_UNSIGNED, origin, MPI_COMM_WORLD);
}
void Group::AllReducePlusUnsignedInt(unsigned int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED, MPI_SUM, MPI_COMM_WORLD);
}
void Group::AllReduceMinimumUnsignedInt(unsigned int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED, MPI_MIN, MPI_COMM_WORLD);
}
void Group::AllReduceMaximumUnsignedInt(unsigned int& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED, MPI_MAX, MPI_COMM_WORLD);
}
void Group::PrefixSumPlusLong(long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::ExPrefixSumPlusLong(long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    long temp = value;
    MPI_Exscan(&temp, &value, 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::BroadcastLong(long& value, size_t origin) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Bcast(&value, 1, MPI_LONG, origin, MPI_COMM_WORLD);
}
void Group::AllReducePlusLong(long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::AllReduceMinimumLong(long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_LONG, MPI_MIN, MPI_COMM_WORLD);
}
void Group::AllReduceMaximumLong(long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_LONG, MPI_MAX, MPI_COMM_WORLD);
}
void Group::PrefixSumPlusUnsignedLong(unsigned long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::ExPrefixSumPlusUnsignedLong(unsigned long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    unsigned long temp = value;
    MPI_Exscan(&temp, &value, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::BroadcastUnsignedLong(unsigned long& value, size_t origin) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Bcast(&value, 1, MPI_UNSIGNED_LONG, origin, MPI_COMM_WORLD);
}
void Group::AllReducePlusUnsignedLong(unsigned long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::AllReduceMinimumUnsignedLong(unsigned long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG, MPI_MIN, MPI_COMM_WORLD);
}
void Group::AllReduceMaximumUnsignedLong(unsigned long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);
}
void Group::PrefixSumPlusLongLong(long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::ExPrefixSumPlusLongLong(long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    long long temp = value;
    MPI_Exscan(&temp, &value, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::BroadcastLongLong(long long& value, size_t origin) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Bcast(&value, 1, MPI_LONG_LONG, origin, MPI_COMM_WORLD);
}
void Group::AllReducePlusLongLong(long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::AllReduceMinimumLongLong(long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
}
void Group::AllReduceMaximumLongLong(long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
}
void Group::PrefixSumPlusUnsignedLongLong(unsigned long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Scan(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::ExPrefixSumPlusUnsignedLongLong(unsigned long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    unsigned long long temp = value;
    MPI_Exscan(&temp, &value, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::BroadcastUnsignedLongLong(unsigned long long& value, size_t origin) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Bcast(&value, 1, MPI_UNSIGNED_LONG_LONG, origin, MPI_COMM_WORLD);
}
void Group::AllReducePlusUnsignedLongLong(unsigned long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
}
void Group::AllReduceMinimumUnsignedLongLong(unsigned long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
}
void Group::AllReduceMaximumUnsignedLongLong(unsigned long long& value) {
    std::unique_lock<std::mutex> lock(g_mutex);
    MPI_Allreduce(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
}
// [[[end]]]

/******************************************************************************/
// mpi::Construct

//! atexit() method to deinitialize the MPI library.
static inline void Deinitialize() {
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Finalize();
}

//! run MPI_Init() if not already done (can be called multiple times).
static inline void Initialize() {

    int flag;
    int r = MPI_Initialized(&flag);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Initialized()", r);

    if (!flag) {
        // fake command line
        int argc = 1;
        const char* argv[] = { "thrill", nullptr };

        int provided;
        int r = MPI_Init_thread(&argc, reinterpret_cast<char***>(&argv),
                                MPI_THREAD_MULTIPLE, &provided);
        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Init_thread()", r);

        if (provided != MPI_THREAD_MULTIPLE)
            LOG1 << "WARNING: MPI_Init_thread() only provided= " << provided;

        // register atexit method
        atexit(&Deinitialize);
    }
}

/*!
 * Construct Group which connects to peers using MPI. As the MPI environment
 * already defines the connections, no hosts or parameters can be
 * given. Constructs group_count mpi::Group objects at once. Within each Group
 * this host has its MPI rank.
 *
 * Returns true if this Thrill host participates in the Group.
 */
bool Construct(size_t group_size,
               std::unique_ptr<Group>* groups, size_t group_count) {
    std::unique_lock<std::mutex> lock(g_mutex);

    Initialize();

    int my_rank;
    int r = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Comm_rank()", r);

    int num_mpi_hosts;
    r = MPI_Comm_size(MPI_COMM_WORLD, &num_mpi_hosts);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Comm_size()", r);

    if (group_size > static_cast<size_t>(num_mpi_hosts))
        throw Exception("mpi::Construct(): fewer MPI processes than hosts requested.");

    for (size_t i = 0; i < group_count; i++) {
        groups[i] = std::make_unique<Group>(my_rank, i, group_size);
    }

    return (static_cast<size_t>(my_rank) < group_size);
}

size_t NumMpiProcesses() {
    std::unique_lock<std::mutex> lock(g_mutex);

    Initialize();

    int num_mpi_hosts;
    int r = MPI_Comm_size(MPI_COMM_WORLD, &num_mpi_hosts);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Comm_size()", r);

    return static_cast<size_t>(num_mpi_hosts);
}

size_t MpiRank() {
    std::unique_lock<std::mutex> lock(g_mutex);

    Initialize();

    int mpi_rank;
    int r = MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Comm_rank()", r);

    return static_cast<size_t>(mpi_rank);
}

} // namespace mpi
} // namespace net
} // namespace thrill

/******************************************************************************/
