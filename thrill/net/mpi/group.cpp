/*******************************************************************************
 * thrill/net/mpi/group.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/net/mpi/dispatcher.hpp>
#include <thrill/net/mpi/group.hpp>

#include <mpi.h>

#include <mutex>
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

    int r = MPI_Send(const_cast<void*>(data), size, MPI_BYTE,
                     peer_, group_tag_, MPI_COMM_WORLD);

    if (r != MPI_SUCCESS)
        throw Exception("Error during SyncSend", r);
}

ssize_t Connection::SendOne(
    const void* data, size_t size, Flags /* flags */) {
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Request request;
    int r = MPI_Isend(const_cast<void*>(data), size, MPI_BYTE,
                      peer_, group_tag_, MPI_COMM_WORLD, &request);

    if (r != MPI_SUCCESS)
        throw Exception("Error during SyncOne", r);

    MPI_Request_free(&request);

    return size;
}

void Connection::SyncRecv(void* out_data, size_t size) {
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Status status;
    int r = MPI_Recv(out_data, size, MPI_BYTE,
                     peer_, group_tag_, MPI_COMM_WORLD, &status);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Recv()", r);

    int count;
    r = MPI_Get_count(&status, MPI_BYTE, &count);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Get_count()", r);

    if (static_cast<size_t>(count) != size)
        throw Exception("Error during SyncRecv: message truncated?");
}

/******************************************************************************/
// mpi::Group

mem::mm_unique_ptr<net::Dispatcher> Group::ConstructDispatcher(
    mem::Manager& mem_manager) const {
    // construct mpi::Dispatcher
    return mem::mm_unique_ptr<net::Dispatcher>(
        mem::mm_new<Dispatcher>(mem_manager,
                                mem_manager, group_tag_, num_hosts()),
        mem::Deleter<net::Dispatcher>(mem_manager));
}

void Group::Barrier() {
    int r = MPI_Barrier(MPI_COMM_WORLD);
    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Barrier()", r);
}

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
                                MPI_THREAD_SERIALIZED, &provided);
        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Init_thread()", r);

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

} // namespace mpi
} // namespace net
} // namespace thrill

/******************************************************************************/
