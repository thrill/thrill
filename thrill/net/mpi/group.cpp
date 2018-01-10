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

Exception::Exception(const std::string& what, int error_code)
    : net::Exception(what + ": [" + std::to_string(error_code) + "] "
                     + GetErrorString(error_code))
{ }

std::string Exception::GetErrorString(int error_code) {
    char string[MPI_MAX_ERROR_STRING];
    int resultlen;
    MPI_Error_string(error_code, string, &resultlen);
    return std::string(string, resultlen);
}

/******************************************************************************/
//! mpi::Connection

std::string Connection::ToString() const {
    return "peer: " + std::to_string(peer_);
}

std::ostream& Connection::OutputOstream(std::ostream& os) const {
    return os << "[mpi::Connection"
              << " group_tag_=" << group_->group_tag()
              << " peer_=" << peer_
              << "]";
}

void Connection::SyncSend(const void* data, size_t size, Flags /* flags */) {

    LOG << "SyncSend()"
        << " data=" << data
        << " size=" << size
        << " peer_=" << peer_
        << " group_tag_=" << group_->group_tag();

    assert(size <= std::numeric_limits<int>::max());

    bool done = false;
    group_->dispatcher().RunInThread(
        [=, &done](net::Dispatcher& dispatcher) {

            auto& disp = static_cast<mpi::Dispatcher&>(dispatcher);

            MPI_Request request =
                disp.ISend(*this, /* seq */ 0, data, size);

            disp.AddAsyncRequest(
                request, [&done](MPI_Status&) { done = true; });
        });

    while (!done)
        std::this_thread::yield();

    tx_bytes_ += size;
}

void Connection::SyncRecv(void* out_data, size_t size) {

    LOG << "SyncRecv()"
        << " out_data=" << out_data
        << " size=" << size
        << " peer_=" << peer_
        << " group_tag_=" << group_->group_tag();

    assert(size <= std::numeric_limits<int>::max());

    bool done = false;
    group_->dispatcher().RunInThread(
        [=, &done](net::Dispatcher& dispatcher) {

            auto& disp = static_cast<mpi::Dispatcher&>(dispatcher);

            MPI_Request request =
                disp.IRecv(*this, /* seq */ 0, out_data, size);

            disp.AddAsyncRequest(
                request, [&done, size](MPI_Status& status) {
                    int count;
                    int r = MPI_Get_count(&status, MPI_BYTE, &count);
                    if (r != MPI_SUCCESS)
                        throw Exception("Error during MPI_Get_count()", r);

                    if (static_cast<size_t>(count) != size)
                        throw Exception("Error during SyncRecv(): message truncated?");

                    done = true;
                });
        });

    while (!done)
        std::this_thread::yield();

    rx_bytes_ += size;
}

void Connection::SyncSendRecv(const void* send_data, size_t send_size,
                              void* recv_data, size_t recv_size) {

    LOG << "SyncSendRecv()"
        << " send_data=" << send_data
        << " send_size=" << send_size
        << " recv_data=" << recv_data
        << " recv_size=" << recv_size
        << " peer_=" << peer_
        << " group_tag_=" << group_->group_tag();

    assert(send_size <= std::numeric_limits<int>::max());
    assert(recv_size <= std::numeric_limits<int>::max());

    unsigned done = 0;
    group_->dispatcher().RunInThread(
        [=, &done](net::Dispatcher& dispatcher) {
            auto& disp = static_cast<mpi::Dispatcher&>(dispatcher);

            MPI_Request send_request =
                disp.ISend(*this, /* seq */ 0, send_data, send_size);

            MPI_Request recv_request =
                disp.IRecv(*this, /* seq */ 0, recv_data, recv_size);

            disp.AddAsyncRequest(
                send_request, [&done](MPI_Status&) { ++done; });
            disp.AddAsyncRequest(
                recv_request, [&done, recv_size](MPI_Status& status) {
                    int count;
                    int r = MPI_Get_count(&status, MPI_BYTE, &count);
                    if (r != MPI_SUCCESS)
                        throw Exception("Error during MPI_Get_count()", r);

                    if (static_cast<size_t>(count) != recv_size)
                        throw Exception("Error during SyncSendRecv(): message truncated?");

                    ++done;
                });
        });

    while (done != 2)
        std::this_thread::yield();

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

std::unique_ptr<net::Dispatcher> Group::ConstructDispatcher() const {
    // construct mpi::Dispatcher
    return std::make_unique<Dispatcher>(num_hosts());
}

void Group::Barrier() {

    bool done = false;
    dispatcher_.RunInThread(
        [=, &done](net::Dispatcher& dispatcher) {
            std::unique_lock<std::mutex> lock(g_mutex);

            MPI_Request request;
            int r = MPI_Ibarrier(MPI_COMM_WORLD, &request);
            if (r != MPI_SUCCESS)
                throw Exception("Error during MPI_Barrier()", r);

            lock.unlock();

            auto& disp = static_cast<mpi::Dispatcher&>(dispatcher);
            disp.AddAsyncRequest(
                request, [&done](MPI_Status&) { done = true; });
        });

    while (!done)
        std::this_thread::yield();
}

template <typename MpiCall>
void Group::WaitForRequest(MpiCall call) {

    bool done = false;
    dispatcher_.RunInThread(
        [=, &done](net::Dispatcher& dispatcher) {
            std::unique_lock<std::mutex> lock(g_mutex);

            MPI_Request request;
            int r = call(request);

            if (r != MPI_SUCCESS)
                throw Exception("Error during WaitForRequest", r);

            lock.unlock();

            auto& disp = static_cast<mpi::Dispatcher&>(dispatcher);
            disp.AddAsyncRequest(
                request, [&done](MPI_Status&) { done = true; });
        });

    while (!done)
        std::this_thread::yield();
}

/******************************************************************************/
// mpi::Group Collective Overrides

/*[[[perl
  for my $e (
    ["int", "Int", "INT"],
    ["unsigned int", "UnsignedInt", "UNSIGNED"],
    ["long", "Long", "LONG"],
    ["unsigned long", "UnsignedLong", "UNSIGNED_LONG"],
    ["long long", "LongLong", "LONG_LONG"],
    ["unsigned long long", "UnsignedLongLong", "UNSIGNED_LONG_LONG"])
  {
    print "void Group::PrefixSumPlus$$e[1]($$e[0]& value, const $$e[0]& initial) {\n";
    print "    LOG << \"Group::PrefixSumPlus($$e[0]);\";\n";
    print "    WaitForRequest(\n";
    print "        [&](MPI_Request& request) {\n";
    print "            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,\n";
    print "                             MPI_SUM, MPI_COMM_WORLD, &request);\n";
    print "        });\n";
    print "    value += initial;\n";
    print "}\n";

    print "void Group::ExPrefixSumPlus$$e[1]($$e[0]& value, const $$e[0]& initial) {\n";
    print "    LOG << \"Group::ExPrefixSumPlus($$e[0]);\";\n";
    print "    WaitForRequest(\n";
    print "        [&](MPI_Request& request) {\n";
    print "            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_$$e[2],\n";
    print "                               MPI_SUM, MPI_COMM_WORLD, &request);\n";
    print "        });\n";
    print "    value = (my_rank_ == 0 ? initial : value + initial);\n";
    print "}\n";

    print "void Group::Broadcast$$e[1]($$e[0]& value, size_t origin) {\n";
    print "    LOG << \"Group::Broadcast($$e[0]);\";\n";
    print "    WaitForRequest(\n";
    print "        [&](MPI_Request& request) {\n";
    print "            return MPI_Ibcast(&value, 1, MPI_$$e[2], origin,\n";
    print "                              MPI_COMM_WORLD, &request);\n";
    print "        });\n";
    print "}\n";

    print "void Group::AllReducePlus$$e[1]($$e[0]& value) {\n";
    print "    LOG << \"Group::AllReducePlus($$e[0]);\";\n";
    print "    WaitForRequest(\n";
    print "        [&](MPI_Request& request) {\n";
    print "            return MPI_Iallreduce(\n";
    print "                MPI_IN_PLACE, &value, 1, MPI_$$e[2],\n";
    print "                MPI_SUM, MPI_COMM_WORLD, &request);\n";
    print "        });\n";
    print "}\n";

    print "void Group::AllReduceMinimum$$e[1]($$e[0]& value) {\n";
    print "    LOG << \"Group::AllReduceMinimum($$e[0]);\";\n";
    print "    WaitForRequest(\n";
    print "        [&](MPI_Request& request) {\n";
    print "            return MPI_Iallreduce(\n";
    print "                MPI_IN_PLACE, &value, 1, MPI_$$e[2],\n";
    print "                MPI_MIN, MPI_COMM_WORLD, &request);\n";
    print "        });\n";
    print "}\n";

    print "void Group::AllReduceMaximum$$e[1]($$e[0]& value) {\n";
    print "    LOG << \"Group::AllReduceMaximum($$e[0]);\";\n";
    print "    WaitForRequest(\n";
    print "        [&](MPI_Request& request) {\n";
    print "            return MPI_Iallreduce(\n";
    print "                MPI_IN_PLACE, &value, 1, MPI_$$e[2],\n";
    print "                 MPI_MAX, MPI_COMM_WORLD, &request);\n";
    print "        });\n";
    print "}\n";
  }
]]]*/
void Group::PrefixSumPlusInt(int& value, const int& initial) {
    LOG << "Group::PrefixSumPlus(int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                             MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value += initial;
}
void Group::ExPrefixSumPlusInt(int& value, const int& initial) {
    LOG << "Group::ExPrefixSumPlus(int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                               MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value = (my_rank_ == 0 ? initial : value + initial);
}
void Group::BroadcastInt(int& value, size_t origin) {
    LOG << "Group::Broadcast(int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Ibcast(&value, 1, MPI_INT, origin,
                              MPI_COMM_WORLD, &request);
        });
}
void Group::AllReducePlusInt(int& value) {
    LOG << "Group::AllReducePlus(int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_INT,
                MPI_SUM, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMinimumInt(int& value) {
    LOG << "Group::AllReduceMinimum(int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_INT,
                MPI_MIN, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMaximumInt(int& value) {
    LOG << "Group::AllReduceMaximum(int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_INT,
                MPI_MAX, MPI_COMM_WORLD, &request);
        });
}
void Group::PrefixSumPlusUnsignedInt(unsigned int& value, const unsigned int& initial) {
    LOG << "Group::PrefixSumPlus(unsigned int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                             MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value += initial;
}
void Group::ExPrefixSumPlusUnsignedInt(unsigned int& value, const unsigned int& initial) {
    LOG << "Group::ExPrefixSumPlus(unsigned int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED,
                               MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value = (my_rank_ == 0 ? initial : value + initial);
}
void Group::BroadcastUnsignedInt(unsigned int& value, size_t origin) {
    LOG << "Group::Broadcast(unsigned int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Ibcast(&value, 1, MPI_UNSIGNED, origin,
                              MPI_COMM_WORLD, &request);
        });
}
void Group::AllReducePlusUnsignedInt(unsigned int& value) {
    LOG << "Group::AllReducePlus(unsigned int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED,
                MPI_SUM, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMinimumUnsignedInt(unsigned int& value) {
    LOG << "Group::AllReduceMinimum(unsigned int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED,
                MPI_MIN, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMaximumUnsignedInt(unsigned int& value) {
    LOG << "Group::AllReduceMaximum(unsigned int);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED,
                MPI_MAX, MPI_COMM_WORLD, &request);
        });
}
void Group::PrefixSumPlusLong(long& value, const long& initial) {
    LOG << "Group::PrefixSumPlus(long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                             MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value += initial;
}
void Group::ExPrefixSumPlusLong(long& value, const long& initial) {
    LOG << "Group::ExPrefixSumPlus(long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_LONG,
                               MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value = (my_rank_ == 0 ? initial : value + initial);
}
void Group::BroadcastLong(long& value, size_t origin) {
    LOG << "Group::Broadcast(long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Ibcast(&value, 1, MPI_LONG, origin,
                              MPI_COMM_WORLD, &request);
        });
}
void Group::AllReducePlusLong(long& value) {
    LOG << "Group::AllReducePlus(long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_LONG,
                MPI_SUM, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMinimumLong(long& value) {
    LOG << "Group::AllReduceMinimum(long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_LONG,
                MPI_MIN, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMaximumLong(long& value) {
    LOG << "Group::AllReduceMaximum(long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_LONG,
                MPI_MAX, MPI_COMM_WORLD, &request);
        });
}
void Group::PrefixSumPlusUnsignedLong(unsigned long& value, const unsigned long& initial) {
    LOG << "Group::PrefixSumPlus(unsigned long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                             MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value += initial;
}
void Group::ExPrefixSumPlusUnsignedLong(unsigned long& value, const unsigned long& initial) {
    LOG << "Group::ExPrefixSumPlus(unsigned long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG,
                               MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value = (my_rank_ == 0 ? initial : value + initial);
}
void Group::BroadcastUnsignedLong(unsigned long& value, size_t origin) {
    LOG << "Group::Broadcast(unsigned long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Ibcast(&value, 1, MPI_UNSIGNED_LONG, origin,
                              MPI_COMM_WORLD, &request);
        });
}
void Group::AllReducePlusUnsignedLong(unsigned long& value) {
    LOG << "Group::AllReducePlus(unsigned long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG,
                MPI_SUM, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMinimumUnsignedLong(unsigned long& value) {
    LOG << "Group::AllReduceMinimum(unsigned long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG,
                MPI_MIN, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMaximumUnsignedLong(unsigned long& value) {
    LOG << "Group::AllReduceMaximum(unsigned long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG,
                MPI_MAX, MPI_COMM_WORLD, &request);
        });
}
void Group::PrefixSumPlusLongLong(long long& value, const long long& initial) {
    LOG << "Group::PrefixSumPlus(long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                             MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value += initial;
}
void Group::ExPrefixSumPlusLongLong(long long& value, const long long& initial) {
    LOG << "Group::ExPrefixSumPlus(long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_LONG_LONG,
                               MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value = (my_rank_ == 0 ? initial : value + initial);
}
void Group::BroadcastLongLong(long long& value, size_t origin) {
    LOG << "Group::Broadcast(long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Ibcast(&value, 1, MPI_LONG_LONG, origin,
                              MPI_COMM_WORLD, &request);
        });
}
void Group::AllReducePlusLongLong(long long& value) {
    LOG << "Group::AllReducePlus(long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_LONG_LONG,
                MPI_SUM, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMinimumLongLong(long long& value) {
    LOG << "Group::AllReduceMinimum(long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_LONG_LONG,
                MPI_MIN, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMaximumLongLong(long long& value) {
    LOG << "Group::AllReduceMaximum(long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_LONG_LONG,
                MPI_MAX, MPI_COMM_WORLD, &request);
        });
}
void Group::PrefixSumPlusUnsignedLongLong(unsigned long long& value, const unsigned long long& initial) {
    LOG << "Group::PrefixSumPlus(unsigned long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iscan(MPI_IN_PLACE, &value, 1, MPI_INT,
                             MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value += initial;
}
void Group::ExPrefixSumPlusUnsignedLongLong(unsigned long long& value, const unsigned long long& initial) {
    LOG << "Group::ExPrefixSumPlus(unsigned long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iexscan(MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG,
                               MPI_SUM, MPI_COMM_WORLD, &request);
        });
    value = (my_rank_ == 0 ? initial : value + initial);
}
void Group::BroadcastUnsignedLongLong(unsigned long long& value, size_t origin) {
    LOG << "Group::Broadcast(unsigned long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Ibcast(&value, 1, MPI_UNSIGNED_LONG_LONG, origin,
                              MPI_COMM_WORLD, &request);
        });
}
void Group::AllReducePlusUnsignedLongLong(unsigned long long& value) {
    LOG << "Group::AllReducePlus(unsigned long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG,
                MPI_SUM, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMinimumUnsignedLongLong(unsigned long long& value) {
    LOG << "Group::AllReduceMinimum(unsigned long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG,
                MPI_MIN, MPI_COMM_WORLD, &request);
        });
}
void Group::AllReduceMaximumUnsignedLongLong(unsigned long long& value) {
    LOG << "Group::AllReduceMaximum(unsigned long long);";
    WaitForRequest(
        [&](MPI_Request& request) {
            return MPI_Iallreduce(
                MPI_IN_PLACE, &value, 1, MPI_UNSIGNED_LONG_LONG,
                MPI_MAX, MPI_COMM_WORLD, &request);
        });
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
                                MPI_THREAD_SERIALIZED, &provided);
        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Init_thread()", r);

        if (provided < MPI_THREAD_SERIALIZED)
            die("ERROR: MPI_Init_thread() only provided= " << provided);

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
bool Construct(size_t group_size, DispatcherThread& dispatcher,
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
        groups[i] = std::make_unique<Group>(my_rank, i, group_size, dispatcher);
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
