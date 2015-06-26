/*******************************************************************************
 * c7a/net/network.hpp
 * 
 * The Network class provides a collection of workers which are arragnged in a
 * certain speified topology. The developer of the network can specify her own
 * new topologies. This arrangement helps with efficient collective
 * communication.
 *
 * Part of Project c7a.
 *
 *      
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_NETWORK_HEADER
#define C7A_NET_NETWORK_HEADER

#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>

namespace c7a {
namespace net {

//! Specify supported Topologies. The purpose of this enum is for the user of
//! the network to know which topology it currently has.
enum Topology { EMPTY, LINKED_LIST }; // FIBONACCI_TREE might be inside here too

//! A collection of group pointers
typedef std::vector<std::shared_ptr<Group> > group_coll_t;

//! @brief Network with a given topology
// 
//! @details
//! Technical details and notes:
//! The implementation of this class can be made either with a template
//! parameter or with inheritance, thereby specifying a topology abstract class
//! from which other class inherit and implement a certain
//! "CreateTopology()"-function. This has the benefit that the topology of the
//! network can be changed during run-time. When the implementation is done with
//! a template parameter, the topology can be specified only at compile-time.
//! This is also achieved with a function, which receives a user-defined
//! topology creation function and runs it.
//!
//! This can be also done by expecting a topology-creation function in a
//! member method. The member method then calls this function on the list of
//! groups in this class.
class Network
{
    static const bool debug = false;

public:
    //! @brief   Creates a fully connected network of groups (i.e. workers) with
    //!          an empty topology.
    //!
    //! @details By default creates a network with no topology. The supplied
    //!          topology creation function sets the pointers of each worker to
    //!          point according to the data structure (e.g. next, prev for
    //!          linked list) and sets the topology variable to the kind of
    //!          topology that was used.

    //! @param   num_clients_ The number of workers in the network
    //// @param   topology_creator_fn The function which creates the topology. It
    ////                              receives the collection of workers of the
    ////                              network and the topology variable.
    Network(size_t num_clients) 
        : num_clients_(num_clients),
          what_topology_(EMPTY) {
        using lowlevel::Socket;

        workers_.resize(num_clients);

        for (size_t i = 0; i != num_clients; ++i) {
            workers_[i] = std::shared_ptr<Group>(new Group(i, num_clients));
        }
        
        // construct a stream socket pair for (i,j) with i < j
        for (size_t i = 0; i != num_clients; ++i) {
            for (size_t j = i + 1; j < num_clients; ++j) {
                LOG << "doing Socket::CreatePair() for i=" << i << " j=" << j;

                std::pair<Socket, Socket> sp = Socket::CreatePair();

                workers_[i]->connections_[j] = std::move(Connection(sp.first));
                workers_[j]->connections_[i] = std::move(Connection(sp.second));
            }
        }
    }

    //! Creates a linked list topology, by setting the next and prev fields in
    //! each group in the collection
    void CreateLinkedListTopology() {
        what_topology_ = LINKED_LIST;

        for (size_t i = 0; i < workers_.size() - 1; ++i) {
            workers_[i]->next = workers_[i + 1];
        }

        root = workers_[0];
    }

    //! Returns the root of the data structure
    std::shared_ptr<Group> getRoot() {
        return root;
    }

    //! Returns what topology the network currently has
    Topology getTopology() {
        return what_topology_;
    }

private:
    size_t num_clients_;
    std::vector<std::shared_ptr<Group> > workers_;
    Topology what_topology_;

    //! The root of the topology data structure
    std::shared_ptr<Group> root;
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NETWORK_HEADER

/******************************************************************************/
