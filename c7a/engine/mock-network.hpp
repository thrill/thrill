/*******************************************************************************
 * c7a/engine/mock-network.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_ENGINE_MOCK_NETWORK_HEADER
#define C7A_ENGINE_MOCK_NETWORK_HEADER

//
// Created by Matthias Stumpp on 25/04/15.
//

#include <mutex>
#include <deque>
#include <vector>
#include <string>
#include <stdlib.h>

namespace c7a {

namespace engine {

class MockNetwork
{
public:
    //! Register a valid receiver
    void add_client(size_t id)
    {
        if (clients_.size() < id + 1) {
            valid_client_.resize(id + 1);
            clients_.resize(id + 1);
        }

        valid_client_[id] = true;
    }

    //! "Send" a message to id.
    void sendToWorker(size_t from_id, size_t dest_id, const std::string& data)
    {
        //assert(dest_id < valid_client_.size() && valid_client_[dest_id]);

        std::unique_lock<std::mutex> lock(mutex_);
        //assert(dest_id < clients_.size());
        clients_[dest_id].push_back(std::make_tuple(from_id, data));
    }

    //! Receive any message for for_id, deliver sender_id (source id) and data.
    bool receiveFromAny(size_t for_id, size_t* sender_id, std::string* out_data)
    {
        //assert(for_id < valid_client_.size() && valid_client_[for_id]);

        // TODO(tb): this is an evil busy waiting loop
        do {
            std::unique_lock<std::mutex> lock(mutex_);

            if (clients_[for_id].size()) {
                std::tie(*sender_id, *out_data) = clients_[for_id].front();
                clients_[for_id].pop_front();
                break;
            }
        } while (1);

        return true;
    }

private:
    //! Mutex to lock access to message queues
    std::mutex mutex_;

    //! vector of valid client ids (use add_client to set to valid)
    std::vector<char> valid_client_;

    //! type of message queue items
    typedef std::tuple<size_t, std::string> DataPair;

    //! type of message queue
    typedef std::deque<DataPair> DataVector;

    //! message queue for each of the registered clients
    std::vector<DataVector> clients_;
};

class MockSelect
{
public:
    MockSelect(MockNetwork& network, size_t my_id)
        : network_(network),
          my_id_(my_id)
    {
        network_.add_client(my_id);
    }

    void sendToWorkerString(size_t dest_id, const std::string& data)
    {
        network_.sendToWorker(my_id_, dest_id, data);
    }

    template <typename T>
    void sendToWorker(size_t dest_id, const T& data)
    {
        // TODO(ms): use serialize and call sendToWorkerString
        abort();
    }

    bool receiveFromAnyString(size_t* sender_id, std::string* out_data)
    {
        return network_.receiveFromAny(my_id_, sender_id, out_data);
    }

    template <typename T>
    bool receiveFromAny(size_t* sender_id, T* out_data)
    {
        // TODO(ms): use serialize and call receiveFromAny
        abort();
    }

protected:
    //! reference to common network.
    MockNetwork& network_;

    //! my client id in network.
    size_t my_id_;
};

} // namespace engine

} // namespace c7a

#endif // !C7A_ENGINE_MOCK_NETWORK_HEADER

/******************************************************************************/
