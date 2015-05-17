/*******************************************************************************
 * c7a/data/data_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_DATA_MANAGER_HEADER
#define C7A_DATA_DATA_MANAGER_HEADER

#include <map>
#include <functional>
#include <string>
#include <stdexcept>
#include <memory> //unique_ptr

#include <c7a/net/channel_multiplexer.hpp>
#include "block_iterator.hpp"
#include "block_emitter.hpp"
#include "buffer_chain.hpp"
#include <c7a/common/logger.hpp>
#include <c7a/data/socket_target.hpp>
#include "input_line_iterator.hpp"

namespace c7a {
namespace data {
struct BufferChain;

//! Identification for DIAs
typedef size_t DIAId;
typedef size_t ChannelId;

//! Manages all kind of memory for data elements
//!
//!
//! Provides Channel creation for sending / receiving data from other workers.
class DataManager {
public:
    DataManager(net::ChannelMultiplexer& cmp) : cmp_(cmp), next_local_id_(0), next_remote_id_(0) { }

    DataManager(const DataManager&) = delete;
    DataManager& operator = (const DataManager&) = delete;

    //! returns iterator on requested partition.
    //!
    //! Data can be emitted into this partition even after the iterator was created.
    //!
    //! \param id ID of the DIA - determined by AllocateDIA()
    template <class T>
    BlockIterator<T> GetLocalBlocks(DIAId id) {
        if (!ContainsLocal(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        return BlockIterator<T>(*(local_buffer_chains_[id]));
    }

    //! Returns iterator on the data that was / will be received on the channel
    //!
    //! \param id ID of the channel - determined by AllocateNetworkChannel()
    template <class T>
    BlockIterator<T> GetRemoteBlocks(ChannelId id) {
        if (!ContainsChannel(id)) {
            throw std::runtime_error("target channel id unknown.");
        }

        return BlockIterator<T>(*(incoming_buffer_chains_[id]));
    }

    //! Returns a number that uniquely addresses a DIA
    //! Calls to this method alter the data managers state.
    //! Calls to this method must be in deterministic order for all workers!
    DIAId AllocateDIA() {
        //sLOG << "Allocate DIA" << data_.size();
        //data_.push_back(std::vector<Blob>());
        //return data_.size() - 1;
        sLOG << "Allocate DIA" << next_local_id_;
        local_buffer_chains_[next_local_id_] = std::make_shared<BufferChain>();
        return next_local_id_++;
    }

    //! Returns a number that uniquely addresses a network channel
    //! Calls to this method alter the data managers state.
    //! Calls to this method must be in deterministic order for all workers!
    ChannelId AllocateNetworkChannel() {
        if (!ContainsChannel(next_remote_id_)) {
            sLOG << "Allocate Network Channel" << next_remote_id_;
            incoming_buffer_chains_[next_remote_id_] = std::make_shared<BufferChain>();
        }
        else {
            sLOG << "Allocate Network Channel" << next_remote_id_ << "already exists";
        }
        return next_remote_id_++;
    }

    //! Returns an emitter that can be used to fill a DIA
    //! Emitters can push data into DIAs even if an intertor was created before.
    //! Data is only visible to the iterator if the emitter was flushed.
    template <class T>
    BlockEmitter<T> GetLocalEmitter(DIAId id) {
        if (!ContainsLocal(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        std::shared_ptr<BufferChain> target = local_buffer_chains_[id];
        return BlockEmitter<T>(target);
    }

    template <class T>
    std::vector<BlockEmitter<T> > GetNetworkEmitters(ChannelId id) {
        if (!ContainsChannel(id)) {
            throw std::runtime_error("target channel id unknown.");
        }
        auto& target = incoming_buffer_chains_[id];
        return cmp_.OpenChannel<T>(id, target);
    }

    //!Returns an InputLineIterator with a given input file stream.
    //!
    //! \param file Input file stream
    //!
    //! \return An InputLineIterator for a given file stream
    InputLineIterator GetInputLineIterator(std::ifstream& file) {
        //TODO: get those from networks
        size_t my_id = 0;
        size_t num_work = 1;

        return InputLineIterator(file, my_id, num_work);
    }

private:
    static const bool debug = true;
    net::ChannelMultiplexer& cmp_;

    DIAId next_local_id_;
    ChannelId next_remote_id_;

    //YES I COULD just use a map of (int, vector) BUT then I have a weird
    //behaviour of std::map on inserts. Sometimes it randomly kills itself.
    //May depend on the compiler. Google it.
    //std::vector<std::vector<Blob> > data_;
    //TODO(ts) check if that sutt is still a problem. if not - remove the comment
    std::map<DIAId, std::shared_ptr<BufferChain> > local_buffer_chains_;

    std::map<ChannelId, std::shared_ptr<BufferChain> > incoming_buffer_chains_;

    //! returns true if the manager holds data of given DIA
    //!
    //! \param id ID of the DIA
    bool ContainsLocal(DIAId id) {
        return local_buffer_chains_.find(id) != local_buffer_chains_.end();
        //return data_.size() > id && id >= 0;
    }

    //! returns true if the manager holds data of given DIA
    //!
    //! \param id ID of the DIA
    bool ContainsChannel(ChannelId id) {
        return incoming_buffer_chains_.find(id) != incoming_buffer_chains_.end();
        //return data_.size() > id && id >= 0;
    }
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_DATA_MANAGER_HEADER

/******************************************************************************/
