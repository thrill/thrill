/*******************************************************************************
 * c7a/data/data_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_DATA_MANAGER_HEADER
#define C7A_DATA_DATA_MANAGER_HEADER

#include <map>
#include <functional>
#include <string>
#include <stdexcept>
#include <memory> //unique_ptr

#include "block_iterator.hpp"
#include "block_emitter.hpp"
#include "buffer_chain.hpp"
#include <c7a/common/logger.hpp>
#include "input_line_iterator.hpp"

namespace c7a {
namespace data {
//! Identification for DIAs
typedef int DIAId;

//! Manages all kind of memory for data elements
//!
//!
//! Provides Channel creation for sending / receiving data from other workers.
class DataManager
{
public:
    DataManager() : nextId_(0) { }

    DataManager(const DataManager&) = delete;
    DataManager& operator = (const DataManager&) = delete;

    //! returns iterator on requested partition
    //!
    //! \param id ID of the DIA
    template <class T>
    BlockIterator<T> GetLocalBlocks(DIAId id)
    {
        if (!Contains(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        return BlockIterator<T>(buffer_chains_[id]);
    }

    //! returns true if the manager holds data of given DIA
    //!
    //! \param id ID of the DIA
    bool Contains(DIAId id)
    {
        return buffer_chains_.find(id) != buffer_chains_.end();
        //return data_.size() > id && id >= 0;
    }

    DIAId AllocateDIA()
    {
        //SpacingLogger(true) << "Allocate DIA" << data_.size();
        //data_.push_back(std::vector<Blob>());
        //return data_.size() - 1;
        SpacingLogger(true) << "Allocate DIA" << nextId_;
        buffer_chains_[nextId_] = BufferChain();
        return nextId_++;
    }

    template <class T>
    BlockEmitter<T> GetLocalEmitter(DIAId id)
    {
        if (!Contains(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        auto& target = buffer_chains_[id];
        return BlockEmitter<T>(target);
    }

    //!Returns an InputLineIterator with a given input file stream.
    //!
    //! \param file Input file stream
    //!
    //! \return An InputLineIterator for a given file stream
    InputLineIterator GetInputLineIterator(std::ifstream& file)
    {
        //TODO: get those from networks
        size_t my_id = 0;
        size_t num_work = 1;

        return InputLineIterator(file, my_id, num_work);
    }

private:
    DIAId nextId_;

    //YES I COULD just use a map of (int, vector) BUT then I have a weird
    //behaviour of std::map on inserts. Sometimes it randomly kills itself.
    //May depend on the compiler. Google it.
    //std::vector<std::vector<Blob> > data_;
    std::map<DIAId, BufferChain> buffer_chains_;
};
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_DATA_MANAGER_HEADER

/******************************************************************************/
