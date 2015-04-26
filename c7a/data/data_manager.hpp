/*******************************************************************************
 * c7a/data/data_manager.hpp
 *
 ******************************************************************************/

#ifndef C7A_DATA_DATA_MANAGER_HEADER
#define C7A_DATA_DATA_MANAGER_HEADER

#include <map>
#include <functional>
#include <stdexcept>
#include <memory> //unique_ptr
#include "block_iterator.hpp"
#include "../common/logger.hpp"

namespace c7a {
namespace data {

//! Identification for DIAs
typedef int DIAId;

//! function Signature for an emitt function
template<typename T>
using BlockEmitter = std::function<void(T)>;

//! Stores in-memory data
//!
//! Future versions: Provide access to remote DIAs
class DataManager
{
public:

    DataManager() : nextId_(0) { }

    DataManager(const DataManager&) = delete;
    DataManager & operator=(const DataManager&) = delete;

    //! returns iterator on requested partition
    //!
    //! \param id ID of the DIA
    template<class T>
    BlockIterator<T> GetLocalBlocks(DIAId id) {
        if (!Contains(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        return BlockIterator<T>(*data_[id]);
    }

    //! returns true if the manager holds data of given DIA
    //!
    //! \param id ID of the DIA
    bool Contains(DIAId id) {
        return data_.find(id) != data_.end();
    }

    DIAId AllocateDIA() {
        SpacingLogger(true) << "Allocate DIA" << nextId_;
        data_[nextId_] = std::unique_ptr<std::vector<Blob>>( new std::vector<Blob>() );
        return nextId_++;
    }

    template<class T>
    BlockEmitter<T> GetLocalEmitter(DIAId id) {
        if (!Contains(id)) {
            throw std::runtime_error("target dia id unknown.");
        }
        auto& target = data_[id]; //yes. const ref to an unique_ptr
        return [& target](T elem){ target->push_back(Serialize(elem)); };
    }

private:
    DIAId nextId_;

    //YES I COULD just use a map of (int, vector) BUT then I have a weird
    //behaviour of std::map on inserts. Sometimes it randomly kills itself.
    //May depend on the compiler. Google it.
    std::map<DIAId, std::unique_ptr<std::vector<Blob>>> data_;
};

}
}

#endif // !C7A_DATA_DATA_MANAGER_HEADER

/******************************************************************************/
