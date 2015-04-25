/*******************************************************************************
 * c7a/data/data_manager.hpp
 *
 ******************************************************************************/

#ifndef C7A_DATA_DATA_MANAGER_HEADER
#define C7A_DATA_DATA_MANAGER_HEADER

#include <map>
#include "block_iterator.hpp"

namespace c7a {
namespace data {

//! Identification for DIAs
typedef int DIAId;


//! Stores in-memory data
//!
//! Future versions: Provide access to remote DIAs
class DataManager
{
public:

    //! returns iterator on requested partition
    //!
    //! \param id ID of the DIA
    template<class T>
    BlockIterator<T> getLocalBlocks(DIAId id) {
        const auto& block = data_[id];
        return BlockIterator<T>(block.cbegin(), block.cend());
    }

    //! returns true if the manager holds data of given DIA
    //!
    //! \param id ID of the DIA
    bool Contains(DIAId id) {
        return data_.find(id) != data_.end();
    }

    /*BlockEmitter<T> getLocalEmitter(DIAId) {
        
    }*/

private:

    std::map<DIAId, std::vector<Blob>> data_;
};

}
}

#endif // !C7A_DATA_DATA_MANAGER_HEADER

/******************************************************************************/
