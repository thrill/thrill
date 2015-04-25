/*******************************************************************************
 * c7a/data/block_iterator.hpp
 *
 ******************************************************************************/
#pragma once

#include <vector>
#include "serializer.hpp"

namespace c7a {
namespace data {

//! BlockIterator gives you access to data of a block
template<class T>
class BlockIterator
{
public:
    //! Creates an instance of iterator that deserializes blobs to T
    BlockIterator<T>(const std::vector<Blob>& data)
        : data_(data), pos_(0) { }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const T Next() {
        return Deserialize<T>(data_[pos_++]);
    }

    //! returns true an element is available
    inline bool HasNext() {
        return data_.size() > pos_;
    }

private:
    const std::vector<Blob>& data_;
    size_t pos_;
};

}
}
