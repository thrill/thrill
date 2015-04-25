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
    BlockIterator<T>(std::vector<Blob>::const_iterator begin, std::vector<Blob>::const_iterator end) : begin_(begin), end_(end) { }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const T Next() {
        const T& elem = *begin_;
        begin_++;
        return Deserialize<T>(elem);
    }

    //! returns true an element is available
    inline bool HasNext() {
        return begin_ != end_;
    }

private:
    std::vector<Blob>::const_iterator begin_;
    std::vector<Blob>::const_iterator end_;
};

}
}
