/*******************************************************************************
 * c7a/data/block_iterator.hpp
 *
 ******************************************************************************/
#pragma once

#include <vector>
#include "serializer.hpp"

namespace c7a {
namespace data {

//! internal representation of data elements
typedef std::string Blob;

//! BlockIterator gives you access to data of a block
template<class T>
class BlockIterator
{
public:
    //! Creates an instance of iterator that deserializes blobs to T
    BlockIterator<T>(std::vector<Blob>::const_iterator begin, std::vector<Blob>::const_iterator end) : _begin(begin), _end(end) { }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const T next() {
        const T& elem = *_begin;
        _begin++;
        return Deserialize<T>(elem);
    }

    //! returns true an element is available
    inline bool has_next() {
        return _begin != _end;
    }

private:
    std::vector<Blob>::const_iterator _begin;
    std::vector<Blob>::const_iterator _end;
};

}
}
