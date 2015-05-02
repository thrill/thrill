/*******************************************************************************
 * c7a/data/input_line_iterator.hpp
 *
 ******************************************************************************/
#pragma once

#include <stdio.h>
#include <string>
#include <iostream>
#include "serializer.hpp"

namespace c7a {
namespace data {

//! InputLineIterator gives you access to lines of a file
class InputLineIterator
{
public:
    //! Creates an instance of iterator that deserializes blobs to T
    InputLineIterator(std::ifstream & file)
        : file_(file), pos_(0) { }

    //! returns the next element if one exists
    //!
    //! does no checks whether a next element exists!
    inline const std::string Next() {
        std::string line;
        std::getline(file_, line);
        return line;
    }

    //! returns true an element is available
    inline bool HasNext() {
        int c = file_.peek();  // peek character

        if ( c != EOF ) {
            return true;
        } else {
            return false;
        }
    }

private:
    std::ifstream & file_;
    size_t pos_;
};

}
}
