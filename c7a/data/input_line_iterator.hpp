/*******************************************************************************
 * c7a/data/input_line_iterator.hpp
 *
 ******************************************************************************/
#pragma once

#include <stdio.h>
#include <string>
#include <iostream>
#include "serializer.hpp"
#include <sys/stat.h>

namespace c7a {
namespace data {

//! InputLineIterator gives you access to lines of a file
class InputLineIterator
{
public:
    //! Creates an instance of iterator that deserializes blobs to T
    InputLineIterator(std::ifstream & file,
                          size_t file_size,
                          size_t my_id, 
                          size_t num_workers)
        : file_(file),
          file_size_(file_size), 
          my_id_(my_id),
          num_workers_(num_workers) {
        
        size_t per_worker = file_size_ / num_workers_;
        size_t my_start = per_worker * my_id_;
        file_.seekg(my_start,ios::beg);

        if(my_id != 0) {
            file_.unget();
            if (file_.get() != '\n') {
                std::string str;
                std::getline(file_,str);
            }            
        }

        
    }

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
        size_t per_worker = file_size_ / num_workers_;
        size_t my_end = per_worker * (my_id_ + 1) - 1;
       
        return (file_.tellg() <= my_end);
     }

private:
    std::ifstream & file_;
    size_t file_size_;
    size_t my_id_;
    size_t num_workers_;
};

}
}
