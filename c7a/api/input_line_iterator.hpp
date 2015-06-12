/*******************************************************************************
 * c7a/data/input_line_iterator.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <uagtc@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_INPUT_LINE_ITERATOR_HEADER
#define C7A_DATA_INPUT_LINE_ITERATOR_HEADER

#include <stdio.h>
#include <string>
#include <fstream>
#include <iostream>

#include <c7a/data/serializer.hpp>

namespace c7a {

//! InputLineIterator gives you access to lines of a file
class InputLineIterator
{
public:
    //! Creates an instance of iterator that reads file line based
    InputLineIterator(std::ifstream& file,
                      size_t my_id,
                      size_t num_workers)
        : file_(file),
          my_id_(my_id),
          num_workers_(num_workers) {
        //Find file size and save it
        file_.seekg(0, std::ios::end);
        file_size_ = file_.tellg();

        //Go to start of 'local part'.
        std::streampos per_worker = file_size_ / num_workers_;
        std::streampos my_start = per_worker * my_id_;
        file_.seekg(my_start, std::ios::beg);

        //Go to next new line if the stream-pointer is not at the beginning of a line
        if (my_id != 0) {
            file_.unget();
            if (file_.get() != '\n') {
                std::string str;
                std::getline(file_, str);
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

    //! returns true, if an element is available in local part
    inline bool HasNext() {
        std::streampos per_worker = file_size_ / num_workers_;
        std::streampos my_end = per_worker * (my_id_ + 1) - 1;

        return (file_.tellg() <= my_end);
    }

private:
    //!Input file stream
    std::ifstream& file_;
    //!File size in bytes
    size_t file_size_;
    //!Worker ID
    size_t my_id_;
    //!Total number of workers
    size_t num_workers_;
};

} // namespace c7a

#endif // !C7A_DATA_INPUT_LINE_ITERATOR_HEADER

/******************************************************************************/
