/*******************************************************************************
 * c7a/api/input_line_iterator.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_INPUT_LINE_ITERATOR_HEADER
#define C7A_API_INPUT_LINE_ITERATOR_HEADER

#include <c7a/data/serialization.hpp>

#include <fstream>
#include <string>

namespace c7a {

//! InputLineIterator gives you access to lines of a file
class InputLineIterator
{
public:
    //! Creates an instance of iterator that reads file line based
    InputLineIterator(std::ifstream& file,
                      size_t my_node_id,
                      size_t num_workers)
        : file_(file),
          my_node_id_(my_node_id),
          num_workers_(num_workers) {
        //Find file size and save it
        file_.seekg(0, std::ios::end);
        file_size_ = file_.tellg();

        //Go to start of 'local part'.
        std::streampos per_worker = file_size_ / num_workers_;
        std::streampos my_start = per_worker * my_node_id_;
        if (my_node_id_ == (num_workers)) {
            my_end_ = file_size_ - 1;
        }
        else {
            my_end_ = per_worker * (my_node_id_ + 1) - 1;
        }

        file_.seekg(my_start, std::ios::beg);

        //Go to next new line if the stream-pointer is not at the beginning of a line
        if (my_node_id != 0) {
            std::streampos previous = (per_worker * my_node_id_) - 1;
            file_.seekg(previous, std::ios::beg);
            //file_.unget();
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
        return (file_.tellg() <= my_end_);
    }

private:
    //!Input file stream
    std::ifstream& file_;
    //!File size in bytes
    size_t file_size_;
    //!Worker ID
    size_t my_node_id_;
    //!Total number of workers
    size_t num_workers_;
    //!End of local block
    std::streampos my_end_;
};

} // namespace c7a

#endif // !C7A_API_INPUT_LINE_ITERATOR_HEADER

/******************************************************************************/
