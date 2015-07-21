/*******************************************************************************
 * c7a/data/output_line_emitter.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_OUTPUT_LINE_EMITTER_HEADER
#define C7A_DATA_OUTPUT_LINE_EMITTER_HEADER

#include <c7a/data/serializer.hpp>

#include <fstream>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

//! OutputLineEmitter let's you write to files
//! Each element is written to a new line
template <class T>
class OutputLineEmitter
{
public:
    explicit OutputLineEmitter(std::ofstream& file) :
        out_(file),
        closed_(false) { }

    void operator () (T x) {
        out_ << Serialize<T>(x) << std::endl;
    }

    //! Flushes and closes the block (cannot be undone)
    //! No further emitt operations can be done afterwards.
    void Close() {
        assert(!closed_);
        closed_ = true;
        out_.close();
    }

    //! Writes the data to the target without closing the emitter
    void Flush() {
        out_.flush();
    }

private:
    std::ofstream& out_;
    bool closed_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_OUTPUT_LINE_EMITTER_HEADER

/******************************************************************************/
