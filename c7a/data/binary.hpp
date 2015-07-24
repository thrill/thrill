/*******************************************************************************
 * c7a/data/binary.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BINARY_HEADER
#define C7A_DATA_BINARY_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/data/file.hpp>
#include <cereal/cereal.hpp>
#include <sstream>

namespace c7a {
namespace data {

/*
  Copyright (c) 2014, Randolph Voorhies, Shane Grant
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:
      * Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.
      * Neither the name of cereal nor the
        names of its contributors may be used to endorse or promote products
        derived from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL RANDOLPH VOORHIES OR SHANE GRANT BE LIABLE FOR ANY
  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// ######################################################################
//! An output archive designed to save data in a compact binary representation
/*! This archive outputs data to a stream in an extremely compact binary
    representation with as little extra metadata as possible.

    This archive does nothing to ensure that the endianness of the saved
    and loaded data is the same.  If you need to have portability over
    architectures with different endianness, use Portablec7aOutputArchive_cp.

    When using a binary archive and a file stream, you must use the
    std::ios::binary format flag to avoid having your data altered
    inadvertently.

    \ingroup Archives */
template <typename Writer>
class c7aOutputArchive_cp : public cereal::OutputArchive<c7aOutputArchive_cp<Writer>, cereal::AllowEmptyClassElision>
{
public:
    //! Construct, outputting to the provided stream
    /*! @param stream The stream to output to.  Can be a stringstream, a file stream, or
                      even cout! */
    c7aOutputArchive_cp(Writer& writer) :
        cereal::OutputArchive<c7aOutputArchive_cp<Writer>, cereal::AllowEmptyClassElision>(this),
        writer_(writer)
    { }

    //! Writes size bytes of data to the output stream
    void saveBinary(const void* data, std::size_t size) {
        writer_.Append(data, size);
    }

private:
    Writer& writer_;
};

// ######################################################################
//! An input archive designed to load data saved using c7aOutputArchive_cp
/*  This archive does nothing to ensure that the endianness of the saved
    and loaded data is the same.  If you need to have portability over
    architectures with different endianness, use Portablec7aOutputArchive_cp.

    When using a binary archive and a file stream, you must use the
    std::ios::binary format flag to avoid having your data altered
    inadvertently.

    \ingroup Archives */
class c7aInputArchive_cp : public cereal::InputArchive<c7aInputArchive_cp, cereal::AllowEmptyClassElision>
{
public:
    //! Construct, loading from the provided stream
    c7aInputArchive_cp(c7a::data::File::Reader& reader) :
        InputArchive<c7aInputArchive_cp, cereal::AllowEmptyClassElision>(this),
        reader_(reader)
    { }

    //! Reads size bytes of data from the input stream
    void loadBinary(void* const data, std::size_t size) {
        reader_.Read(data, size);
    }

private:
    c7a::data::File::Reader& reader_;
};

// ######################################################################
// Common BinaryArchive serialization functions

//! Saving for POD types to binary
template <class T, typename Writer>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_SAVE_FUNCTION_NAME(c7aOutputArchive_cp<Writer>& ar, T const& t) {
    ar.saveBinary(std::addressof(t), sizeof(t));
}

//! Loading for POD types from binary
template <class T>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_LOAD_FUNCTION_NAME(c7aInputArchive_cp& ar, T& t) {
    ar.loadBinary(std::addressof(t), sizeof(t));
}

//! Serializing NVP types to binary
template <class Archive, class T, typename Writer>
inline
CEREAL_ARCHIVE_RESTRICT(c7aInputArchive_cp, c7aOutputArchive_cp<Writer>)
CEREAL_SERIALIZE_FUNCTION_NAME(Archive & ar, cereal::NameValuePair<T>&t)
{
    ar(t.value);
}

//! Serializing SizeTags to binary
template <class Archive, class T, typename Writer>
inline
CEREAL_ARCHIVE_RESTRICT(c7aInputArchive_cp, c7aOutputArchive_cp<Writer>)
CEREAL_SERIALIZE_FUNCTION_NAME(Archive & ar, cereal::SizeTag<T>&t)
{
    ar(t.size);
}

//! Saving binary data
template <class T, typename Writer>
inline
void CEREAL_SAVE_FUNCTION_NAME(c7aOutputArchive_cp<Writer>& ar, cereal::BinaryData<T> const& bd) {
    ar.saveBinary(bd.data, static_cast<std::size_t>(bd.size));
}

//! Loading binary data
template <class T>
inline
void CEREAL_LOAD_FUNCTION_NAME(c7aInputArchive_cp& ar, cereal::BinaryData<T>& bd) {
    ar.loadBinary(bd.data, static_cast<std::size_t>(bd.size));
}
} //namespace data
} //namespace c7a

// register archives for polymorphic support
// CEREAL_REGISTER_ARCHIVE(c7a::data::c7aOutputArchive_cp)
// CEREAL_REGISTER_ARCHIVE(c7a::data::c7aInputArchive_cp)

// tie input and output archives together

// CEREAL_SETUP_ARCHIVE_TRAITS(c7a::data::c7aInputArchive_cp, c7a::data::c7aOutputArchive_cp)
namespace cereal {
namespace traits {
namespace detail {

template <>
struct get_output_from_input<c7a::data::c7aInputArchive_cp>
{
    using type = c7a::data::c7aOutputArchive_cp<c7a::data::BlockWriter>;
};

template <typename Writer>
struct get_input_from_output<c7a::data::c7aOutputArchive_cp<Writer>>
{
    using type = c7a::data::c7aInputArchive_cp;
};

}
}
}     /* end namespaces */

#endif // !C7A_DATA_BINARY_HEADER

/******************************************************************************/
