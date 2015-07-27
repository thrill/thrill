/*******************************************************************************
 * c7a/data/serializer_cereal_archive.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER
#define C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER

#include <cereal/cereal.hpp>
#include <cereal/details/traits.hpp>

#include <sstream>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

namespace serializer_cereal {

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
/*! This archive outputs data to a reader of writer of c7a directly in an
    extremely compact binary representation with as little extra metadata as
    possible.

    This archive does nothing to ensure that the endianness of the saved
    and loaded data is the same.  If you need to have portability over
    architectures with different endianness, use Portablec7aOutputArchive.

    When using a binary archive and a file stream, you must use the
    std::ios::binary format flag to avoid having your data altered
    inadvertently.

    \ingroup Archives */

template <typename Writer>
class c7aOutputArchive : public cereal::OutputArchive<c7aOutputArchive<Writer>, cereal::AllowEmptyClassElision>
{
public:
    //! Construct, outputting to the provided c7a writer
    /*! @param stream The stream to output to.  Can be a stringstream, a file stream, or
                      even cout! */
    c7aOutputArchive(Writer& writer) :
        cereal::OutputArchive<c7aOutputArchive<Writer>, cereal::AllowEmptyClassElision>(this),
        writer_(writer)
    { }

    //! Writes size bytes of data to the c7a writer
    void saveBinary(const void* data, std::size_t size) {
        writer_.Append(data, size);
    }

private:
    Writer& writer_;
};

// ######################################################################
//! An input archive designed to load data saved using c7aOutputArchive
/*  This archive does nothing to ensure that the endianness of the saved
    and loaded data is the same.  If you need to have portability over
    architectures with different endianness, use Portablec7aOutputArchive.

    When using a binary archive and a file stream, you must use the
    std::ios::binary format flag to avoid having your data altered
    inadvertently.

    \ingroup Archives */
template <typename Reader>
class c7aInputArchive : public cereal::InputArchive<c7aInputArchive<Reader>, cereal::AllowEmptyClassElision>
{
public:
    //! Construct, loading from the provided c7a reader
    c7aInputArchive(Reader& reader) :
        cereal::InputArchive<c7aInputArchive<Reader>, cereal::AllowEmptyClassElision>(this),
        reader_(reader)
    { }

    //! Reads size bytes of data from the input c7a reader
    void loadBinary(void* const data, std::size_t size) {
        reader_.Read(data, size);
    }

private:
    Reader& reader_;
};

// ######################################################################
// Common BinaryArchive serialization functions

//! Saving for POD types to binary
template <class T, typename Writer>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_SAVE_FUNCTION_NAME(c7aOutputArchive<Writer>& ar, T const& t) {
    ar.saveBinary(std::addressof(t), sizeof(t));
}

//! Loading for POD types from binary
template <class Reader, class T>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_LOAD_FUNCTION_NAME(c7aInputArchive<Reader>& ar, T& t) {
    ar.loadBinary(std::addressof(t), sizeof(t));
}

//! Serializing NVP types to binary
template <class Writer, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(c7aOutputArchive<Writer>& ar, cereal::NameValuePair<T>& t) {
    ar(t.value);
}

//! Serializing NVP types to binary
template <class Reader, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(c7aInputArchive<Reader>& ar, cereal::NameValuePair<T>& t) {
    ar(t.value);
}

//! Serializing SizeTags to binary
template <class Writer, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(c7aOutputArchive<Writer>& ar, cereal::SizeTag<T>& t) {
    ar(t.size);
}

//! Serializing SizeTags to binary
template <class Reader, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(c7aInputArchive<Reader>& ar, cereal::SizeTag<T>& t) {
    ar(t.size);
}

//! Saving binary data
template <class T, typename Writer>
inline
void CEREAL_SAVE_FUNCTION_NAME(c7aOutputArchive<Writer>& ar, cereal::BinaryData<T> const& bd) {
    ar.saveBinary(bd.data, static_cast<std::size_t>(bd.size));
}

//! Loading binary data
template <class Reader, class T>
inline
void CEREAL_LOAD_FUNCTION_NAME(c7aInputArchive<Reader>& ar, cereal::BinaryData<T>& bd) {
    ar.loadBinary(bd.data, static_cast<std::size_t>(bd.size));
}

} // namespace serializer_cereal

/************** Use cereal if serialization function is given *****************/
template <typename Archive, typename T>
struct Serializer<Archive, T, typename std::enable_if<
                      cereal::traits::is_input_serializable<T, Archive>::value&&
                      !std::is_pod<T>::value
                      >::type>
{
    static void Serialize(const T& t, Archive& a) {
        serializer_cereal::c7aOutputArchive<Archive> oarchive(a); // Create an output archive
        oarchive(t);                           // Write the data to the archive
    }

    static T Deserialize(Archive& a) {
        serializer_cereal::c7aInputArchive<Archive> iarchive(a);  // Create an output archive
        T res;
        iarchive(res);                         // Read the data from the archive
        return res;
    }
    static const bool fixed_size = false;
};

//! \}

} // namespace data
} // namespace c7a

// register archives for polymorphic support
// CEREAL_REGISTER_ARCHIVE(c7a::data::c7aOutputArchive)
// CEREAL_REGISTER_ARCHIVE(c7a::data::c7aInputArchive)

// tie input and output archives together
// CEREAL_SETUP_ARCHIVE_TRAITS(c7a::data::c7aInputArchive,
//                             c7a::data::c7aOutputArchive)

#endif // !C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER

/******************************************************************************/
