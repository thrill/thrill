/*******************************************************************************
 * thrill/data/serialization_cereal.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_SERIALIZATION_CEREAL_HEADER
#define THRILL_DATA_SERIALIZATION_CEREAL_HEADER

#include <cereal/cereal.hpp>
#include <cereal/details/traits.hpp>
#include <thrill/data/serialization.hpp>

#include <sstream>

namespace thrill {
namespace data {

//! \defgroup data_internal Data Internals
//! \ingroup data
//! \{

namespace serialization_cereal {

/*
  Original Archive Code from cereal library is
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

/*!
 * An output archive designed to save data in a compact binary
 * representation. Originally cereal::BinaryOutputArchive, adapted for thrill
 * BlockWriter interface.
 */
template <typename Writer>
class ThrillOutputArchive
    : public cereal::OutputArchive<ThrillOutputArchive<Writer>,
                                   cereal::AllowEmptyClassElision>
{
public:
    //! Construct, outputting to the provided thrill writer
    explicit ThrillOutputArchive(Writer& writer)
        : cereal::OutputArchive<ThrillOutputArchive<Writer>,
                                cereal::AllowEmptyClassElision>(this),
          writer_(writer)
    { }

    //! Writes size bytes of data to the thrill writer
    void saveBinary(const void* data, std::size_t size) {
        writer_.Append(data, size);
    }

private:
    Writer& writer_;
};

//! An input archive designed to load data saved using ThrillOutputArchive
template <typename Reader>
class ThrillInputArchive
    : public cereal::InputArchive<ThrillInputArchive<Reader>,
                                  cereal::AllowEmptyClassElision>
{
public:
    //! Construct, loading from the provided thrill reader
    explicit ThrillInputArchive(Reader& reader)
        : cereal::InputArchive<ThrillInputArchive<Reader>,
                               cereal::AllowEmptyClassElision>(this),
          reader_(reader)
    { }

    //! Reads size bytes of data from the input thrill reader
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
CEREAL_SAVE_FUNCTION_NAME(ThrillOutputArchive<Writer>& ar, T const& t) {
    ar.saveBinary(std::addressof(t), sizeof(t));
}

//! Loading for POD types from binary
template <class Reader, class T>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_LOAD_FUNCTION_NAME(ThrillInputArchive<Reader>& ar, T& t) {
    ar.loadBinary(std::addressof(t), sizeof(t));
}

//! Serializing NVP types to binary
template <class Writer, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(
    ThrillOutputArchive<Writer>& ar, cereal::NameValuePair<T>& t) {
    ar(t.value);
}

//! Serializing NVP types to binary
template <class Reader, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(
    ThrillInputArchive<Reader>& ar, cereal::NameValuePair<T>& t) {
    ar(t.value);
}

//! Serializing SizeTags to binary
template <class Writer, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(
    ThrillOutputArchive<Writer>& ar, cereal::SizeTag<T>& t) {
    ar(t.size);
}

//! Serializing SizeTags to binary
template <class Reader, class T>
inline void
CEREAL_SERIALIZE_FUNCTION_NAME(
    ThrillInputArchive<Reader>& ar, cereal::SizeTag<T>& t) {
    ar(t.size);
}

//! Saving binary data
template <class T, typename Writer>
inline
void CEREAL_SAVE_FUNCTION_NAME(
    ThrillOutputArchive<Writer>& ar, cereal::BinaryData<T> const& bd) {
    ar.saveBinary(bd.data, static_cast<std::size_t>(bd.size));
}

//! Loading binary data
template <class Reader, class T>
inline
void CEREAL_LOAD_FUNCTION_NAME(
    ThrillInputArchive<Reader>& ar, cereal::BinaryData<T>& bd) {
    ar.loadBinary(bd.data, static_cast<std::size_t>(bd.size));
}

} // namespace serialization_cereal

//! \}

//! \addtogroup data Data Subsystem
//! \{

/************** Use cereal if serialization function is given *****************/

template <typename Archive, typename T>
struct Serialization<Archive, T, typename std::enable_if<
                         cereal::traits::is_input_serializable<T, Archive>::value&&
                         !std::is_pod<T>::value
                         >::type>
{
    static void Serialize(const T& t, Archive& ar) {
        // Create an output archive
        serialization_cereal::ThrillOutputArchive<Archive> oarchive(ar);
        // Write the data to the archive
        oarchive(t);
    }

    static T Deserialize(Archive& ar) {
        // Create an output archive
        serialization_cereal::ThrillInputArchive<Archive> iarchive(ar);
        // Read the data from the archive
        T res;
        iarchive(res);
        return res;
    }
    static const bool   is_fixed_size = false;
    static const size_t fixed_size = 0;
};

//! \}

} // namespace data
} // namespace thrill

// register archives for polymorphic support
// CEREAL_REGISTER_ARCHIVE(thrill::data::ThrillOutputArchive)
// CEREAL_REGISTER_ARCHIVE(thrill::data::ThrillInputArchive)

// tie input and output archives together
// CEREAL_SETUP_ARCHIVE_TRAITS(thrill::data::ThrillInputArchive,
//                             thrill::data::ThrillOutputArchive)

#endif // !THRILL_DATA_SERIALIZATION_CEREAL_HEADER

/******************************************************************************/
