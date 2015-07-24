/*******************************************************************************
 * c7a/data/serializer_cereal_archive.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER
#define C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER
/*! \file binary.hpp
    \brief Binary input and output archives */
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
#ifndef CEREAL_ARCHIVES_C7A_HPP_
#define CEREAL_ARCHIVES_C7A_HPP_

#include "cereal/cereal.hpp"
#include <c7a/common/string.hpp>
#include <iostream>
#include <sstream>

// namespace c7a {
// namespace data {
//   template <typename BlockSink>
//   class BlockWriter : public common::ItemWriterToolsBase<BlockWriter<BlockSink> >;
// }}

namespace cereal {

// ######################################################################
//! An output archive designed to save data in a compact binary representation
/*! This archive outputs data to a stream in an extremely compact binary
    representation with as little extra metadata as possible.

    This archive does nothing to ensure that the endianness of the saved
    and loaded data is the same

    When using a binary archive and a file stream, you must use the
    std::ios::binary format flag to avoid having your data altered
    inadvertently.

    \ingroup Archives */
template <typename WriterArchive>
class c7aOutputArchive : public OutputArchive<c7aOutputArchive<WriterArchive>, AllowEmptyClassElision>
{
public:
    //! Construct, outputting to the provided stream
    /*! @param stream The stream to output to.  Can be a stringstream, a file stream, or
                      even cout! */
    c7aOutputArchive(WriterArchive& a) :
        OutputArchive<c7aOutputArchive, AllowEmptyClassElision>(this), a_(a)
    { }

    //! Writes size bytes of data to the output stream
    void saveBinary(const void* data, std::size_t size) {
        a_.Append(data, size);
    }

private:
    WriterArchive& a_;
};

// ######################################################################
//! An in
// put archive designed to load data saved using c7aOutputArchive
// This archive does nothing to ensure that the endianness of the saved
//   and loaded data is the same.  If you need to have portability over
//   architectures with different endianness, use Portablec7aOutputArchive.

//   When using a binary archive and a file stream, you must use the
//   std::ios::binary format flag to avoid having your data altered
//   inadvertently.

//   \ingroup Archives

template <typename ReaderArchive>
class c7aInputArchive : public InputArchive<c7aInputArchive<ReaderArchive>, AllowEmptyClassElision>
{
public:
    //! Construct, loading from the provided stream
    c7aInputArchive(ReaderArchive& a) :
        InputArchive<c7aInputArchive, AllowEmptyClassElision>(this), a_(a)
    { }

    //! Reads size bytes of data from the input stream
    void loadBinary(void* const data, std::size_t size) {
        a_.Read(data, size);
    }

private:
    ReaderArchive& a_;
};

// ######################################################################
// Common BinaryArchive serialization functions

//! Saving for POD types to binary
template <class T, typename WriterArchive>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_SAVE_FUNCTION_NAME(c7aOutputArchive<WriterArchive>& ar, T const& t) {
    ar.saveBinary(std::addressof(t), sizeof(t));
}

//! Loading for POD types from binary
template <class T, typename ReaderArchive>
inline
typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_LOAD_FUNCTION_NAME(c7aInputArchive<ReaderArchive>& ar, T& t) {
    ar.loadBinary(std::addressof(t), sizeof(t));
}

//! Serializing NVP types to binary
template <class Archive, class T, typename ReaderArchive, typename WriterArchive>
inline
CEREAL_ARCHIVE_RESTRICT(c7aInputArchive<ReaderArchive>, c7aOutputArchive<WriterArchive>)
CEREAL_SERIALIZE_FUNCTION_NAME(Archive & ar, NameValuePair<T>&t)
{
    ar(t.value);
}

//! Serializing SizeTags to binary
template <class Archive, class T, typename ReaderArchive, typename WriterArchive>
inline
CEREAL_ARCHIVE_RESTRICT(c7aInputArchive<ReaderArchive>, c7aOutputArchive<WriterArchive>)
CEREAL_SERIALIZE_FUNCTION_NAME(Archive & ar, SizeTag<T>&t)
{
    ar(t.size);
}

//! Saving binary data
template <class T, typename WriterArchive>
inline
void CEREAL_SAVE_FUNCTION_NAME(c7aOutputArchive<WriterArchive>& ar, BinaryData<T> const& bd) {
    ar.saveBinary(bd.data, static_cast<std::size_t>(bd.size));
}

//! Loading binary data
template <class T, typename ReaderArchive>
inline
void CEREAL_LOAD_FUNCTION_NAME(c7aInputArchive<ReaderArchive>& ar, BinaryData<T>& bd) {
    ar.loadBinary(bd.data, static_cast<std::size_t>(bd.size));
}
#endif // !C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER

} // namespace cereal

#endif // !C7A_DATA_SERIALIZER_CEREAL_ARCHIVE_HEADER

/******************************************************************************/
