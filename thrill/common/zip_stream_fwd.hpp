/*******************************************************************************
 * thrill/common/zip_stream_fwd.hpp
 *
 * An on-the-fly gzip and zlib ostream-compatible stream decompressor.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_ZIP_STREAM_FWD_HEADER
#define THRILL_COMMON_ZIP_STREAM_FWD_HEADER

#include <string>

namespace thrill {
namespace common {

template <class CharT,
          class Traits = std::char_traits<CharT> >
class basic_zip_ostream;

template <class CharT,
          class Traits = std::char_traits<CharT> >
class basic_zip_istream;

//! A typedef for basic_zip_ostream<char>
using zip_ostream = basic_zip_ostream<char>;
//! A typedef for basic_zip_istream<char>
using zip_istream = basic_zip_istream<char>;

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_ZIP_STREAM_FWD_HEADER

/******************************************************************************/
