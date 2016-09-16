/*******************************************************************************
 * tbt/tools/zip_stream_fwd.hpp
 *
 * Forward declarations for zip_stream.hpp
 ******************************************************************************/

#ifndef TBT_TOOLS_ZIP_STREAM_FWD_HEADER
#define TBT_TOOLS_ZIP_STREAM_FWD_HEADER

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

#endif // !TBT_TOOLS_ZIP_STREAM_FWD_HEADER

/******************************************************************************/
