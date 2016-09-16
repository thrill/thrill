/*******************************************************************************
 * tbt/tools/zip_stream.cpp
 *
 *
 ******************************************************************************/

/*
zipstream Library License:
--------------------------

The zlib/libpng License Copyright (c) 2003 Jonathan de Halleux.

This software is provided 'as-is', without any express or implied warranty. In
no event will the authors be held liable for any damages arising from the use
of this software.

Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it freely,
subject to the following restrictions:

1. The origin of this software must not be misrepresented; you must not claim
   that you wrote the original software. If you use this software in a
   product, an acknowledgment in the product documentation would be
   appreciated but is not required.

2. Altered source versions must be plainly marked as such, and must not be
   misrepresented as being the original software.

3. This notice may not be removed or altered from any source distribution

Author: Jonathan de Halleux, dehalleux@pelikhan.com, 2003

Altered by: Andreas Zieringer 2003 for OpenSG project
            made it platform independent, gzip conform, fixed gzip footer

Altered by: Geoffrey Hutchison 2005 for Open Babel project
            minor namespace modifications, VC++ compatibility

Altered by: Mathieu Malaterre 2008, for GDCM project
            when reading deflate bit stream in DICOM special handling of \0 is needed
            also when writing deflate back to disk, the add_footer must be called
*/

#include <thrill/common/logger.hpp>
#include <thrill/common/zip_stream.hpp>

#include <cassert>
#include <cstring>

#ifdef WIN32 /* Window 95 & Windows NT */
#  define OS_CODE  0x0b
#endif
#if defined(MACOS) || defined(TARGET_OS_MAC)
#  define OS_CODE  0x07
#endif
#ifndef OS_CODE
#  define OS_CODE  0x03  /* assume Unix */
#endif

namespace thrill {
namespace common {

static constexpr bool debug = false;

/******************************************************************************/

static const int gz_magic[2] = { 0x1f, 0x8b }; /* gzip magic header */

/* gzip flag byte */
static const int gz_ascii_flag = 0x01;         /* bit 0 set: file probably ascii text */
static const int gz_head_crc = 0x02;           /* bit 1 set: header CRC present */
static const int gz_extra_field = 0x04;        /* bit 2 set: extra field present */
static const int gz_orig_name = 0x08;          /* bit 3 set: original file name present */
static const int gz_comment = 0x10;            /* bit 4 set: file comment present */
static const int gz_reserved = 0xE0;           /* bits 5..7: reserved */

/******************************************************************************/
// basic_zip_streambuf

template <typename CharT, typename Traits>
basic_zip_streambuf<CharT, Traits>::basic_zip_streambuf(
    ostream_reference ostream,
    int level, ZipStrategy strategy,
    int window_size, int memory_level, size_t buffer_size)
    : ostream_(ostream),
      output_buffer_(buffer_size, 0),
      buffer_(buffer_size, 0),
      crc_(0)
{
    zip_stream_.zalloc = (alloc_func)0;
    zip_stream_.zfree = (free_func)0;

    zip_stream_.next_in = NULL;
    zip_stream_.avail_in = 0;
    zip_stream_.next_out = NULL;
    zip_stream_.avail_out = 0;

    if (level > 9) level = 9;
    if (memory_level > 9) memory_level = 9;

    err_ = deflateInit2(&zip_stream_, level, Z_DEFLATED,
                        window_size, memory_level,
                        static_cast<int>(strategy));

    // assign buffer
    this->setp(buffer_.data(), buffer_.data() + buffer_.size() - 1);
}

template <typename CharT, typename Traits>
basic_zip_streambuf<CharT, Traits>::~basic_zip_streambuf()
{
    flush();
    err_ = deflateEnd(&zip_stream_);

    LOG << "~basic_zip_streambuf() total=" << ostream_.tellp();
}

template <typename CharT, typename Traits>
int
basic_zip_streambuf<CharT, Traits>::sync()
{
    size_t size = static_cast<size_t>(this->pptr() - this->pbase());

    LOG << "basic_zip_streambuf::sync()"
        << " size=" << size;

    if (!zip_to_stream(this->pbase(), size))
        return -1;

    return 0;
}

template <typename CharT, typename Traits>
typename basic_zip_streambuf<CharT, Traits>::int_type
basic_zip_streambuf<CharT, Traits>::overflow(int_type c)
{
    LOG << "basic_zip_streambuf::overflow() c=" << c;

    size_t size = static_cast<size_t>(this->pptr() - this->pbase());
    if (c != Traits::eof())
    {
        *this->pptr() = static_cast<char>(c);
        ++size;
    }
    if (zip_to_stream(this->pbase(), size))
    {
        this->setp(this->pbase(), this->epptr());
        return c;
    }
    else
    {
        return Traits::eof();
    }
}

template <typename CharT, typename Traits>
std::streamsize
basic_zip_streambuf<CharT, Traits>::flush()
{
    LOG << "basic_zip_streambuf::flush()";

    std::streamsize written_byte_size = 0, total_written_byte_size = 0;

    // updating crc
    crc_ = static_cast<uint32_t>(
        crc32(crc_, zip_stream_.next_in, zip_stream_.avail_in));

    do
    {
        err_ = deflate(&zip_stream_, Z_FINISH);
        if (err_ == Z_OK || err_ == Z_STREAM_END)
        {
            written_byte_size = static_cast<std::streamsize>(
                output_buffer_.size()) - zip_stream_.avail_out;

            total_written_byte_size += written_byte_size;
            // ouput buffer is full, dumping to ostream
            ostream_.write(
                reinterpret_cast<const char*>(output_buffer_.data()),
                static_cast<std::streamsize>(written_byte_size));

            zip_stream_.avail_out = static_cast<uInt>(output_buffer_.size());
            zip_stream_.next_out = output_buffer_.data();
        }
    }
    while (err_ == Z_OK); // NOLINT

    ostream_.flush();

    return total_written_byte_size;
}

template <typename CharT, typename Traits>
typename basic_zip_streambuf<CharT, Traits>::ostream_reference
basic_zip_streambuf<CharT, Traits>::get_ostream() const
{
    return ostream_;
}

template <typename CharT, typename Traits>
int basic_zip_streambuf<CharT, Traits>::get_zerr() const
{
    return err_;
}

template <typename CharT, typename Traits>
uint32_t
basic_zip_streambuf<CharT, Traits>::get_crc() const
{
    return crc_;
}

template <typename CharT, typename Traits>
uint32_t
basic_zip_streambuf<CharT, Traits>::get_in_size() const
{
    return static_cast<uint32_t>(zip_stream_.total_in);
}

template <typename CharT, typename Traits>
unsigned long
basic_zip_streambuf<CharT, Traits>::get_out_size() const
{
    return zip_stream_.total_out;
}

template <typename CharT, typename Traits>
bool
basic_zip_streambuf<CharT, Traits>::zip_to_stream(
    char* buffer, std::streamsize buffer_size)
{
    LOG << "basic_zip_streambuf::zip_to_stream()"
        << " buffer_size=" << buffer_size;

    std::streamsize written_byte_size = 0, total_written_byte_size = 0;

    zip_stream_.next_in = reinterpret_cast<byte_type*>(buffer);
    zip_stream_.avail_in = static_cast<uInt>(buffer_size);
    zip_stream_.next_out = output_buffer_.data();
    zip_stream_.avail_out = static_cast<uInt>(output_buffer_.size());

    // update crc
    crc_ = static_cast<uint32_t>(
        crc32(crc_, zip_stream_.next_in, zip_stream_.avail_in));

    do
    {
        err_ = deflate(&zip_stream_, Z_NO_FLUSH);

        if (err_ == Z_OK || err_ == Z_STREAM_END)
        {
            written_byte_size =
                static_cast<std::streamsize>(output_buffer_.size()) -
                zip_stream_.avail_out;
            total_written_byte_size += written_byte_size;

            // output buffer is full, dumping to ostream
            ostream_.write(
                reinterpret_cast<const char*>(output_buffer_.data()),
                static_cast<std::streamsize>(written_byte_size));

            zip_stream_.avail_out = static_cast<uInt>(output_buffer_.size());
            zip_stream_.next_out = output_buffer_.data();
        }
    }
    while (zip_stream_.avail_in != 0 && err_ == Z_OK); // NOLINT

    return (err_ == Z_OK);
}

/******************************************************************************/
// basic_unzip_streambuf

template <typename CharT, typename Traits>
basic_unzip_streambuf<CharT, Traits>::basic_unzip_streambuf(
    istream_reference istream, int window_size,
    size_t read_buffer_size, size_t input_buffer_size)
    : istream_(istream),
      input_buffer_(input_buffer_size),
      buffer_(read_buffer_size),
      crc_(0)
{
    // setting zalloc, zfree and opaque
    zip_stream_.zalloc = (alloc_func)nullptr;
    zip_stream_.zfree = (free_func)nullptr;

    zip_stream_.next_in = NULL;
    zip_stream_.avail_in = 0;
    zip_stream_.avail_out = 0;
    zip_stream_.next_out = NULL;

    err_ = inflateInit2(&zip_stream_, window_size);

    this->setg(buffer_.data() + 4,     // beginning of putback area
               buffer_.data() + 4,     // read position
               buffer_.data() + 4);    // end position
}

template <typename CharT, typename Traits>
basic_unzip_streambuf<CharT, Traits>::~basic_unzip_streambuf()
{
    inflateEnd(&zip_stream_);
}

template <typename CharT, typename Traits>
typename basic_unzip_streambuf<CharT, Traits>::int_type
basic_unzip_streambuf<CharT, Traits>::underflow()
{
    if (this->gptr() && (this->gptr() < this->egptr()))
        return *reinterpret_cast<unsigned char*>(this->gptr());

    int n_putback = static_cast<int>(this->gptr() - this->eback());
    if (n_putback > 4)
        n_putback = 4;

    memcpy(buffer_.data() + (4 - n_putback),
           this->gptr() - n_putback,
           n_putback * sizeof(char_type));

    std::streamsize num = unzip_from_stream(
        buffer_.data() + 4,
        static_cast<std::streamsize>((buffer_.size() - 4) * sizeof(char_type)));

    if (num <= 0)                                // ERROR or EOF
        return Traits::eof();

    // reset buffer pointers
    this->setg(buffer_.data() + (4 - n_putback), // beginning of putback area
               buffer_.data() + 4,               // read position
               buffer_.data() + 4 + num);        // end of buffer

    // return next character
    return *reinterpret_cast<unsigned char*>(this->gptr());
}

template <typename CharT, typename Traits>
typename basic_unzip_streambuf<CharT, Traits>::istream_reference
basic_unzip_streambuf<CharT, Traits>::get_istream()
{
    return istream_;
}

template <typename CharT, typename Traits>
z_stream&
basic_unzip_streambuf<CharT, Traits>::get_zip_stream()
{
    return zip_stream_;
}

template <typename CharT, typename Traits>
int
basic_unzip_streambuf<CharT, Traits>::get_zerr() const
{
    return err_;
}

template <typename CharT, typename Traits>
uint32_t
basic_unzip_streambuf<CharT, Traits>::get_crc() const
{
    return crc_;
}

template <typename CharT, typename Traits>
unsigned long
basic_unzip_streambuf<CharT, Traits>::get_out_size() const
{
    return zip_stream_.total_out;
}

template <typename CharT, typename Traits>
uint32_t
basic_unzip_streambuf<CharT, Traits>::get_in_size() const
{
    return static_cast<uint32_t>(zip_stream_.total_in);
}

template <typename CharT, typename Traits>
void
basic_unzip_streambuf<CharT, Traits>::put_back_from_zip_stream()
{
    if (zip_stream_.avail_in == 0)
        return;

    LOG << "basic_unzip_streambuf::put_back_from_zip_stream()"
        << " avail_in=" << zip_stream_.avail_in;

    istream_.clear(std::ios::goodbit);
    istream_.seekg(-static_cast<int>(zip_stream_.avail_in), std::ios_base::cur);

    zip_stream_.avail_in = 0;
}

template <typename CharT, typename Traits>
std::streamsize
basic_unzip_streambuf<CharT, Traits>::unzip_from_stream(
    char_type* buffer, std::streamsize buffer_size)
{
    zip_stream_.next_out = reinterpret_cast<byte_type*>(buffer);
    zip_stream_.avail_out = static_cast<uInt>(buffer_size * sizeof(char_type));
    size_t count = zip_stream_.avail_in;

    do
    {
        if (zip_stream_.avail_in == 0)
            count = fill_input_buffer();

        err_ = inflate(&zip_stream_, Z_SYNC_FLUSH);
    }
    while (err_ == Z_OK && zip_stream_.avail_out != 0 && count != 0); // NOLINT

    std::streamsize theSize =
        buffer_size -
        ((std::streamsize)zip_stream_.avail_out) / sizeof(char_type);
    // assert (theSize >= 0 && theSize < std::numeric_limits<uInt>::max());

    // updating crc
    crc_ = static_cast<uint32_t>(
        crc32(crc_, reinterpret_cast<byte_type*>(buffer), (uInt)theSize));

    std::streamsize n_read =
        buffer_size - zip_stream_.avail_out / sizeof(char_type);

    // check if it is the end
    if (err_ == Z_STREAM_END)
        put_back_from_zip_stream();

    return n_read;
}

template <typename CharT, typename Traits>
size_t
basic_unzip_streambuf<CharT, Traits>::fill_input_buffer()
{
    zip_stream_.next_in = input_buffer_.data();
    istream_.read(reinterpret_cast<char_type*>(input_buffer_.data()),
                  static_cast<std::streamsize>(input_buffer_.size() /
                                               sizeof(char_type)));

    std::streamsize nbytesread = istream_.gcount() * sizeof(char_type);
    LOG << "basic_unzip_streambuf::fill_input_buffer()"
        << " nbytesread=" << nbytesread;

    return (zip_stream_.avail_in = (uInt)nbytesread);
}

/******************************************************************************/
// basic_zip_ostream

template <typename CharT, typename Traits>
basic_zip_ostream<CharT, Traits>::basic_zip_ostream(
    ostream_reference ostream,
    ZipFormat format, int level, ZipStrategy strategy,
    int window_size, int memory_level, size_t buffer_size)
    : basic_zip_streambuf<CharT, Traits>(ostream, level, strategy, window_size,
                                         memory_level, buffer_size),
      std::basic_ostream<CharT, Traits>(this),
      format_(format),
      added_footer_(false)
{
    if (format == ZipFormat::GZip)
        add_header();
}

template <typename CharT, typename Traits>
basic_zip_ostream<CharT, Traits>::~basic_zip_ostream()
{
    finished();
}

template <typename CharT, typename Traits>
ZipFormat basic_zip_ostream<CharT, Traits>::format() const
{
    return format_;
}

template <typename CharT, typename Traits>
basic_zip_ostream<CharT, Traits>& basic_zip_ostream<CharT, Traits>::zflush()
{
    static_cast<std::basic_ostream<CharT, Traits>*>(this)->flush();
    static_cast<basic_zip_streambuf<CharT, Traits>*>(this)->flush();
    return *this;
}

template <typename CharT, typename Traits>
void basic_zip_ostream<CharT, Traits>::finished()
{
    if (format_ == ZipFormat::CrcFooter || format_ == ZipFormat::GZip)
        add_footer();
    else
        zflush();
}

template <typename CharT, typename Traits>
basic_zip_ostream<CharT, Traits>& basic_zip_ostream<CharT, Traits>::add_header()
{
    char_type zero = 0;

    this->get_ostream()
        << static_cast<char_type>(gz_magic[0])
        << static_cast<char_type>(gz_magic[1])
        << static_cast<char_type>(Z_DEFLATED)
        << zero                                         //flags
        << zero << zero << zero << zero                 // time
        << zero                                         //xflags
        << static_cast<char_type>(OS_CODE);

    return *this;
}

template <typename CharT, typename Traits>
basic_zip_ostream<CharT, Traits>& basic_zip_ostream<CharT, Traits>::add_footer()
{
    if (added_footer_)
        return *this;

    zflush();

    added_footer_ = true;

    // Writes crc and length in LSB order to the stream.
    uint32_t crc = this->get_crc();
    for (int n = 0; n < 4; ++n)
    {
        this->get_ostream().put(static_cast<char>(crc & 0xff));
        crc >>= 8;
    }

    uint32_t length = this->get_in_size();
    for (int m = 0; m < 4; ++m)
    {
        this->get_ostream().put(static_cast<char>(length & 0xff));
        length >>= 8;
    }

    return *this;
}

/******************************************************************************/
// basic_zip_istream

template <typename CharT, typename Traits>
basic_zip_istream<CharT, Traits>::basic_zip_istream(
    istream_reference istream,
    int window_size, size_t read_buffer_size, size_t input_buffer_size)
    : basic_unzip_streambuf<CharT, Traits>(
          istream, window_size,
          read_buffer_size, input_buffer_size),
      std::basic_istream<CharT, Traits>(this),
      is_gzip_(false),
      gzip_crc_(0),
      gzip_data_size_(0)
{
    if (this->get_zerr() == Z_OK)
    {
        int check = check_header();
        (void)check;
        //LOG << "check_header:" << check << std::endl;
    }
}

template <typename CharT, typename Traits>
bool
basic_zip_istream<CharT, Traits>::is_gzip() const
{
    return is_gzip_;
}

template <typename CharT, typename Traits>
bool
basic_zip_istream<CharT, Traits>::check_crc()
{
    read_footer();
    return this->get_crc() == gzip_crc_;
}

template <typename CharT, typename Traits>
bool
basic_zip_istream<CharT, Traits>::check_data_size() const
{
    return this->get_out_size() == gzip_data_size_;
}

template <typename CharT, typename Traits>
long
basic_zip_istream<CharT, Traits>::get_gzip_crc() const
{
    return gzip_crc_;
}

template <typename CharT, typename Traits>
long
basic_zip_istream<CharT, Traits>::get_gzip_data_size() const
{
    return gzip_data_size_;
}

template <typename CharT, typename Traits>
int
basic_zip_istream<CharT, Traits>::check_header()
{
    int method;    /* method byte */
    int flagsbyte; /* flags byte */
    uInt len;
    int c;
    int err = 0;
    z_stream& zip_stream = this->get_zip_stream();

    /* Check the gzip magic header */
    for (len = 0; len < 2; len++)
    {
        c = static_cast<int>(this->get_istream().get());
        if (c != gz_magic[len])
        {
            if (len != 0)
                this->get_istream().unget();
            if (c != Traits::eof())
            {
                this->get_istream().unget();
            }

            err = zip_stream.avail_in != 0 ? Z_OK : Z_STREAM_END;
            is_gzip_ = false;
            return err;
        }
    }

    is_gzip_ = true;
    method = static_cast<int>(this->get_istream().get());
    flagsbyte = static_cast<int>(this->get_istream().get());
    if (method != Z_DEFLATED || (flagsbyte & gz_reserved) != 0)
    {
        err = Z_DATA_ERROR;
        return err;
    }

    /* Discard time, xflags and OS code: */
    for (len = 0; len < 6; len++)
        this->get_istream().get();

    if ((flagsbyte & gz_extra_field) != 0)
    {
        /* skip the extra field */
        len = (uInt)this->get_istream().get();
        len += ((uInt)this->get_istream().get()) << 8;
        /* len is garbage if EOF but the loop below will quit anyway */
        while (len-- != 0 && this->get_istream().get() != Traits::eof()) { }
    }
    if ((flagsbyte & gz_orig_name) != 0)
    {
        /* skip the original file name */
        while ((c = this->get_istream().get()) != 0 && c != Traits::eof()) { }
    }
    if ((flagsbyte & gz_comment) != 0)
    {
        /* skip the .gz file comment */
        while ((c = this->get_istream().get()) != 0 && c != Traits::eof()) { }
    }
    if ((flagsbyte & gz_head_crc) != 0)
    {           /* skip the header crc */
        for (len = 0; len < 2; len++)
            this->get_istream().get();
    }
    err = this->get_istream().eof() ? Z_DATA_ERROR : Z_OK;

    return err;
}

template <typename CharT, typename Traits>
void
basic_zip_istream<CharT, Traits>::read_footer()
{
    if (is_gzip_ || /* compressor always adds footer? -tb */ 1)
    {
        gzip_crc_ = 0;
        for (int n = 0; n < 4; ++n)
            gzip_crc_ += (static_cast<uint32_t>(this->get_istream().get()) & 0xff) << (8 * n);

        gzip_data_size_ = 0;
        for (int n = 0; n < 4; ++n)
            gzip_data_size_ +=
                 (static_cast<uint32_t>(this->get_istream().get()) & 0xff) << (8 * n);
    }
}

/******************************************************************************/

//! Helper function to check whether stream is compressed or not.
bool isGZip(std::istream& is)
{
    const int gz_magic[2] = { 0x1f, 0x8b };

    int c1 = is.get();
    if (c1 != gz_magic[0])
    {
        is.putback(static_cast<char>(c1));
        return false;
    }

    int c2 = is.get();
    if (c2 != gz_magic[1])
    {
        is.putback(static_cast<char>(c2));
        is.putback(static_cast<char>(c1));
        return false;
    }

    is.putback(static_cast<char>(c2));
    is.putback(static_cast<char>(c1));
    return true;
}

/******************************************************************************/

template class basic_zip_streambuf<char>;
template class basic_unzip_streambuf<char>;

template class basic_zip_ostream<char>;
template class basic_zip_istream<char>;

// template class basic_zip_streambuf<wchar_t>;
// template class basic_unzip_streambuf<wchar_t>;

// template class basic_zip_ostream<wchar_t>;
// template class basic_zip_istream<wchar_t>;

} // namespace tools
} // namespace tbt

/******************************************************************************/
