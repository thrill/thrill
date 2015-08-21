/*******************************************************************************
 * thrill/common/item_serialization_tools.hpp
 *
 * Abstract methods common to many serializer and deserializers: serialize
 * Varint (7-bit encoding), and Strings by prefixing them with their length.
 * Included by BlockWriter and BinaryBufferBuilder via CRTP.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_ITEM_SERIALIZATION_TOOLS_HEADER
#define THRILL_COMMON_ITEM_SERIALIZATION_TOOLS_HEADER

#include <cstdint>
#include <stdexcept>
#include <string>

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_LIKELY(c)   __builtin_expect((c), 1)
#define THRILL_UNLIKELY(c) __builtin_expect((c), 0)
#else
#define THRILL_LIKELY(c)   c
#define THRILL_UNLIKELY(c) c
#endif

namespace thrill {
namespace common {

/*!
 * CRTP class to enhance item/memory writer classes with Varint encoding and
 * String encoding.
 */
template <typename Writer>
class ItemWriterToolsBase
{
public:
    //! Append a varint to the writer.
    Writer & PutVarint32(uint32_t v) {
        Writer& w = *static_cast<Writer*>(this);

        if (v < 128) {
            w.PutByte(uint8_t(v));
        }
        else if (v < 128 * 128) {
            w.PutByte(uint8_t(((v >> 0) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 7) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 0) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 7) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 0) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 7) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 21) & 0x7F));
        }
        else {
            w.PutByte(uint8_t(((v >> 0) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 7) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 28) & 0x7F));
        }

        return w;
    }

    //! Append a varint to the writer.
    Writer & PutVarint(uint64_t v) {
        Writer& w = *static_cast<Writer*>(this);

        if (v < 128) {
            w.PutByte(uint8_t(v));
        }
        else if (v < 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 07) & 0x7F));
        }
        else if (v < 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 14) & 0x7F));
        }
        else if (v < 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 21) & 0x7F));
        }
        else if (v < 128llu * 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 28) & 0x7F));
        }
        else if (v < 128llu * 128 * 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 28) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 35) & 0x7F));
        }
        else if (v < 128llu * 128 * 128 * 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 28) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 35) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 42) & 0x7F));
        }
        else if (v < 128llu * 128 * 128 * 128 * 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 28) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 35) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 42) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 49) & 0x7F));
        }
        else if (v < 128llu * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128) {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 28) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 35) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 42) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 49) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 56) & 0x7F));
        }
        else {
            w.PutByte(uint8_t(((v >> 00) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 07) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 14) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 21) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 28) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 35) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 42) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 49) & 0x7F) | 0x80));
            w.PutByte(uint8_t(((v >> 56) & 0x7F) | 0x80));
            w.PutByte(uint8_t((v >> 63) & 0x7F));
        }

        return w;
    }

    //! Put a string by saving it's length followed by the data itself.
    Writer & PutString(const char* data, size_t len) {
        return PutVarint(len).Append(data, len);
    }

    //! Put a string by saving it's length followed by the data itself.
    Writer & PutString(const uint8_t* data, size_t len) {
        return PutVarint(len).Append(data, len);
    }

    //! Put a string by saving it's length followed by the data itself.
    Writer & PutString(const std::string& str) {
        return PutString(str.data(), str.size());
    }
};

/*!
 * CRTP class to enhance item/memory reader classes with Varint decoding and
 * String decoding.
 */
template <typename Reader>
class ItemReaderToolsBase
{
public:
    //! Fetch a varint with up to 32-bit from the reader at the cursor.
    uint32_t GetVarint32() {
        Reader& r = *static_cast<Reader*>(this);

        uint32_t u, v = r.GetByte();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = r.GetByte(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = r.GetByte();
        if (u & 0xF0)
            throw std::overflow_error("Overflow during varint decoding.");
        v |= (u & 0x7F) << 28;
        return v;
    }

    //! Fetch a 64-bit varint from the reader at the cursor.
    uint64_t GetVarint() {
        Reader& r = *static_cast<Reader*>(this);

        uint64_t u, v = r.GetByte();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = r.GetByte(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 28;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 35;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 42;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 49;
        if (!(u & 0x80)) return v;
        u = r.GetByte(), v |= (u & 0x7F) << 56;
        if (!(u & 0x80)) return v;
        u = r.GetByte();
        if (u & 0xFE)
            throw std::overflow_error("Overflow during varint64 decoding.");
        v |= (u & 0x7F) << 63;
        return v;
    }

    //! Fetch a string which was Put via Put_string().
    std::string GetString() {
        Reader& r = *static_cast<Reader*>(this);
        return r.Read(GetVarint());
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_ITEM_SERIALIZATION_TOOLS_HEADER

/******************************************************************************/
