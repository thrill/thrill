/*******************************************************************************
 * c7a/data/block_reader.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_READER_HEADER
#define C7A_DATA_BLOCK_READER_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/serializer.hpp>
#include <c7a/common/config.hpp>
#include <c7a/common/item_serializer_tools.hpp>

#include <algorithm>
#include <string>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

template <typename BlockSource>
class BlockReader
    : public common::ItemReaderToolsBase<BlockReader<BlockSource> >
{
public:
    static const bool self_verify = common::g_self_verify;

    using Byte = unsigned char;

    using Block = typename BlockSource::Block;
    using BlockCPtr = std::shared_ptr<const Block>;

    //! Start reading a File
    explicit BlockReader(BlockSource&& source)
        : source_(std::move(source)) { }

    //! \name Reading (Generic) Items
    //! \{

    //! Next() reads a complete item T
    template <typename T>
    T Next() {
        if (self_verify) {
            // for self-verification, T is prefixed with its hash code
            size_t code = Get<size_t>();
            if (code != typeid(T).hash_code()) {
                throw std::runtime_error(
                          "BlockReader::Next() attempted to retrieve item "
                          "with different typeid!");
            }
        }
        return Serializer<BlockReader, T>::deserialize(*this);
    }

    //! HasNext() returns true if at least one more byte is available.
    bool HasNext() {
        while (current_ == end_) {
            if (!NextBlock())
                return false;
        }
        return true;
    }

    //! \}

    //! \name Cursor Reading Methods
    //! \{

    //! Fetch a number of unstructured bytes from the current block, advancing
    //! the cursor.
    BlockReader & Read(void* outdata, size_t size) {

        Byte* cdata = reinterpret_cast<Byte*>(outdata);

        while (current_ + size > end_) {
            // partial copy of remainder of block
            size_t partial_size = end_ - current_;
            std::copy(current_, current_ + partial_size, cdata);

            cdata += partial_size;
            size -= partial_size;

            if (!NextBlock())
                throw std::runtime_error("Data underflow in BlockReader.");
        }

        // copy rest from current block
        std::copy(current_, current_ + size, cdata);
        current_ += size;

        return *this;
    }

    //! Fetch a number of unstructured bytes from the buffer as std::string,
    //! advancing the cursor.
    std::string Read(size_t datalen) {
        std::string out(datalen, 0);
        Read(const_cast<char*>(out.data()), out.size());
        return out;
    }

    //! Fetch a single byte from the current block, advancing the cursor.
    Byte GetByte() {
        // loop, since blocks can actually be empty.
        while (current_ == end_) {
            if (!NextBlock())
                throw std::runtime_error("Data underflow in BlockReader.");
        }
        return *current_++;
    }

    //! Fetch a single item of the template type Type from the buffer,
    //! advancing the cursor. Be careful with implicit type conversions!
    template <typename Type>
    Type Get() {
        static_assert(std::is_pod<Type>::value,
                      "You only want to Get() POD types as raw values.");

        Type ret;
        Read(&ret, sizeof(ret));
        return ret;
    }

    //! \}

protected:
    //! Instance of BlockSource. This is NOT a reference, as to enable embedding
    //! of FileBlockSource to compose classes into File::Reader.
    BlockSource source_;

    //! current read pointer into current block of file.
    const Byte* current_ = nullptr;

    //! pointer to end of current block.
    const Byte* end_ = nullptr;

    //! Call source_.NextBlock with appropriate parameters
    bool NextBlock() {
        return source_.NextBlock(&current_, &end_);
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_READER_HEADER

/******************************************************************************/
