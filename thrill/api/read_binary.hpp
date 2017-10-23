/*******************************************************************************
 * thrill/api/read_binary.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_READ_BINARY_HEADER
#define THRILL_API_READ_BINARY_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/item_serialization_tools.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/io/syscall_file.hpp>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/vfs/file_io.hpp>

#include <tlx/string/join.hpp>

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs a line-based Read operation. Read reads a file from
 * the file system and emits it as a DIA.
 *
 * \ingroup api_layer
 */
template <typename ValueType>
class ReadBinaryNode final : public SourceNode<ValueType>
{
    static constexpr bool debug = false;

    //! for testing old method of pushing items instead of PushFile().
    static constexpr bool debug_no_extfile = false;

private:
    class VfsFileBlockSource;

public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    using VfsFileBlockReader = data::BlockReader<VfsFileBlockSource>;

    //! flag whether ValueType is fixed size
    static constexpr bool is_fixed_size_ =
        data::Serialization<VfsFileBlockReader, ValueType>::is_fixed_size;

    //! fixed size of ValueType or zero.
    static constexpr size_t fixed_size_ =
        data::Serialization<VfsFileBlockReader, ValueType>::fixed_size;

    //! structure to store info on what to read from files
    struct FileInfo {
        std::string   path;
        //! begin and end offsets in file.
        common::Range range;
        //! whether file is compressed
        bool          is_compressed;
    };

    //! sentinel to disable size limit
    static constexpr uint64_t no_size_limit_ =
        std::numeric_limits<uint64_t>::max();

    ReadBinaryNode(Context& ctx, const std::vector<std::string>& globlist,
                   uint64_t size_limit, bool local_storage)
        : Super(ctx, "ReadBinary") {

        vfs::FileList files = vfs::Glob(globlist, vfs::GlobType::File);

        if (files.size() == 0)
            die("ReadBinary: no files found in globs: " + tlx::join(' ', globlist));

        if (size_limit != no_size_limit_)
            files.total_size = std::min(files.total_size, size_limit);

        if (is_fixed_size_ && !files.contains_compressed)
        {
            // use fixed_size information to split binary files.

            // check that files have acceptable sizes
            for (size_t i = 0; i < files.size(); ++i) {
                if (files[i].size % fixed_size_ == 0) continue;

                die("ReadBinary: path " + files[i].path +
                    " size is not a multiple of " << size_t(fixed_size_));
            }

            common::Range my_range;

            if (local_storage) {
                my_range = context_.CalculateLocalRangeOnHost(
                    files.total_size / fixed_size_);
            }
            else {
                my_range = context_.CalculateLocalRange(
                    files.total_size / fixed_size_);
            }

            my_range.begin *= fixed_size_;
            my_range.end *= fixed_size_;

            sLOG << "ReadBinaryNode:" << ctx.num_workers()
                 << "my_range" << my_range;

            size_t i = 0;
            while (i < files.size() &&
                   files[i].size_inc_psum() <= my_range.begin) {
                i++;
            }

            for ( ; i < files.size() &&
                  files.size_ex_psum(i) <= my_range.end; ++i) {

                size_t file_begin = files.size_ex_psum(i);
                size_t file_end = files.size_inc_psum(i);
                size_t file_size = files[i].size;

                FileInfo fi;
                fi.path = files[i].path;
                fi.range = common::Range(
                    my_range.begin <= file_begin ? 0 : my_range.begin - file_begin,
                    my_range.end >= file_end ? file_size : my_range.end - file_begin);
                fi.is_compressed = false;

                sLOG << "ReadBinary: fileinfo"
                     << "path" << fi.path << "range" << fi.range;

                if (fi.range.begin == fi.range.end) continue;

                if (files.contains_remote_uri || debug_no_extfile) {
                    // push file and range into file list for remote files
                    // (these cannot be mapped using the io layer)
                    my_files_.push_back(fi);
                }
                else {
                    // new method: map blocks into a File using io layer

                    io::FileBasePtr file(
                        new io::SyscallFile(
                            fi.path,
                            io::FileBase::RDONLY | io::FileBase::NO_LOCK));

                    size_t item_off = 0;

                    for (size_t off = fi.range.begin; off < fi.range.end;
                         off += data::default_block_size) {

                        size_t bsize = std::min(
                            off + data::default_block_size, fi.range.end) - off;

                        data::ByteBlockPtr bbp =
                            context_.block_pool().MapExternalBlock(
                                file, off, bsize);

                        size_t item_num =
                            (bsize - item_off + fixed_size_ - 1) / fixed_size_;

                        data::Block block(
                            std::move(bbp), 0, bsize, item_off, item_num,
                            /* typecode_verify */ false);

                        item_off += item_num * fixed_size_ - bsize;

                        LOG << "ReadBinary: adding Block " << block;
                        ext_file_.AppendBlock(std::move(block));
                    }

                    use_ext_file_ = true;
                }
            }
        }
        else
        {
            // split filelist by whole files.
            size_t i = 0;

            common::Range my_range;

            if (local_storage) {
                my_range = context_.CalculateLocalRangeOnHost(
                    files.total_size);
            }
            else {
                my_range = context_.CalculateLocalRange(files.total_size);
            }

            while (i < files.size() &&
                   files[i].size_inc_psum() <= my_range.begin) {
                i++;
            }

            while (i < files.size() &&
                   files[i].size_inc_psum() <= my_range.end) {
                my_files_.push_back(
                    FileInfo { files[i].path,
                               common::Range(0, std::numeric_limits<size_t>::max()),
                               files[i].IsCompressed() });
                i++;
            }

            sLOG << "ReadBinary:" << my_files_.size() << "files,"
                 << "my_range" << my_range;
        }
    }

    ReadBinaryNode(Context& ctx, const std::string& glob, uint64_t size_limit,
                   bool local_storage)
        : ReadBinaryNode(ctx, std::vector<std::string>{ glob }, size_limit,
                         local_storage) { }

    void PushData(bool consume) final {
        LOG << "ReadBinaryNode::PushData() start " << *this
            << " consume=" << consume
            << " use_ext_file_=" << use_ext_file_;

        if (use_ext_file_)
            return this->PushFile(ext_file_, consume);

        // Hook Read
        for (const FileInfo& file : my_files_) {
            LOG << "ReadBinaryNode::PushData() opening " << file.path;

            VfsFileBlockReader br(
                VfsFileBlockSource(file, context_,
                                   stats_total_bytes, stats_total_reads));

            while (br.HasNext()) {
                this->PushItem(br.template NextNoSelfVerify<ValueType>());
            }
        }

        Super::logger_
            << "class" << "ReadBinaryNode"
            << "event" << "done"
            << "total_bytes" << stats_total_bytes
            << "total_reads" << stats_total_reads;
    }

    void Dispose() final {
        std::vector<FileInfo>().swap(my_files_);
        ext_file_.Clear();
    }

private:
    //! list of files for non-mapped File push
    std::vector<FileInfo> my_files_;

    //! File containing Blocks mapped directly to a io fileimpl.
    bool use_ext_file_ = false;
    data::File ext_file_ { context_.GetFile(this) };

    size_t stats_total_bytes = 0;
    size_t stats_total_reads = 0;

    class VfsFileBlockSource
    {
    public:
        const size_t block_size = data::default_block_size;

        VfsFileBlockSource(const FileInfo& fileinfo,
                           Context& ctx,
                           size_t& stats_total_bytes,
                           size_t& stats_total_reads)
            : context_(ctx),
              remain_size_(fileinfo.range.size()),
              is_compressed_(fileinfo.is_compressed),
              stats_total_bytes_(stats_total_bytes),
              stats_total_reads_(stats_total_reads) {
            // open file
            if (!is_compressed_) {
                stream_ = vfs::OpenReadStream(fileinfo.path, fileinfo.range);
            }
            else {
                stream_ = vfs::OpenReadStream(fileinfo.path);
            }
        }

        data::PinnedBlock NextBlock() {
            if (done_ || remain_size_ == 0)
                return data::PinnedBlock();

            data::PinnedByteBlockPtr bytes
                = context_.block_pool().AllocateByteBlock(
                block_size, context_.local_worker_id());

            size_t rb = is_compressed_
                        ? block_size : std::min(block_size, remain_size_);

            ssize_t size = stream_->read(bytes->data(), rb);
            stats_total_bytes_ += size;
            stats_total_reads_++;

            LOG << "VfsFileBlockSource::NextBlock() size " << size;

            if (size > 0) {
                if (!is_compressed_) {
                    assert(remain_size_ >= rb);
                    remain_size_ -= rb;
                }
                return data::PinnedBlock(std::move(bytes), 0, size, 0, 0,
                                         /* typecode_verify */ false);
            }
            else if (size < 0) {
                throw common::ErrnoException("Error reading vfs file");
            }
            else {
                // size == 0 -> read finished
                stream_->close();
                done_ = true;
                return data::PinnedBlock();
            }
        }

    private:
        Context& context_;
        vfs::ReadStreamPtr stream_;
        size_t remain_size_;
        bool is_compressed_;
        size_t& stats_total_bytes_;
        size_t& stats_total_reads_;
        bool done_ = false;
    };
};

/*!
 * ReadBinary is a DOp, which reads a file written by WriteBinary from the file
 * system and creates a DIA.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 * \param size_limit Optional limit to the total file size (e.g. for testing
 * algorithms on prefixes)
 *
 * \ingroup dia_sources
 */
template <typename ValueType>
DIA<ValueType> ReadBinary(
    Context& ctx, const std::vector<std::string>& filepath,
    uint64_t size_limit = ReadBinaryNode<ValueType>::no_size_limit_) {

    auto node = tlx::make_counting<ReadBinaryNode<ValueType> >(
        ctx, filepath, size_limit, /* local_storage */ false);

    return DIA<ValueType>(node);
}

template <typename ValueType>
DIA<ValueType> ReadBinary(
    struct LocalStorageTag, Context& ctx,
    const std::vector<std::string>& filepath,
    uint64_t size_limit = ReadBinaryNode<ValueType>::no_size_limit_) {

    auto node = tlx::make_counting<ReadBinaryNode<ValueType> >(
        ctx, filepath, size_limit, /* local_storage */ true);

    return DIA<ValueType>(node);
}

/*!
 * ReadBinary is a DOp, which reads a file written by WriteBinary from the file
 * system and creates a DIA.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 * \param size_limit Optional limit to the total file size (e.g. for testing
 * algorithms on prefixes)
 *
 * \ingroup dia_sources
 */
template <typename ValueType>
DIA<ValueType> ReadBinary(
    Context& ctx, const std::string& filepath,
    uint64_t size_limit = ReadBinaryNode<ValueType>::no_size_limit_) {

    auto node = tlx::make_counting<ReadBinaryNode<ValueType> >(
        ctx, filepath, size_limit, /* local_storage */ false);

    return DIA<ValueType>(node);
}

template <typename ValueType>
DIA<ValueType> ReadBinary(
    struct LocalStorageTag, Context& ctx, const std::string& filepath,
    uint64_t size_limit = ReadBinaryNode<ValueType>::no_size_limit_) {

    auto node = tlx::make_counting<ReadBinaryNode<ValueType> >(
        ctx, filepath, size_limit, /* local_storage */ true);

    return DIA<ValueType>(node);
}

} // namespace api

//! imported from api namespace
using api::ReadBinary;

} // namespace thrill

#endif // !THRILL_API_READ_BINARY_HEADER

/******************************************************************************/
