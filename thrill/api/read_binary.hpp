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
#include <thrill/common/stat_logger.hpp>
#include <thrill/core/file_io.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a line-based Read operation. Read reads a file from
 * the file system and emits it as a DIA.
 */
template <typename ValueType>
class ReadBinaryNode final : public SourceNode<ValueType>
{
    static const bool debug = false;

public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    //! flag whether ValueType is fixed size
    static const bool is_fixed_size_ =
        data::Serialization<data::DynBlockWriter, ValueType>::is_fixed_size;

    //! fixed size of ValueType or zero.
    static const size_t fixed_size_ =
        data::Serialization<data::DynBlockWriter, ValueType>::fixed_size;

    //! structure to store info on what to read from files
    struct FileInfo {
        std::string path;
        //! begin and end offsets in file.
        size_t      begin, end;
        //! size of exert from file
        size_t      size() const { return end - begin; }
    };

    using SysFileInfo = core::SysFileInfo;

    ReadBinaryNode(Context& ctx,
                   const std::string& filepath,
                   StatsNode* stats_node)
        : Super(ctx, { }, stats_node)
    {
        core::SysFileList files = core::GlobFileSizePrefixSum(filepath);

        if (is_fixed_size_ && !files.contains_compressed)
        {
            // use fixed_size information to split binary files.

            // check that files have acceptable sizes
            for (size_t i = 0; i < files.count(); ++i) {
                if (files.list[i].size % fixed_size_ == 0) continue;

                die("ReadBinary() path " + files.list[i].path +
                    " size is not a multiple of " +
                    std::to_string(fixed_size_));
            }

            common::Range my_range =
                context_.CalculateLocalRange(files.total_size / fixed_size_);

            my_range.begin *= fixed_size_;
            my_range.end *= fixed_size_;

            sLOG << "ReadBinaryNode" << ctx.num_workers()
                 << "my_range" << my_range;

            size_t i = 0;
            while (i < files.count() &&
                   files.list[i].size_inc_psum() <= my_range.begin) {
                i++;
            }

            while (i < files.count() &&
                   files.list[i].size_ex_psum <= my_range.end) {

                size_t file_begin = files.list[i].size_ex_psum;
                size_t file_end = files.list[i].size_inc_psum();
                size_t file_size = files.list[i].size;

                FileInfo fi;
                fi.path = files.list[i].path;
                fi.begin = my_range.begin <= file_begin ? 0 : my_range.begin - file_begin;
                fi.end = my_range.end >= file_end ? file_size : my_range.end - file_begin;

                sLOG << "FileInfo"
                     << "path" << fi.path
                     << "begin" << fi.begin << "end" << fi.end;

                if (fi.begin != fi.end)
                    my_files_.push_back(fi);

                i++;
            }
        }
        else
        {
            // split filelist by whole files.
            size_t i = 0;

            common::Range my_range =
                context_.CalculateLocalRange(files.total_size);

            while (i < files.count() &&
                   files.list[i].size_inc_psum() <= my_range.begin) {
                i++;
            }

            while (i < files.count() &&
                   files.list[i].size_inc_psum() <= my_range.end) {
                my_files_.push_back(
                    FileInfo { files.list[i].path, 0,
                               std::numeric_limits<size_t>::max() });
                i++;
            }

            LOG << my_files_.size() << " files, my range " << my_range;
        }
    }

    void PushData(bool /* consume */) final {
        static const bool debug = false;
        LOG << "READING data " << std::to_string(this->id());

        // Hook Read
        for (const FileInfo& file : my_files_) {
            LOG << "OPENING FILE " << file.path;

            data::BlockReader<SysFileBlockSource> br(
                SysFileBlockSource(file, context_,
                                   stats_total_bytes, stats_total_reads));

            while (br.HasNext()) {
                this->PushItem(br.template NextNoSelfVerify<ValueType>());
            }
        }

        STATC << "NodeType" << "ReadBinary"
              << "TotalBytes" << stats_total_bytes
              << "TotalReads" << stats_total_reads;

        LOG << "DONE!";
    }

    void Dispose() final { }

private:
    std::vector<FileInfo> my_files_;

    size_t stats_total_bytes = 0;
    size_t stats_total_reads = 0;

    class SysFileBlockSource
    {
    public:
        const size_t block_size = data::default_block_size;

        SysFileBlockSource(const FileInfo& fileinfo,
                           Context& ctx,
                           size_t& stats_total_bytes,
                           size_t& stats_total_reads)
            : context_(ctx),
              sysfile_(core::SysFile::OpenForRead(fileinfo.path)),
              remain_size_(fileinfo.size()),
              stats_total_bytes_(stats_total_bytes),
              stats_total_reads_(stats_total_reads) {
            if (fileinfo.begin != 0) {
                // seek to beginning
                size_t p = sysfile_.lseek(fileinfo.begin);
                die_unequal(fileinfo.begin, p);
            }
        }

        data::PinnedBlock NextBlock() {
            if (done_) return data::PinnedBlock();

            data::PinnedByteBlockPtr bytes
                = context_.block_pool().AllocateByteBlock(block_size);

            size_t rb = std::min(block_size, remain_size_);

            ssize_t size = sysfile_.read(bytes->data(), rb);
            stats_total_bytes_ += size;
            stats_total_reads_++;

            if (size > 0) {
                remain_size_ -= rb;
                return data::PinnedBlock(std::move(bytes), 0, size, 0, 0);
            }
            else if (size < 0) {
                throw common::ErrnoException("File reading error");
            }
            else {
                // size == 0 -> read finished
                sysfile_.close();
                done_ = true;
                return data::PinnedBlock();
            }
        }

    private:
        Context& context_;
        core::SysFile sysfile_;
        size_t remain_size_;
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
 */
template <typename ValueType>
DIA<ValueType> ReadBinary(Context& ctx, const std::string& filepath) {

    StatsNode* stats_node =
        ctx.stats_graph().AddNode("ReadBinary", DIANodeType::GENERATOR);

    auto shared_node =
        std::make_shared<ReadBinaryNode<ValueType> >(
            ctx, filepath, stats_node);

    return DIA<ValueType>(shared_node, { stats_node });
}

//! \}

} // namespace api

//! imported from api namespace
using api::ReadBinary;

} // namespace thrill

#endif // !THRILL_API_READ_BINARY_HEADER

/******************************************************************************/
