/*******************************************************************************
 * thrill/api/read_binary.hpp
 *
 * Part of Project Thrill.
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

            size_t my_start, my_end;
            std::tie(my_start, my_end) =
                context_.CalculateLocalRange(files.total_size / fixed_size_);

            my_start *= fixed_size_;
            my_end *= fixed_size_;

            sLOG << "ReadBinaryNode" << ctx.num_workers()
                 << "my_start" << my_start << "my_end" << my_end;

            size_t i = 0;
            while (files.list[i].size_inc_psum() <= my_start) {
                i++;
            }

            while (i < files.count() &&
                   files.list[i].size_ex_psum <= my_end) {

                size_t file_begin = files.list[i].size_ex_psum;
                size_t file_end = files.list[i].size_inc_psum();
                size_t file_size = files.list[i].size;

                FileInfo fi;
                fi.path = files.list[i].path;
                fi.begin = my_start <= file_begin ? 0 : my_start - file_begin;
                fi.end = my_end >= file_end ? file_size : my_end - file_begin;

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

            size_t my_start, my_end;
            std::tie(my_start, my_end) =
                context_.CalculateLocalRange(files.total_size);

            while (files.list[i].size_inc_psum() <= my_start) {
                i++;
            }

            while (i < files.count() &&
                   files.list[i].size_inc_psum() <= my_end) {
                my_files_.push_back(
                    FileInfo { files.list[i].path, 0,
                               std::numeric_limits<size_t>::max() });
                i++;
            }

            LOG << my_files_.size() << " files "
                << "from " << my_start << " to " << my_end;
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

    auto ProduceStack() {
        return FunctionStack<ValueType>();
    }

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

        data::Block NextBlock() {
            if (done_) return data::Block();

            data::ByteBlockPtr bytes
                = data::ByteBlock::Allocate(block_size, context_.block_pool());

            size_t rb = std::min(block_size, remain_size_);

            ssize_t size = sysfile_.read(bytes->data(), rb);
            stats_total_bytes_ += size;
            stats_total_reads_++;

            if (size > 0) {
                remain_size_ -= rb;
                return data::Block(bytes, 0, size, 0, 0);
            }
            else if (size < 0) {
                throw common::ErrnoException("File reading error");
            }
            else {
                // size == 0 -> read finished
                sysfile_.close();
                done_ = true;
                return data::Block();
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
        ctx.stats_graph().AddNode("ReadBinary", DIANodeType::DOP);

    auto shared_node =
        std::make_shared<ReadBinaryNode<ValueType> >(
            ctx, filepath, stats_node);

    auto read_stack = shared_node->ProduceStack();

    return DIA<ValueType, decltype(read_stack)>(
        shared_node, read_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_READ_BINARY_HEADER

/******************************************************************************/
