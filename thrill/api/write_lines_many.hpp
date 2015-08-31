/*******************************************************************************
 * thrill/api/write_lines_many.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_LINES_MANY_HEADER
#define THRILL_API_WRITE_LINES_MANY_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>

#include <fstream>
#include <string>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class WriteLinesManyNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    WriteLinesManyNode(const ParentDIARef& parent,
                       const std::string& path_out,
                       size_t max_file_size,
                       StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, "Write", stats_node),
          out_pathbase_(path_out),
          file_(core::make_path(
                    out_pathbase_, context_.my_rank(), 0)),
          max_file_size_(max_file_size)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](std::string input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    void PreOp(std::string input) {
        current_file_size_ += input.size() + 1;
        file_ << input << "\n";
        if (THRILL_UNLIKELY(current_file_size_ >= max_file_size_)) {
            LOG << "Closing file" << out_serial_;
            file_.close();
            std::string new_path = core::make_path(
                out_pathbase_, context_.my_rank(), out_serial_++);
            file_.open(new_path);
            LOG << "Opening file: " << new_path;
        
            current_file_size_ = 0;
        }
    }

    //! Closes the output file
    void Execute() final {
        sLOG << "closing file";
        file_.close();
    }

    void Dispose() final { }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() final {
        return "[WriteNode] Id:" + std::to_string(this->id());
    }

private:
    //! Base path of the output file.
    std::string out_pathbase_;

    //! File to write to
    std::ofstream file_;

    //! Maximal file size in bytes
    size_t max_file_size_;

    //! Current file size in bytes
    size_t current_file_size_ = 0;

    //! File serial number for this worker
    size_t out_serial_ = 1;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteLinesMany(
    const std::string& filepath, size_t max_file_size) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLinesMany needs an std::string as input parameter");

    using WriteResultNode = WriteLinesManyNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteLinesMany", DIANodeType::ACTION);

    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          max_file_size,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_LINES_MANY_HEADER

/******************************************************************************/
