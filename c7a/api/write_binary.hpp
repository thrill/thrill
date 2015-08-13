/*******************************************************************************
 * c7a/api/write_binary.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_BINARY_HEADER
#define C7A_API_WRITE_BINARY_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/common/item_serialization_tools.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/data/serialization.hpp>

#include <fstream>
#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class WriteBinaryNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::result_file_;
    using Super::context_;

    WriteBinaryNode(const ParentDIARef& parent,
                    const std::string& path_out,
                    StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() },
                     "WriteBinary", stats_node),
          path_out_(path_out + std::to_string(context_.my_rank())),
          bfw_(path_out_)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    void PreOp(ValueType input) {
        // file_ << input;
        data::Serialization<BinaryFileWriter, ValueType>::Serialize(input, bfw_);
    }

    //! Closes the output file
    void Execute() final {
        sLOG << "closing file" << path_out_;
        bfw_.CloseStream();
    }

    void Dispose() final { }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() final {
        return "[WriteNode] Id:" + result_file_.ToString();
    }

private:
    //! Path of the output file.
    std::string path_out_;

    class BinaryFileWriter
        : public common::ItemWriterToolsBase<BinaryFileWriter>
    {
    public:
        BinaryFileWriter(std::string file) : outstream_(file) { }

        virtual ~BinaryFileWriter() { }

        void CloseStream() {
            outstream_.close();
        }

        template <typename Binary>
        BinaryFileWriter & Put(const Binary& item) {
            outstream_.write(reinterpret_cast<const char*>(&item), sizeof(item));
            return *this;
        }

        BinaryFileWriter & PutByte(const uint8_t& byte) {
            outstream_.put(byte);
            return *this;
        }

        BinaryFileWriter & Append(const void* data, size_t size) {
            const char* cdata = reinterpret_cast<const char*>(data);
            outstream_.write(cdata, size);
            return *this;
        }

    private:
        std::ofstream outstream_;
    };

    BinaryFileWriter bfw_;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteBinary(
    const std::string& filepath) const {

    using WriteResultNode = WriteBinaryNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteBinary", NodeType::ACTION);

    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_WRITE_BINARY_HEADER

/******************************************************************************/
