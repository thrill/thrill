/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_NODE_HEADER
#define C7A_API_WRITE_NODE_HEADER

#include <string>
#include "action_node.hpp"
#include "dia_node.hpp"
#include "function_stack.hpp"

namespace c7a {
namespace api {

template <typename ParentType, typename ValueType,
          typename Stack, typename WriteFunction>
class WriteNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;
    using Super::data_id_;

    WriteNode(Context& ctx,
              DIANode<ParentType>* parent, //TODO(??) don't we need to pass shared ptrs for the ref counting?
              Stack& stack,
              WriteFunction write_function,
              std::string path_out)
        : ActionNode(ctx, { parent }),
          local_stack_(stack),
          write_function_(write_function),
          path_out_(path_out),
          file_(path_out_),
          emit_(context_.get_data_manager().
                template GetOutputLineEmitter<std::string>(file_))
    {
        sLOG << "Creating write node.";

        auto pre_op_function = [=](ValueType input) {
                                   PreOp(input);
                               };
        auto lop_chain = local_stack_.push(pre_op_function).emit();
        parent->RegisterChild(lop_chain);
    }

    void PreOp(ValueType input) {
        emit_(write_function_(input));
    }

    virtual ~WriteNode() { }

    //! Closes the output file
    void Execute() override {
        sLOG << "closing file" << path_out_;
        emit_.Close();
    }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() override {
        return "[WriteNode] Id:" + data_id_.ToString();
    }

private:
    //! Local stack
    Stack local_stack_;

    //! The write function which is applied on every line read.
    WriteFunction write_function_;

    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;

    //! Emitter to file
    data::OutputLineEmitter<std::string> emit_;

    static const bool debug = false;
};

template <typename ParentType, typename ValueType, typename Stack>
template <typename WriteFunction>
void DIARef<ParentType, ValueType, Stack>::WriteToFileSystem(const std::string& filepath,
                                         const WriteFunction& write_function) {
    using WriteResultNode = WriteNode<ParentType, ValueType, 
                                      decltype(local_stack_), WriteFunction>;

    auto shared_node =
        std::make_shared<WriteResultNode>(node_->get_context(),
                                          node_.get(),
                                          local_stack_,
                                          write_function,
                                          filepath);

    core::StageBuilder().RunScope(shared_node.get());
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
