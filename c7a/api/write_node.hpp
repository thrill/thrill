/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_NODE_HEADER
#define C7A_API_WRITE_NODE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia_node.hpp>
#include <c7a/api/function_stack.hpp>

#include <string>

namespace c7a {
namespace api {

template <typename ValueType, typename ParentStack, typename WriteFunction>
class WriteNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;
    using Super::data_id_;

    using ParentInput = typename ParentStack::Input;

    WriteNode(Context& ctx,
              std::shared_ptr<DIANode<ParentInput> > parent,
              const ParentStack& parent_stack,
              WriteFunction write_function,
              std::string path_out)
        : ActionNode(ctx, { parent }),
          write_function_(write_function),
          path_out_(path_out),
          file_(path_out_),
          emit_(context_.data_manager().
                template GetOutputLineEmitter<std::string>(file_))
    {
        sLOG << "Creating write node.";

        auto pre_op_function = [=](ValueType input) {
                                   PreOp(input);
                               };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent_stack.push(pre_op_function).emit();
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

template <typename ValueType, typename Stack>
template <typename WriteFunction>
void DIARef<ValueType, Stack>::WriteToFileSystem(const std::string& filepath,
                                                 const WriteFunction& write_function) {

    using WriteResultNode = WriteNode<
              ValueType, Stack, WriteFunction>;

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<WriteFunction>::template arg<0> >::type,
            ValueType>::value,
        "WriteFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<WriteFunction>::result_type,
            std::string>::value,
        "WriteFunction should have std::string as output type.");

    auto shared_node =
        std::make_shared<WriteResultNode>(node_->context(),
                                          node_,
                                          stack_,
                                          write_function,
                                          filepath);

    core::StageBuilder().RunScope(shared_node.get());
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
