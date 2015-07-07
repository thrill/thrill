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

#include <string>
#include "action_node.hpp"
#include "dia_node.hpp"
#include "function_stack.hpp"

namespace c7a {
namespace api {

template <typename Input, typename Output,
          typename WriteFunction, typename Stack>
class WriteNode : public ActionNode<Input>
{
public:
    using Super = ActionNode<Input>;
    using Super::context_;
    using Super::data_id_;
    using WriteArg = typename FunctionTraits<WriteFunction>::template arg<0>;

    WriteNode(Context& ctx,
              DIANode<Input>* parent, //TODO(??) don't we need to pass shared ptrs for the ref counting?
              Stack& stack,
              WriteFunction write_function,
              std::string path_out)
        : ActionNode<Input>(ctx, { parent }),
          local_stack_(stack),
          write_function_(write_function),
          path_out_(path_out),
          file_(path_out_),
          emit_(context_.get_data_manager().
                template GetOutputLineEmitter<Output>(file_))
    {
        sLOG << "Creating write node.";

        auto pre_op_function = [=](WriteArg input) {
                                   PreOp(input);
                               };
        auto lop_chain = local_stack_.push(pre_op_function).emit();
        parent->RegisterChild(lop_chain);
    }

    void PreOp(WriteArg input) {
        emit_(write_function_(input));
    }

    virtual ~WriteNode() { }

    //! Closes the output file
    void execute() override {
        sLOG << "closing file" << path_out_;
        emit_.Close();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity

        using WriteArg =
                  typename FunctionTraits<WriteFunction>::template arg<0>;
        auto id_fn = [=](WriteArg t, auto emit_func) {
                         return emit_func(t);
                     };

        return MakeFunctionStack<WriteArg>(id_fn);
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
    data::OutputLineEmitter<Output> emit_;

    static const bool debug = false;
};

template <typename T, typename Stack>
template <typename WriteFunction>
void DIARef<T, Stack>::WriteToFileSystem(const std::string& filepath,
                                         const WriteFunction& write_function) {

    using WriteResult = typename FunctionTraits<WriteFunction>::result_type;
    using WriteResultNode = WriteNode<T, WriteResult, WriteFunction,
                                      decltype(local_stack_)>;

    auto shared_node =
        std::make_shared<WriteResultNode>(node_->get_context(),
                                          node_.get(),
                                          local_stack_,
                                          write_function,
                                          filepath);

    auto write_stack = shared_node->ProduceStack();
    core::StageBuilder().RunScope(shared_node.get());
}
}

} // namespace api

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
