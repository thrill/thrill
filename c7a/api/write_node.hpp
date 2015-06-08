/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
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

template <typename Input, typename Output, typename WriteFunction, typename Stack>
class WriteNode : public ActionNode<Input>
{
public:
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
          emit_(context_.get_data_manager().template GetOutputLineEmitter<Output>(file_))
    {
        sLOG << "Creating WriteNode with" << this->get_parents().size() << "parents to" << path_out_;

        // using WriteArg = typename FunctionTraits<WriteFunction>::template arg<0>;
        auto pre_op_fn = [=](WriteArg input) {
                             PreOp(input);
                         };
        auto lop_chain = local_stack_.push(pre_op_fn).emit();
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
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity

        using WriteArg = typename FunctionTraits<WriteFunction>::template arg<0>;
        auto id_fn = [=](WriteArg t, std::function<void(WriteArg)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() override {
        return "[WriteNode] Id:" + this->data_id_.ToString();
    }

private:
    //! context
    using ActionNode<Input>::context_;
    
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

} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
