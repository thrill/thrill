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

template <typename Input, typename Output, typename WriteFunction>
class WriteNode : public ActionNode<Input>
{
public:
    WriteNode(Context& ctx,
              DIANode<Input>* parent, //TODO(??) don't we need to pass shared ptrs for the ref counting?
              WriteFunction write_function,
              std::string path_out)
        : ActionNode<Input>(ctx, { parent }),
          write_function_(write_function),
          path_out_(path_out)
    {
        LOG1 << this->get_parents().size();
        core::RunScope(this);
    }

    virtual ~WriteNode() { }

    //! Executes the write operation. Writes a file line by line and emits it to
    //! the DataManager after applying the write function on it.
    void execute() override {
        LOG1 << "WRITING data with id " << this->data_id_;

        std::ofstream file(path_out_);

        auto emit = (this->context_).get_data_manager().template GetOutputLineEmitter<Output>(file);

        // get data from data manager
        assert(this->get_parents().size() == 1);
        data::BlockIterator<Input> it = this->context_.get_data_manager().template GetLocalBlocks<Input>(
            this->get_parents().front()->get_data_id());
        // loop over output
        while (!it.IsClosed()) {
            it.WaitForMore();
            while (it.HasNext()) {
                const Input& item = it.Next();
                emit(write_function_(item));
            }
        }

        emit.Close();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](Input t, std::function<void(Input)> emit_func) {
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
        return "[WriteNode] Id:" + std::to_string(this->data_id_);
    }

private:
    //! The write function which is applied on every line read.
    WriteFunction write_function_;
    //! Path of the output file.
    std::string path_out_;
};

} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
