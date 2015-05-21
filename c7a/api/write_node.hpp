/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
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
template <typename Input, typename Output, typename WriteFunction>
class WriteNode : public ActionNode<Output>
{
public:
    /*!
    * Constructor for a WriteNode. Sets the DataManager, parents, write_function and file path.
    *
    * \param data_manager Data Manager.
    * \param parent Parent DIANode.
    * \param write_function Write function, which defines how each line of the file is written
    * \param path_out Path of the output file
    */
    WriteNode(data::DataManager& data_manager,
              DIANode<Input>* parent,
              WriteFunction write_function,
              std::string path_out)
        : ActionNode<Output>(data_manager, { parent }),
          write_function_(write_function),
          path_out_(path_out)
    { }

    virtual ~WriteNode() { }

    //! Executes the write operation. Writes a file line by line and emits it to
    //! the DataManager after applying the write function on it.
    void execute()
    {
        // BlockEmitter<Output> GetLocalEmitter(DIAId id) {
        SpacingLogger(true) << "Writing data with id" << this->data_id_;

        std::ofstream file(path_out_);
        assert(file.good());

        // Todo(ms):
        auto emit = (this->context_).get_data_manager().template GetOutputLineEmitter<Output>(file);

        // get data from data manager
        data::BlockIterator<Output> it = context_.get_data_manager().template GetLocalBlocks<Output>(data_id_);
        // loop over output
        while (it.HasNext()) {
            const Output& item = it.Next();
            //emit(write_function_(item));
        }
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [ = ](Output t, std::function<void(Output)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns "[WriteNode]" as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() override
    {
        return "[WriteNode] Id: " + std::to_string(this->data_id_);
    }

private:
    //! The write function which is applied on every line written.
    WriteFunction write_function_;
    //! Path of the output file.
    std::string path_out_;
};
} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
