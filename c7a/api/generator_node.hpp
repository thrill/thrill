/*******************************************************************************
 * c7a/api/read_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once

#include <c7a/common/logger.hpp>
#include <c7a/api/dop_node.hpp>
#include <c7a/api/function_stack.hpp>

#include <string>
#include <fstream>

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Read operation. Read reads a file from the file system and
 * emits it to the DataManager according to a given read function.
 *
 * \tparam Output Output type of the Read operation.
 * \tparam ReadFunction Type of the read function.
 */
template <typename Output, typename GeneratorFunction>
class GeneratorNode : public DOpNode<Output>
{
public:
    /*!
    * Constructor for a ReadNode. Sets the DataManager, parents, read_function and file path.
    *
    * \param ctx Reference to Context, which holds references to data and network.
    * \param read_function Read function, which defines how each line of the file is read and emitted
    * \param path_in Path of the input file
    */
    GeneratorNode(Context& ctx,
                  GeneratorFunction generator_function,
                  std::string path_in,
                  size_t size )
        : DOpNode<Output>(ctx, { }),
          generator_function_(generator_function),
          path_in_(path_in),
          size_(size)
    { }

    virtual ~GeneratorNode() { }

    //! Executes the read operation. Reads a file line by line and emits it to
    //! the DataManager after applying the read function on it.
    void execute() {
        // BlockEmitter<Output> GetLocalEmitter(DIAId id) {
        /*  LOG1 << "READING data with id " << this->data_id_;

        std::ifstream file(path_in_);
        assert(file.good());

        data::InputLineIterator it = (this->context_).get_data_manager().GetInputLineIterator(file, (this->context_).rank(), (this->context_).number_worker());

        data::BlockEmitter<Output> emit = (this->context_).get_data_manager().template GetLocalEmitter<Output>(this->data_id_);

        // Hook Read
        while (it.HasNext()) {
            auto item = it.Next();
            for (auto func : DIANode<Output>::callbacks_) {
                func(read_function_(item));
            }
            }*/
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](Output t, std::function<void(Output)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns "[ReadNode]" as a string.
     * \return "[ReadNode]"
     */
    std::string ToString() override {
        return "[GeneratorNode] Id: " + this->data_id_.ToString();
    }

private:
    //! The read function which is applied on every line read.
    GeneratorFunction generator_function_;
    //! Path of the input file.
    std::string path_in_;

    size_t size_;
};

} // namespace c7a

//! \}


/******************************************************************************/
