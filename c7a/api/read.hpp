/*******************************************************************************
 * c7a/api/read.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_READ_HEADER
#define C7A_API_READ_HEADER

#include <c7a/api/dia.hpp>
#include <c7a/api/dop_node.hpp>
#include <c7a/api/input_line_iterator.hpp>
#include <c7a/common/logger.hpp>

#include <fstream>
#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Read operation. Read reads a file from the file system and
 * emits it to the DataManager according to a given read function.
 *
 * \tparam ValueType Output type of the Read operation.
 * \tparam ReadFunction Type of the read function.
 */
template <typename ValueType, typename ReadFunction>
class ReadNode : public DOpNode<ValueType>
{
public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::result_file_;

    /*!
    * Constructor for a ReadNode. Sets the DataManager, parents, read_function and file path.
    *
    * \param ctx Reference to Context, which holds references to data and network.
    * \param read_function Read function, which defines how each line of the file is read and emitted
    * \param path_in Path of the input file
    */
    ReadNode(Context& ctx,
             ReadFunction read_function,
             const std::string& path_in,
             StatsNode* stats_node)
        : DOpNode<ValueType>(ctx, { }, "Read", stats_node),
          read_function_(read_function),
          path_in_(path_in)
    { }

    virtual ~ReadNode() { }

    //! Returns an InputLineIterator with a given input file stream.
    //!
    //! \param file Input file stream
    //! \param my_id Id of this worker
    //! \param num_work Number of workers
    //!
    //! \return An InputLineIterator for a given file stream
    InputLineIterator GetInputLineIterator(
        std::ifstream& file, size_t my_id, size_t num_work) {
        return InputLineIterator(file, my_id, num_work);
    }

    //! Executes the read operation. Reads a file line by line and emits it to
    //! the DataManager after applying the read function on it.
    void Execute() override {
        this->StartExecutionTimer();
        this->StopExecutionTimer();
    }

    void PushData() override {
        static const bool debug = false;
        LOG << "READING data " << result_file_.ToString();

        std::ifstream file(path_in_);
        assert(file.good());

        InputLineIterator it = GetInputLineIterator(
            file, context_.rank(), context_.max_rank());

        // Hook Read
        while (it.HasNext()) {
            auto item = it.Next();
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(read_function_(item));
            }
        }
    }

    void Dispose() override { }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueType>();
    }

    /*!
     * Returns "[ReadNode]" as a string.
     * \return "[ReadNode]"
     */
    std::string ToString() override {
        return "[ReadNode] Id: " + result_file_.ToString();
    }

private:
    //! The read function which is applied on every line read.
    ReadFunction read_function_;
    //! Path of the input file.
    std::string path_in_;
};

template <typename ReadFunction>
auto ReadLines(Context & ctx, std::string filepath,
               const ReadFunction &read_function) {

    using ReadResult =
              typename common::FunctionTraits<ReadFunction>::result_type;

    using ReadResultNode = ReadNode<ReadResult, ReadFunction>;

    static_assert(
        std::is_same<
            typename common::FunctionTraits<ReadFunction>::template arg<0>,
            const std::string&>::value,
        "Read function needs const std::string& as input parameter.");

    StatsNode* stats_node = ctx.stats_graph().AddNode("ReadLines", "DOp");
    auto shared_node =
        std::make_shared<ReadResultNode>(ctx,
                                         read_function,
                                         filepath,
                                         stats_node);

    auto read_stack = shared_node->ProduceStack();
    return DIARef<ReadResult, decltype(read_stack)>
               (shared_node, 
                read_stack, 
                { stats_node });
}

//! \}

} // namespace api
} // namespace c7a

//! \}
#endif // !C7A_API_READ_HEADER

/******************************************************************************/
