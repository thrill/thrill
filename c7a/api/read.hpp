/*******************************************************************************
 * c7a/api/read.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
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
 * A DIANode which performs a line-based Read operation. Read reads a file from
 * the file system and emits it as a DIA.
 */
class ReadNode : public DOpNode<std::string>
{
public:
    using Super = DOpNode<std::string>;
    using Super::context_;
    using Super::result_file_;

    /*!
     * Constructor for a ReadNode. Sets the DataManager, parents, read_function
     * and file path.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param filepath Path of the input file
     */
    ReadNode(Context& ctx,
             const std::string& filepath,
             StatsNode* stats_node)
        : Super(ctx, { }, "Read", stats_node),
          filepath_(filepath)
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
    void Execute() override { }

    void PushData() override {
        static const bool debug = false;
        LOG << "READING data " << result_file_.ToString();

        std::ifstream file(filepath_);
        assert(file.good());

        InputLineIterator it = GetInputLineIterator(
            file, context_.rank(), context_.number_worker());

        // Hook Read
        while (it.HasNext()) {
            auto item = it.Next();
            for (auto func : Super::callbacks_) {
                func(item);
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
        return FunctionStack<std::string>();
    }

    /*!
     * Returns "[ReadNode]" as a string.
     * \return "[ReadNode]"
     */
    std::string ToString() override {
        return "[ReadNode] Id: " + result_file_.ToString();
    }

private:
    //! Path of the input file.
    std::string filepath_;
};

DIARef<std::string> ReadLines(Context& ctx, std::string filepath) {

    StatsNode* stats_node = ctx.stats_graph().AddNode("ReadLines", "DOp");

    auto shared_node =
        std::make_shared<ReadNode>(
            ctx, filepath, stats_node);

    auto read_stack = shared_node->ProduceStack();

    return DIARef<std::string, decltype(read_stack)>(
        shared_node, read_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_READ_HEADER

/******************************************************************************/
