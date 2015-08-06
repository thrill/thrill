/*******************************************************************************
 * c7a/api/generate_from_file.hpp
 *
 * DIANode for a generate operation. Performs the actual generate operation
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_GENERATE_FROM_FILE_HEADER
#define C7A_API_GENERATE_FROM_FILE_HEADER

#include <c7a/api/dia.hpp>
#include <c7a/api/dop_node.hpp>
#include <c7a/common/logger.hpp>

#include <fstream>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a GenerateFromFile operation. GenerateFromFile uses
 * a file from the file system to generate random inputs. Therefore
 * GenerateFromFile reads the complete file and applies the generator function
 * on each element. Afterwards each worker generates a DIA with a certain number
 * of random (possibly duplicate) elements from the generator file.
 *
 * \tparam ValueType Output type of the Generate operation.
 * \tparam ReadFunction Type of the generate function.
 */
template <typename ValueType, typename GeneratorFunction>
class GenerateFileNode : public DOpNode<ValueType>
{
public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::result_file_;

    /*!
     * Constructor for a GenerateFileNode. Sets the Context, parents, generator
     * function and file path.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param generator_function Generator function, which defines how each line
     * of the file is read and used for generation of a DIA.
     * \param path_in Path of the input file
     * \param size Number of elements in the generated DIA
     */
    GenerateFileNode(Context& ctx,
                     GeneratorFunction generator_function,
                     std::string path_in,
                     size_t size,
                     StatsNode* stats_node)
        : DOpNode<ValueType>(ctx, { }, "GenerateFromFile", stats_node),
          generator_function_(generator_function),
          path_in_(path_in),
          size_(size)
    { }

    virtual ~GenerateFileNode() { }

    //! Executes the generate operation. Reads a file line by line and creates a
    //! element vector, out of which elements are randomly chosen (possibly
    //! duplicated).
    void Execute() override { }

    void PushData() override {
        LOG << "GENERATING data to file " << result_file_.ToString();

        std::ifstream file(path_in_);
        assert(file.good());

        std::string line;
        while (std::getline(file, line))
        {
            if (*line.rbegin() == '\r') {
                line.erase(line.length() - 1);
            }
            elements_.push_back(generator_function_(line));
        }

        size_t local_elements;
        size_t elements_per_worker = size_ / (context_.max_rank() + 1);
        if (context_.max_rank() == context_.rank()) {
            //last worker gets leftovers
            local_elements = size_ -
                             (context_.max_rank() * elements_per_worker);
        }
        else {
            local_elements = elements_per_worker;
        }

        std::default_random_engine generator({ std::random_device()() });
        std::uniform_int_distribution<int> distribution(0, elements_.size() - 1);

        for (size_t i = 0; i < local_elements; i++) {
            size_t rand_element = distribution(generator);
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(elements_[rand_element]);
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
     * Returns information about the GeneratorNode as a string.
     * \return Stringified node.
     */
    std::string ToString() override {
        return "[GeneratorNode] Id: " + result_file_.ToString();
    }

private:
    //! The read function which is applied on every line read.
    GeneratorFunction generator_function_;
    //! Path of the input file.
    std::string path_in_;
    //! Element vector used for generation
    std::vector<ValueType> elements_;
    //! Size of the output DIA.
    size_t size_;

    static const bool debug = false;
};

template <typename GeneratorFunction>
auto GenerateFromFile(Context & ctx, std::string filepath,
                      const GeneratorFunction &generator_function,
                      size_t size) {

    using GeneratorResult =
              typename common::FunctionTraits<GeneratorFunction>::result_type;

    using GenerateResultNode =
              GenerateFileNode<GeneratorResult, GeneratorFunction>;

    static_assert(
        std::is_same<
            typename common::FunctionTraits<GeneratorFunction>::template arg<0>,
            const std::string&>::value,
        "GeneratorFunction needs a const std::string& as input");

    StatsNode* stats_node = ctx.stats_graph().AddNode("GenerateFromFile", "DOp");
    auto shared_node =
        std::make_shared<GenerateResultNode>(
            ctx, generator_function, filepath, size, stats_node);

    auto generator_stack = shared_node->ProduceStack();

    return DIARef<GeneratorResult, decltype(generator_stack)>(
        shared_node, generator_stack, { });
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_GENERATE_FROM_FILE_HEADER

/******************************************************************************/
