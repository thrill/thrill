/*******************************************************************************
 * c7a/api/generate.hpp
 *
 * DIANode for a generate operation. Performs the actual generate operation
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_GENERATE_HEADER
#define C7A_API_GENERATE_HEADER

#include <c7a/api/dop_node.hpp>
#include <c7a/common/logger.hpp>

#include <fstream>
#include <random>
#include <string>
#include <type_traits>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Generate operation. Generate creates an DIA according
 * to a generator function. This function is used to generate a DIA of a certain
 * size by applying it to integers from 0 to size - 1.
 *
 * \tparam ValueType Output type of the Generate operation.
 * \tparam ReadFunction Type of the generate function.
 */
template <typename ValueType, typename GeneratorFunction>
class GenerateNode : public DOpNode<ValueType>
{
public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
    /*!
    * Constructor for a GenerateNode. Sets the Context, parents, generator
    * function and file path.
    *
    * \param ctx Reference to Context, which holds references to data and network.
    * \param generator_function Generator function, which defines how each line
    * of the file is read and used for generation of a DIA.
    * \param generator_function generates elements from an index
    * \param size Number of elements in the generated DIA
    */
    GenerateNode(Context& ctx,
                 GeneratorFunction generator_function,
                 size_t size,
                 StatsNode* stats_node)
        : DOpNode<ValueType>(ctx, { }, "Generate", stats_node),
          generator_function_(generator_function),
          size_(size)
    { }

    virtual ~GenerateNode() { }

    //! Executes the generate operation. Reads a file line by line and creates a
    //! element vector, out of which elements are randomly chosen (possibly
    //! duplicated).
    void Execute() override {
        this->StartExecutionTimer();
        this->StopExecutionTimer();
    }

    void PushData() override {
        LOG << "GENERATING data with id " << this->result_file_;

        using InputArgument
                  = typename common::FunctionTraits<GeneratorFunction>::template arg<0>;

        static_assert(std::is_convertible<size_t, InputArgument>::value,
                      "The GeneratorFunction needs a size_t as input parameter");

        size_t elements_per_worker = (size_ / (context_.max_rank() + 1));
        size_t offset = elements_per_worker * context_.rank();
        size_t local_elements = elements_per_worker;

        //last worker might has less elements than others
        if (context_.max_rank() == context_.rank() && context_.max_rank() > 0) {
            size_t elements_of_workers_before = elements_per_worker * context_.max_rank();
            local_elements = size_ - elements_of_workers_before;
        }

        for (size_t i = 0; i < local_elements; i++) {
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(generator_function_(i + offset));
            }
        }
    }

    void Dispose() override { }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
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
        return "[GeneratorNode] Id: " + this->result_file_.ToString();
    }

private:
    //! The read function which is applied on every line read.
    GeneratorFunction generator_function_;
    //! Size of the output DIA.
    size_t size_;

    static const bool debug = false;
};

template <typename GeneratorFunction>
auto Generate(Context & ctx,
              const GeneratorFunction &generator_function,
              size_t size) {

    using GeneratorResult =
              typename common::FunctionTraits<GeneratorFunction>::result_type;

    using GenerateResultNode =
              GenerateNode<GeneratorResult, GeneratorFunction>;

    static_assert(
        std::is_convertible<
            size_t,
            typename common::FunctionTraits<GeneratorFunction>::template arg<0>
            >::value,
        "GeneratorFunction needs a const unsigned long int& (aka. size_t) as input");

    StatsNode* stats_node = ctx.stats_graph().AddNode("Generate", "DOp");
    auto shared_node =
        std::make_shared<GenerateResultNode>(ctx,
                                             generator_function,
                                             size,
                                             stats_node);

    auto generator_stack = shared_node->ProduceStack();

    return DIARef<GeneratorResult, decltype(generator_stack)>
               (shared_node, 
                generator_stack,
                { stats_node });
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_GENERATE_HEADER

/******************************************************************************/
