/*******************************************************************************
 * thrill/api/generate.hpp
 *
 * DIANode for a generate operation. Performs the actual generate operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GENERATE_HEADER
#define THRILL_API_GENERATE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/logger.hpp>

#include <random>
#include <type_traits>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs a Generate operation. Generate creates an DIA
 * according to a generator function. This function is used to generate a DIA of
 * a certain size by applying it to integers from 0 to size - 1.
 *
 * \tparam ValueType Output type of the Generate operation.
 * \tparam GenerateNode Type of the generate function.
 * \ingroup api_layer
 */
template <typename ValueType, typename GenerateFunction>
class GenerateNode final : public SourceNode<ValueType>
{
public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a GenerateNode. Sets the Context, parents, generator
     * function and file path.
     */
    GenerateNode(Context& ctx,
                 GenerateFunction generate_function,
                 size_t size)
        : Super(ctx, "Generate"),
          generate_function_(generate_function),
          size_(size)
    { }

    void PushData(bool /* consume */) final {
        common::Range local = context_.CalculateLocalRange(size_);

        for (size_t i = local.begin; i < local.end; i++) {
            this->PushItem(generate_function_(i));
        }
    }

private:
    //! The generator function which is applied to every index.
    GenerateFunction generate_function_;
    //! Size of the output DIA.
    size_t size_;
};

/*!
 * Generate is a Source-DOp, which creates a DIA of given size using a
 * generator function. The generator function called for each index in the range
 * of `[0,size)` and must output exactly one item.
 *
 * \param ctx Reference to the Context object
 *
 * \param size Size of the output DIA
 *
 * \param generate_function Generator function, which maps `size_t` from
 * `[0,size)` to elements. Input type has to be `size_t`.
 *
 * \ingroup dia_sources
 */
template <typename GenerateFunction>
auto Generate(Context& ctx, size_t size,
              const GenerateFunction& generate_function) {

    using GenerateResult =
              typename common::FunctionTraits<GenerateFunction>::result_type;

    using GenerateNode =
              api::GenerateNode<GenerateResult, GenerateFunction>;

    static_assert(
        common::FunctionTraits<GenerateFunction>::arity == 1,
        "GenerateFunction must take exactly one parameter");

    static_assert(
        std::is_convertible<
            size_t,
            typename common::FunctionTraits<GenerateFunction>::template arg<0>
            >::value,
        "GenerateFunction needs a const unsigned long int& (aka. size_t) as input");

    auto node = tlx::make_counting<GenerateNode>(
        ctx, generate_function, size);

    return DIA<GenerateResult>(node);
}

/*!
 * Generate is a Source-DOp, which creates a DIA of given size containing the
 * size_t indexes `[0,size)`.
 *
 * \param ctx Reference to the Context object
 *
 * \param size Size of the output DIA
 *
 * \ingroup dia_sources
 */
static inline
auto Generate(Context& ctx, size_t size) {
    return Generate(ctx, size, [](const size_t& index) { return index; });
}

} // namespace api

//! imported from api namespace
using api::Generate;

} // namespace thrill

#endif // !THRILL_API_GENERATE_HEADER

/******************************************************************************/
