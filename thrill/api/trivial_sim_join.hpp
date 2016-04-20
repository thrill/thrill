/*******************************************************************************
 * thrill/api/trivial_sim_join.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_TRIVIAL_SIM_JOIN_HEADER
#define THRILL_API_TRIVIAL_SIM_JOIN_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace thrill {
namespace api {

template <typename ValueType, typename DistanceFunction, 
		  typename ParentDIA0, typename ParentDIA1>
class TrivialSimJoinNode : public DOpNode<ValueType>
{
    static constexpr bool debug = false;
    static constexpr bool self_verify = debug && common::g_debug_mode;

    //! Set this variable to true to enable generation and output of merge stats
    static constexpr bool stats_enabled = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TrivialSimJoinNode(const DistanceFunction& distance_function,
              const ParentDIA0& parent0,
              const ParentDIA1& parent1)
        : Super(parent0.ctx(), "TrivialSimJoin",
                { parent0.id(), parent1.id()},
                { parent0.node(), parent1.node()}),
          distance_function_(distance_function)
    {	    

    }

    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
    }

    void Dispose() final { }

private:
    //! Merge comparator
    DistanceFunction distance_function_;

   
    /*!
     * Receives elements from other workers and re-balance them, so each worker
     * has the same amount after merging.
     */
    void MainOp() {
       
    }
};

template <typename ValueType, typename Stack>
template <typename DistanceFunction, typename SecondDIA>
auto DIA<ValueType, Stack>::TrivialSimJoin(const SecondDIA &second_dia,
					const DistanceFunction &distance_function) const {

	using OutputPair = std::pair<ValueType, ValueType>;

    using TrivialSimJoinNode =
		api::TrivialSimJoinNode<OutputPair, DistanceFunction, DIA, SecondDIA>;

    // Assert function types.
    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<DistanceFunction>::template arg<0>
            >::value,
        "Distance Function has the wrong input type in argument 0");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<DistanceFunction>::template arg<1>
            >::value,
        "Distance Function has the wrong input type in argument 1");

    // Assert meaningful return type of comperator.
    static_assert(
        std::is_convertible<
		typename FunctionTraits<DistanceFunction>::result_type,
            float
            >::value,
			"Distance Function must return a numeral.");

    auto trivial_sim_join_node =
        common::MakeCounting<TrivialSimJoinNode>(distance_function, *this, second_dia);

    return DIA<OutputPair>(trivial_sim_join_node);
}

} // namespace api

} // namespace thrill

#endif // !THRILL_API_TRIVIAL_SIM_JOIN_HEADER

/******************************************************************************/
