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
          typename ParentDIA0, typename ParentDIA1, typename Threshhold>
class TrivialSimJoinNode : public DOpNode<ValueType>
{
    static constexpr bool debug = false;
    static constexpr bool self_verify = debug && common::g_debug_mode;

    //! Set this variable to true to enable generation and output of merge stats
    static constexpr bool stats_enabled = false;

    using InputType = typename common::FunctionTraits<DistanceFunction>::template arg<0>;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TrivialSimJoinNode(const DistanceFunction& distance_function,
                       const ParentDIA0& parentR,
                       const ParentDIA1& parentS,
		               const Threshhold& threshhold)
        : Super(parentR.ctx(), "TrivialSimJoin",
                { parentR.id(), parentS.id() },
                { parentR.node(), parentS.node() }),
          distance_function_(distance_function),
		  threshhold_(threshhold)
    {

        auto pre_op_fn_R = [this](const InputType& input) {
                               elements_R_.push_back(input);
                           };
        auto lop_chain_R = parentR.stack().push(pre_op_fn_R).fold();
        parentR.node()->AddChild(this, lop_chain_R, 0);

        elements_S_ = context_.GetNewMixStream(this);

        element_S_writers_ = elements_S_->GetWriters();

        auto pre_op_fn_S = [this](const InputType& input) {
                               for (auto& writer : element_S_writers_) {
                                   writer.Put(input);
                               }
                           };

        auto lop_chain_S = parentS.stack().push(pre_op_fn_S).fold();

        parentS.node()->AddChild(this, lop_chain_S, 1);
    }

    void Execute() final {
        for (auto& writer : element_S_writers_) {
            writer.Close();
        }
        // MainOp();
    }

    void PushData(bool consume) final {
        data::MixStream::MixReader reader = elements_S_->GetMixReader(consume);
        while (reader.HasNext()) {
            InputType elementS = reader.template Next<InputType>();
            for (InputType elementR : elements_R_) {
                if (distance_function_(elementS, elementR) < threshhold_) {
                    this->PushItem(std::make_pair(elementS, elementR));
                }
            }
        }
    }

    void Dispose() final { }

private:
    //! Merge comparator
    DistanceFunction distance_function_;

	Threshhold threshhold_;

    std::vector<InputType> elements_R_;

    data::MixStreamPtr elements_S_;
    std::vector<data::MixStream::Writer> element_S_writers_;

    /*!
     * Receives elements from other workers and re-balance them, so each worker
     * has the same amount after merging.
     */
    /* void MainOp() {

       }*/
};

template <typename ValueType, typename Stack>
template <typename DistanceFunction, typename SecondDIA, typename Threshhold>
auto DIA<ValueType, Stack>::TrivialSimJoin(const SecondDIA &second_dia,
                                           const DistanceFunction &distance_function,
	                                       const Threshhold &threshhold) const {
	assert(IsValid());

    using OutputPair = std::pair<ValueType, ValueType>;					
																		
    using TrivialSimJoinNode =											
		api::TrivialSimJoinNode<OutputPair, DistanceFunction, DIA, SecondDIA, Threshhold>;

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

	static_assert(
		std::is_same<
		typename FunctionTraits<DistanceFunction>::result_type,
		Threshhold>::value,
		"Distance Function must return the type of the distance threshhold");

    auto node =
        common::MakeCounting<TrivialSimJoinNode>(distance_function, *this, second_dia, threshhold);

    return DIA<OutputPair>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_TRIVIAL_SIM_JOIN_HEADER

/******************************************************************************/
