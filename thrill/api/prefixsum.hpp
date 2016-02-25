/*******************************************************************************
 * thrill/api/prefixsum.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_PREFIXSUM_HEADER
#define THRILL_API_PREFIXSUM_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <string>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIA, typename SumFunction>
class PrefixSumNode final : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    PrefixSumNode(const ParentDIA& parent,
                  const SumFunction& sum_function,
                  const ValueType& initial_element)
        : Super(parent.ctx(), "PrefixSum", { parent.id() }, { parent.node() }),
          sum_function_(sum_function),
          local_sum_(initial_element),
          initial_element_(initial_element)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* id */) final {
        writer_ = file_.GetWriter();
    }

    //! PreOp: compute local prefixsum and store items.
    void PreOp(const ValueType& input) {
        LOG << "Input: " << input;
        local_sum_ = sum_function_(local_sum_, input);
        writer_.Put(input);
    }

    void StopPreOp(size_t /* id */) final {
        writer_.Close();
    }

    //! Executes the prefixsum operation.
    void Execute() final {
        LOG << "MainOp processing";

        ValueType sum = context_.net.PrefixSum(
            local_sum_, initial_element_, sum_function_, false);

        if (context_.my_rank() == 0) {
            sum = initial_element_;
        }

        local_sum_ = sum;
    }

    void PushData(bool consume) final {
        data::File::Reader reader = file_.GetReader(consume);

        ValueType sum = local_sum_;

        for (size_t i = 0; i < file_.num_items(); ++i) {
            sum = sum_function_(sum, reader.Next<ValueType>());
            this->PushItem(sum);
        }
    }

    void Dispose() final {
        file_.Clear();
    }

private:
    //! The sum function which is applied to two elements.
    SumFunction sum_function_;
    //! Local sum to be used in all reduce operation.
    ValueType local_sum_;
    //! Initial element.
    ValueType initial_element_;

    //! Local data file
    data::File file_ { context_.GetFile() };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_;
};

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIA<ValueType, Stack>::PrefixSum(
    const SumFunction &sum_function, const ValueType &initial_element) const {
    assert(IsValid());

    using SumResultNode
              = PrefixSumNode<ValueType, DIA, SumFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<SumFunction>::template arg<0>
            >::value,
        "SumFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<SumFunction>::template arg<1> >::value,
        "SumFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<SumFunction>::result_type,
            ValueType>::value,
        "SumFunction has the wrong input type");

    auto shared_node
        = std::make_shared<SumResultNode>(*this, sum_function, initial_element);

    return DIA<ValueType>(shared_node);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_PREFIXSUM_HEADER

/******************************************************************************/
