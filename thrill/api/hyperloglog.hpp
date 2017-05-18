/*******************************************************************************
 * thrill/api/hyperloglog.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Moritz Kiefer <moritz.kiefer@purelyfunctional.org>
 * Copyright (C) 2017 Tino Fuhrmann <tino-fuhrmann@web.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_HYPERLOGLOG_HEADER
#define THRILL_API_HYPERLOGLOG_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/hyperloglog.hpp>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <size_t p, typename ValueType>
class HyperLogLogNode final
    : public ActionResultNode<core::HyperLogLogRegisters<p> >
{
    static constexpr bool debug = false;

    using Super = ActionResultNode<core::HyperLogLogRegisters<p> >;
    using Super::context_;

public:
    template <typename ParentDIA>
    HyperLogLogNode(const ParentDIA& parent, const char* label)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }) {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             registers_.insert(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! Executes the sum operation.
    void Execute() final {
        // process the reduce
        registers_ = context_.net.AllReduce(registers_);
    }

    //! Returns result of global sum.
    const core::HyperLogLogRegisters<p>& result() const final
    { return registers_; }

private:
    core::HyperLogLogRegisters<p> registers_;
};

template <typename ValueType, typename Stack>
template <size_t p>
double DIA<ValueType, Stack>::HyperLogLog() const {
    assert(IsValid());

    auto node = tlx::make_counting<HyperLogLogNode<p, ValueType> >(
        *this, "HyperLogLog");
    node->RunScope();
    auto registers = node->result();
    return registers.result();
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_HYPERLOGLOG_HEADER

/******************************************************************************/
