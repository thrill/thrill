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

#include <thrill/core/hyperloglog.hpp>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <size_t p, typename ValueType>
class HyperLogLogNode final : public ActionResultNode<Registers<p> >
{
    static constexpr bool debug = false;

    using Super = ActionResultNode<Registers<p> >;
    using Super::context_;

public:
    template <typename ParentDIA>
    HyperLogLogNode(const ParentDIA& parent, const char* label)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }) {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             registers.insert(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! Executes the sum operation.
    void Execute() final {
        // process the reduce
        registers = context_.net.AllReduce(registers);
    }

    //! Returns result of global sum.
    const Registers<p>& result() const final { return registers; }

private:
    Registers<p> registers;
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
