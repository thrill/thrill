/*******************************************************************************
 * thrill/api/size.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SIZE_HEADER
#define THRILL_API_SIZE_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/net/group.hpp>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ParentDIA>
class SizeNode final : public ActionNode
{
    static constexpr bool debug = false;

    using Super = ActionNode;
    using Super::context_;

    //! input type is the parent's output value type.
    using Input = typename ParentDIA::ValueType;

public:
    explicit SizeNode(const ParentDIA& parent)
        : ActionNode(parent.ctx(), "Size", { parent.id() }, { parent.node() })
    {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const Input&) { ++local_size_; };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!ParentDIA::stack_empty) return false;
        local_size_ = file.num_items();
        return true;
    }

    //! Executes the size operation.
    void Execute() final {
        MainOp();
    }

    /*!
     * Returns result of global size.
     * \return result
     */
    size_t result() const {
        return global_size_;
    }

private:
    // Local size to be used.
    size_t local_size_ = 0;
    // Global size resulting from all reduce.
    size_t global_size_ = 0;

    void MainOp() {
        // get the number of elements that are stored on this worker
        LOG << "MainOp processing, sum: " << local_size_;

        // process the reduce, default argument is SumOp.
        global_size_ = context_.net.AllReduce(local_size_);
    }
};

template <typename ValueType, typename Stack>
size_t DIA<ValueType, Stack>::Size() const {
    assert(IsValid());

    using SizeNode = api::SizeNode<DIA>;

    auto node = std::make_shared<SizeNode>(*this);

    node->RunScope();

    return node->result();
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SIZE_HEADER

/******************************************************************************/
