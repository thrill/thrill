/*******************************************************************************
 * thrill/api/collapse.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_COLLAPSE_HEADER
#define THRILL_API_COLLAPSE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>

#include <algorithm>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class CollapseNode final : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     */
    template <typename ParentDIA>
    explicit CollapseNode(const ParentDIA& parent)
        : Super(parent.ctx(), "Collapse", { parent.id() }, { parent.node() }),
          parent_stack_empty_(ParentDIA::stack_empty)
    {
        auto propagate_fn = [this](const ValueType& input) {
                                this->PushItem(input);
                            };
        auto lop_chain = parent.stack().push(propagate_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! A CollapseNode cannot be executed, it never contains any data.
    bool ForwardDataOnly() const final { return true; }

    bool RequireParentPushData(size_t /* parent_index */) const final
    { return true; }

    void Execute() final { abort(); }

    void StartPreOp(size_t /* id */) final {
        for (typename Super::Child & child : Super::children_)
            child.node->StartPreOp(child.parent_index);
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOG1 << "Collapse rejected File from parent "
                 << "due to non-empty function stack.";
            return false;
        }

        // forward file
        LOG1 << "Collapse accepted File from parent";
        data::File file_copy = file.Copy();
        this->PushFile(file_copy, /* consume */ true);
        return true;
    }

    void StopPreOp(size_t /* id */) final {
        for (typename Super::Child & child : Super::children_)
            child.node->StopPreOp(child.parent_index);
    }

    void PushData(bool /* consume */) final { }

    size_t consume_counter() const final {
        // calculate consumption of parents
        size_t c = Super::kNeverConsume;
        for (auto& p : Super::parents_) {
            c = std::min(c, p->consume_counter());
        }
        return c;
    }

    void IncConsumeCounter(size_t consume) final {
        // propagate consumption up to parents.
        for (auto& p : Super::parents_) {
            p->IncConsumeCounter(consume);
        }
    }

    void DecConsumeCounter(size_t consume) final {
        // propagate consumption up to parents.
        for (auto& p : Super::parents_) {
            p->DecConsumeCounter(consume);
        }
    }

    void SetConsumeCounter(size_t consume) final {
        // propagate consumption up to parents.
        for (auto& p : Super::parents_) {
            p->SetConsumeCounter(consume);
        }
    }

private:
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;
};

#ifndef THRILL_DOXYGEN_IGNORE

template <typename ValueType, typename Stack>
template <typename AnyStack>
DIA<ValueType, Stack>::DIA(const DIA<ValueType, AnyStack>& rhs)
// Create new CollapseNode. Transfer stack from rhs to CollapseNode. Build new
// DIA with empty stack and CollapseNode
    : DIA(common::MakeCounting<api::CollapseNode<ValueType> >(rhs)) {
    LOG0 << "WARNING: cast to DIA creates CollapseNode instead of inline chaining.";
    LOG0 << "Consider whether you can use auto instead of DIA.";
}

#endif  // THRILL_DOXYGEN_IGNORE

//! Template switch to generate a CollapseNode if there is a non-empty Stack
template <typename ValueType, typename Stack>
struct CollapseSwitch {
    static DIA<ValueType> MakeCollapse(const DIA<ValueType, Stack>& dia) {
        assert(dia.IsValid());

        // Create new CollapseNode. Transfer stack from rhs to
        // CollapseNode. Build new DIA with empty stack and CollapseNode
        using CollapseNode = api::CollapseNode<ValueType>;

        return DIA<ValueType>(common::MakeCounting<CollapseNode>(dia));
    }
};

//! Template switch to NOT generate a CollapseNode if there is an empty Stack.
template <typename ValueType>
struct CollapseSwitch<ValueType, FunctionStack<ValueType> >{
    static DIA<ValueType> MakeCollapse(
        const DIA<ValueType, FunctionStack<ValueType> >& dia) {
        return dia;
    }
};

template <typename ValueType, typename Stack>
DIA<ValueType> DIA<ValueType, Stack>::Collapse() const {
    return CollapseSwitch<ValueType, Stack>::MakeCollapse(*this);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_COLLAPSE_HEADER

/******************************************************************************/
