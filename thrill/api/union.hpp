/*******************************************************************************
 * thrill/api/union.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_UNION_HEADER
#define THRILL_API_UNION_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>
#include <tlx/meta/call_foreach_with_index.hpp>
#include <tlx/meta/vexpand.hpp>

#include <algorithm>
#include <initializer_list>
#include <vector>

namespace thrill {
namespace api {

/*!
 * Implements a Union operation by hooking any number of input DIANodes and
 * forwarding the output immediately to all children.
 *
 * The problem with Union is that children can be added to the node after
 * creation (as with all other nodes). This however requires the UnionNode to
 * remember which of its children has already got which input's items. This is
 * recorded in each UnionChild's pushed_inputs array.
 *
 * For example this occurs in the following DIA graph:
 *
 *   [Gen.1] ------v
 *                 [Union.3] --> [Sort.4] -------> [Size.6]
 *             +---^       +-------------------------------> [Size.7]
 *   [Gen.2] --+
 *             +------------------------> [Size.5]
 *
 * Size.5 triggers Execute and PushData such that Sort.4's PreOp gets all data
 * from Gen.2, but not from Gen.1. Then, running Size.6 requires Union.3 to get
 * data from Gen.1 and NOT run Gen.2 again.
 *
 * Another situation then occur, when [Size.7] is added later.
 *
 * \ingroup api_layer
 */
template <typename ValueType>
class UnionNode final : public DIANode<ValueType>
{
    static constexpr bool debug = false;

public:
    using Super = DIANode<ValueType>;
    using Super::context_;
    using Callback = typename Super::Callback;

    enum class ChildStatus { NEW, PUSHING, DONE };

    struct UnionChild {
        //! reference to child node
        DIABase             * node;
        //! callback to invoke (currently for each item)
        Callback            callback;
        //! index this node has among the parents of the child (passed to
        //! callbacks), e.g. for ZipNode which has multiple parents and their
        //! order is important.
        size_t              parent_index;
        //! status of the child.
        ChildStatus         status;
        //! vector of inputs which were delivered to this child.
        std::vector<size_t> pushed_inputs;

        //! check if all inputs were pushed to the child
        bool                AllInputsDone() const {
            for (const size_t& i : pushed_inputs)
                if (!i) return false;
            return true;
        }
    };

    //! Constructor for variant with variadic parent parameter pack, which each
    //! parent may have a different FunctionStack.
    template <typename ParentDIA0, typename... ParentDIAs>
    explicit UnionNode(const ParentDIA0& parent0,
                       const ParentDIAs& ... parents)
        : Super(parent0.ctx(), "Union",
                { parent0.id(), parents.id() ... },
                { parent0.node(), parents.node() ... }),
          num_inputs_(1 + sizeof ... (ParentDIAs))
    {
        tlx::call_foreach_with_index(
            RegisterParent(this), parent0, parents...);
    }

    //! Constructor for variant with a std::vector of parents all with the same
    //! (usually empty) FunctionStack.
    template <typename ParentDIA>
    explicit UnionNode(const std::vector<ParentDIA>& parents)
        : Super(parents.front().ctx(), "Union",
                common::MapVector(
                    parents, [](const ParentDIA& d) { return d.id(); }),
                common::MapVector(
                    parents, [](const ParentDIA& d) {
                        return DIABasePtr(d.node().get());
                    })),
          num_inputs_(parents.size())
    {
        for (size_t i = 0; i < num_inputs_; ++i)
        {
            auto propagate_fn = [this, i](const ValueType& input) {
                                    this->PushItem(input, i);
                                };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parents[i].stack().push(propagate_fn).fold();
            parents[i].node()->AddChild(this, lop_chain, i);
        }
    }

    //! Constructor for variant with a std::initializer_list of parents all with
    //! the same (usually empty) FunctionStack.
    template <typename ParentDIA>
    explicit UnionNode(const std::initializer_list<ParentDIA>& parents)
        : UnionNode(std::vector<ParentDIA>(parents)) { }

    //! Register Parent Hooks, operator() is instantiated and called for each
    //! Union parent
    class RegisterParent
    {
    public:
        explicit RegisterParent(UnionNode* union_node)
            : union_node_(union_node) { }

        template <typename Index, typename Parent>
        void operator () (const Index&, Parent& parent) {

            UnionNode* union_node = union_node_;
            auto propagate_fn = [union_node](const ValueType& input) {
                                    union_node->PushItem(input, Index::index);
                                };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parent.stack().push(propagate_fn).fold();
            parent.node()->AddChild(union_node_, lop_chain, Index::index);
        }

    private:
        UnionNode* union_node_;
    };

    /*!
     * Enables children to push their "folded" function chains to their parent.
     * This way the parent can push all its result elements to each of the
     * children. This procedure enables the minimization of IO-accesses.
     */
    void AddChild(DIABase* node, const Callback& callback,
                  size_t parent_index = 0) final {
        children_.emplace_back(UnionChild {
                                   node, callback, parent_index,
                                   ChildStatus::NEW, std::vector<size_t>(num_inputs_)
                               });
    }

    //! Remove a child from the vector of children. This method is called by the
    //! destructor of children.
    void RemoveChild(DIABase* node) final {
        typename std::vector<UnionChild>::iterator swap_end =
            std::remove_if(
                children_.begin(), children_.end(),
                [node](const UnionChild& c) { return c.node == node; });

        // assert(swap_end != children_.end());
        children_.erase(swap_end, children_.end());
    }

    void RemoveAllChildren() final {
        // remove all children other than Collapse and Union nodes
        children_.erase(
            std::remove_if(
                children_.begin(), children_.end(),
                [this](UnionChild& child) {
                    if (child.status != ChildStatus::DONE)
                        return false;
                    if (child.node->ForwardDataOnly())
                        return false;
                    child.node->RemoveParent(this);
                    return true;
                }),
            children_.end());

        // recurse into remaining nodes (CollapseNode and UnionNode)
        for (UnionChild& child : children_) {
            if (!child.node->ForwardDataOnly())
                continue;
            child.node->RemoveAllChildren();
        }
    }

    //! Returns the children of this DIABase.
    std::vector<DIABase*> children() const final {
        std::vector<DIABase*> out;
        out.reserve(children_.size());
        for (const UnionChild& child : children_)
            out.emplace_back(child.node);
        return out;
    }

    //! A UnionNode cannot be executed, it never contains any data.
    bool ForwardDataOnly() const final { return true; }

    //! Check whether we need PushData() from the specific parent
    bool RequireParentPushData(size_t parent_index) const final {
        for (const UnionChild& child : children_) {
            if (!child.pushed_inputs[parent_index]) return true;
        }
        return false;
    }

    void Execute() final { abort(); }

    void StartPreOp(size_t parent_index) final {
        LOG0 << "UnionNode::StartPreOp parent_index=" << parent_index;
        for (UnionChild& child : children_) {
            if (child.status == ChildStatus::NEW) {
                // start push to NEW child
                LOG << "UnionNode::StartPreOp triggered"
                    << " StartPreOp on child " << *child.node;
                child.node->StartPreOp(child.parent_index);
                child.status = ChildStatus::PUSHING;
            }
        }
    }

    //! Method for derived classes to Push a single item to all children.
    void PushItem(const ValueType& item, size_t parent_index) const {
        for (const UnionChild& child : children_) {
            if (!child.pushed_inputs[parent_index])
                child.callback(item);
        }
    }

    void StopPreOp(size_t parent_index) final {
        LOG0 << "UnionNode::StopPreOp parent_index=" << parent_index;
        for (UnionChild& child : children_) {
            if (child.status == ChildStatus::PUSHING) {
                assert(!child.pushed_inputs[parent_index]);
                child.pushed_inputs[parent_index] = 1;
            }
            if (child.AllInputsDone()) {
                LOG << "UnionNode::StopPreOp triggered"
                    << " StopPreOp on child " << *child.node;
                child.node->StopPreOp(child.parent_index);
                child.status = ChildStatus::DONE;
            }
        }
    }

    void RunPushData() final { abort(); }

    void PushData(bool /* consume */) final { abort(); }

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
    size_t num_inputs_;

    //! Callback functions from the child nodes.
    std::vector<UnionChild> children_;
};

/*!
 * Union is a LOp, which creates the union of all items from any number of DIAs
 * as a single DIA, where the items are in an arbitrary order.  All input DIAs
 * must contain the same type, which is also the output DIA's type.
 *
 * The Union operation concatenates all _local_ pieces of a DIA, no rebalancing
 * is performed, and no communication is needed.
 *
 * \param first_dia first DIA
 * \param dias DIAs, which are unified with the first DIA.
 *
 * \ingroup dia_lops
 */
template <typename FirstDIA, typename... DIAs>
auto Union(const FirstDIA& first_dia, const DIAs& ... dias) {

    tlx::vexpand((first_dia.AssertValid(), 0), (dias.AssertValid(), 0) ...);

    using ValueType = typename FirstDIA::ValueType;

    using UnionNode = api::UnionNode<ValueType>;

    return DIA<ValueType>(tlx::make_counting<UnionNode>(first_dia, dias...));
}

/*!
 * Union is a LOp, which creates the union of all items from any number of DIAs
 * as a single DIA, where the items are in an arbitrary order.  All input DIAs
 * must contain the same type, which is also the output DIA's type.
 *
 * The Union operation concatenates all _local_ pieces of a DIA, no rebalancing
 * is performed, and no communication is needed.
 *
 * \param dias DIAs, which are unified.
 *
 * \ingroup dia_lops
 */
template <typename ValueType>
auto Union(const std::initializer_list<DIA<ValueType> >& dias) {

    for (const DIA<ValueType>& d : dias)
        d.AssertValid();

    using UnionNode = api::UnionNode<ValueType>;

    return DIA<ValueType>(tlx::make_counting<UnionNode>(dias));
}

/*!
 * Union is a LOp, which creates the union of all items from any number of DIAs
 * as a single DIA, where the items are in an arbitrary order.  All input DIAs
 * must contain the same type, which is also the output DIA's type.
 *
 * The Union operation concatenates all _local_ pieces of a DIA, no rebalancing
 * is performed, and no communication is needed.
 *
 * \param dias DIAs, which are unified.
 *
 * \ingroup dia_lops
 */
template <typename ValueType>
auto Union(const std::vector<DIA<ValueType> >& dias) {

    for (const DIA<ValueType>& d : dias)
        d.AssertValid();

    using UnionNode = api::UnionNode<ValueType>;

    return DIA<ValueType>(tlx::make_counting<UnionNode>(dias));
}

template <typename ValueType, typename Stack>
template <typename SecondDIA>
auto DIA<ValueType, Stack>::Union(
    const SecondDIA& second_dia) const {
    return api::Union(*this, second_dia);
}

} // namespace api

//! imported from api namespace
using api::Union;

} // namespace thrill

#endif // !THRILL_API_UNION_HEADER

/******************************************************************************/
