/*******************************************************************************
 * thrill/api/dia_node.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DIA_NODE_HEADER
#define THRILL_API_DIA_NODE_HEADER

#include <thrill/api/dia_base.hpp>
#include <thrill/common/stats.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode is a typed node representing and operation in Thrill. It is the
 * super class for all operation nodes and stores the state of the
 * operation. The type of a DIANode is the type, in which the DIA is after the
 * last global barrier in the operation (between MainOp and PostOp).
 *
 * \tparam ValueType Type of the DIA between MainOp and PostOp
 */
template <typename ValueType>
class DIANode : public DIABase
{
public:
    using Callback = std::function<void(const ValueType&)>;

    struct Child {
        Child(DIABase* node, const Callback& callback, size_t parent_index)
            : node_(node), callback_(callback), parent_index_(parent_index) { }

        //! reference to child node
        DIABase  * node_;
        //! callback to invoke (currently for each item)
        Callback callback_;
        //! index this node has among the parents of the child (passed to
        //! callbacks), e.g. for ZipNode which has multiple parents and their order
        //! is important.
        size_t   parent_index_;
    };

    //! Default constructor for a DIANode.
    DIANode() = default;

    /*!
     * Constructor for a DIANode, which sets references to the
     * parent nodes. Calls the constructor of DIABase with the same parameters.
     */
    DIANode(Context& ctx,
            const std::vector<std::shared_ptr<DIABase> >& parents,
            StatsNode* stats_node)
        : DIABase(ctx, parents, stats_node)
    { }

    /*!
     * Enables children to push their "folded" function chains to their parent.
     * This way the parent can push all its result elements to each of the
     * children. This procedure enables the minimization of IO-accesses.
     */
    void AddChild(DIABase* node,
                  const Callback& callback,
                  size_t parent_index = 0) {
        children_.emplace_back(node, callback, parent_index);
    }

    //! Remove a child from the vector of children. This method is called by the
    //! destructor of children.
    void RemoveChild(DIABase* node) final {
        typename std::vector<Child>::iterator swap_end =
            std::remove_if(
                children_.begin(), children_.end(),
                [node](const Child& c) { return c.node_ == node; });

        // assert(swap_end != children_.end());
        children_.erase(swap_end, children_.end());
    }

    void RemoveAllChildren() final {
        // remove all children other than Collapse nodes
        children_.erase(
            std::remove_if(
                children_.begin(), children_.end(),
                [](const Child& child) {
                    return child.node_->type() != DIANodeType::COLLAPSE;
                }),
            children_.end());

        // recurse into remaining nodes (CollapseNode)
        for (Child& child : children_)
            child.node_->RemoveAllChildren();
    }

    //! Returns the children of this DIABase.
    std::vector<DIABase*> children() const final {
        std::vector<DIABase*> out;
        out.reserve(children_.size());
        for (const Child& child : children_)
            out.emplace_back(child.node_);
        return out;
    }

    //! Performing push operation. Notifies children and calls actual push
    //! method. Then cleans up the DIA graph by freeing parent references of
    //! children.
    void RunPushData(bool consume) final {
        for (const Child& child : children_)
            child.node_->StartPreOp(child.parent_index_);

        PushData(consume && context().consume());

        for (const Child& child : children_)
            child.node_->StopPreOp(child.parent_index_);
    }

    void PushItem(const ValueType& elem) const {
        for (const Child& child : children_) {
            child.callback_(elem);
        }
    }

protected:
    //! Callback functions from the child nodes.
    std::vector<Child> children_;
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_NODE_HEADER

/******************************************************************************/
