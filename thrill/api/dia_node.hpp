/*******************************************************************************
 * thrill/api/dia_node.hpp
 *
 * Part of Project Thrill.
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

template <typename ValueType>
struct CallbackPair {
    CallbackPair(const std::function<void(const ValueType&)>& cb,
                 DIANodeType type)
        : cb_(cb), type_(type) { }

    void operator () (const ValueType& elem) const {
        cb_(elem);
    }

    //! callback to invoke (currently for each item)
    std::function<void(const ValueType&)> cb_;
    //! node invoked.
    DIANodeType                           type_;
};

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
    /*!
     * Default constructor for a DIANode.
     */
    DIANode() { }

    /*!
     * Constructor for a DIANode, which sets references to the
     * parent nodes. Calls the constructor of DIABase with the same parameters.
     *
     * \param ctx Reference to Context, which holds references to data and
     * network.
     *
     * \param parents Reference to parents of this node, which have to be
     * computed previously
     */
    DIANode(Context& ctx,
            const std::vector<std::shared_ptr<DIABase> >& parents,
            StatsNode* stats_node)
        : DIABase(ctx, parents, stats_node)
    { }

    //! Virtual destructor for a DIANode.
    virtual ~DIANode() { }

    /*!
     * Enables children to push their "folded" function chains to their parent.
     * This way the parent can push all its result elements to each of the
     * children.  This procedure enables the minimization of IO-accesses.
     *
     * \param callback Callback function from the child including all
     * locally processable operations between the parent and child.
     */
    void RegisterChild(const std::function<void(const ValueType&)>& callback,
                       const DIANodeType& child_type) {
        callbacks_.emplace_back(callback, child_type);
    }

    void UnregisterChilds() final {
        callbacks_.erase(
            std::remove_if(
                callbacks_.begin(), callbacks_.end(),
                [](const auto& cb) { return cb.type_ != DIANodeType::COLLAPSE; }),
            callbacks_.end());

        children_.erase(
            std::remove_if(
                children_.begin(), children_.end(),
                [](const auto& c) { return c->type() != DIANodeType::COLLAPSE; }),
            children_.end());
    }

    std::vector<CallbackPair<ValueType> > & callbacks() {
        return callbacks_;
    }

    void callback_functions(std::vector<std::function<void(const ValueType&)> >& cbs) {
        for (auto& cb_pair : callbacks_) cbs.push_back(cb_pair.cb_);
    }

    void PushItem(const ValueType& elem) const {
        for (const auto& callback : callbacks_) {
            callback(elem);
        }
    }

private:
    //! Callback functions from the child nodes.
    std::vector<CallbackPair<ValueType> > callbacks_;
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_NODE_HEADER

/******************************************************************************/
