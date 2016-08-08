/*******************************************************************************
 * thrill/api/action_node.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ACTION_NODE_HEADER
#define THRILL_API_ACTION_NODE_HEADER

#include <thrill/api/dia_base.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \ingroup api_layer
//! \{

class ActionNode : public DIABase
{
public:
    ActionNode(Context& ctx, const char* label,
               const std::initializer_list<size_t>& parent_ids,
               const std::initializer_list<DIABasePtr>& parents)
        : DIABase(ctx, label, parent_ids, parents) { }

    //! ActionNodes do not have children.
    void RemoveChild(DIABase* /* node */) final { }

    //! ActionNodes do not have children.
    void RemoveAllChildren() final { }

    //! ActionNodes do not have children.
    std::vector<DIABase*> children() const final
    { return std::vector<DIABase*>(); }

    //! ActionNodes do not push data, they only Execute.
    void PushData(bool /* consume */) final { abort(); }

    //! ActionNodes do not push data, they only Execute.
    void RunPushData() final { abort(); }

    void IncConsumeCounter(size_t /* counter */) final {
        die("Setting .Keep() on Actions does not make sense.");
    }

    void DecConsumeCounter(size_t /* counter */) final {
        die("Setting .Keep() on Actions does not make sense.");
    }

    void SetConsumeCounter(size_t /* counter */) final {
        die("Setting .Keep() on Actions does not make sense.");
    }
};

template <typename ResultType>
class ActionResultNode : public ActionNode
{
public:
    ActionResultNode(Context& ctx, const char* label,
                     const std::initializer_list<size_t>& parent_ids,
                     const std::initializer_list<DIABasePtr>& parents)
        : ActionNode(ctx, label, parent_ids, parents) { }

    //! virtual method to return result via an ActionFuture
    virtual const ResultType& result() const = 0;
};

/*!
 * The return type class for all ActionFutures. This is not a multi-threading
 * Future, instead it is only a variable placeholder containing a pointer to the
 * action node to retrieve the value once it is calculated.
 */
template <typename ValueType = void>
class Future
{
public:
    using ActionResultNodePtr =
              common::CountingPtr<ActionResultNode<ValueType> >;

    explicit Future(const ActionResultNodePtr& node)
        : node_(node) { }

    //! Evaluate the DIA data-flow graph for this ActionFuture.
    void wait() {
        if (node_->state() == DIAState::NEW)
            node_->RunScope();
    }

    //! true if already executed/valid
    bool valid() const {
        return (node_->state() == DIAState::EXECUTED);
    }

    //! Return the value of the ActionFuture
    const ValueType& get() {
        if (node_->state() == DIAState::NEW)
            node_->RunScope();

        return node_->result();
    }

    //! Return the value of the ActionFuture
    const ValueType& operator () () {
        return get();
    }

private:
    //! shared pointer to the action node, which may not be executed yet.
    ActionResultNodePtr node_;
};

/*!
 * Specialized template class for ActionFuture which return void. This class
 * does not have a get() method.
 */
template <>
class Future<void>
{
public:
    using ActionNodePtr = common::CountingPtr<ActionNode>;

    explicit Future(const ActionNodePtr& node)
        : node_(node) { }

    //! Evaluate the DIA data-flow graph for this ActionFuture.
    void wait() {
        if (node_->state() == DIAState::NEW)
            node_->RunScope();
    }

    //! true if already executed/valid
    bool valid() const {
        return (node_->state() == DIAState::EXECUTED);
    }

    //! Return the value of the ActionFuture
    void operator () () {
        return wait();
    }

private:
    //! shared pointer to the action node, which may not be executed yet.
    ActionNodePtr node_;
};

//! \}

} // namespace api

//! imported from api namespace
template <typename ValueType = void>
using Future = api::Future<ValueType>;

} // namespace thrill

#endif // !THRILL_API_ACTION_NODE_HEADER

/******************************************************************************/
