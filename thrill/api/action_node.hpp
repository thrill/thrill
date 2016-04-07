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

    //! ActionNodes are so short-lived they need not be Disposed.
    void Dispose() final { }

    //! ActionNodes do not push data, they only Execute.
    void PushData(bool /* consume */) final { abort(); }

    //! ActionNodes do not push data, they only Execute.
    void RunPushData() final { abort(); }

    void IncConsumeCounter(size_t /* counter */) final {
        die("Setting .Keep() on Actions does not make sense.");
    }

    void SetConsumeCounter(size_t /* counter */) final {
        die("Setting .Keep() on Actions does not make sense.");
    }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ACTION_NODE_HEADER

/******************************************************************************/
