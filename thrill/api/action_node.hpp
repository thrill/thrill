/*******************************************************************************
 * thrill/api/action_node.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ACTION_NODE_HEADER
#define THRILL_API_ACTION_NODE_HEADER

#include <thrill/api/dia_node.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

class ActionNode : public DIABase
{
public:
    ActionNode(Context& ctx,
               const std::vector<std::shared_ptr<DIABase> >& parents,
               StatsNode* stats_node)
        : DIABase(ctx, parents, stats_node)
    { }

    //! ActionNodes do not have children.
    void UnregisterChilds() final { }

    //! Actionnodes do not push data, they only Execute.
    void PushData(bool /* consume */) final { }

    void SetConsume(bool /* consume */) final {
        die("Setting .Keep() or .Consume() on Actions does not make sense.");
    }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ACTION_NODE_HEADER

/******************************************************************************/
