/*******************************************************************************
 * thrill/api/action_node.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
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
               const std::string& stats_tag,
               StatsNode* stats_node)
        : DIABase(ctx, parents, stats_tag, stats_node)
    { }

    //! ActionNodes do not have children.
    void UnregisterChilds() final { }

    //! Actionnodes do not push data, they only Execute.
    void PushData() final { }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ACTION_NODE_HEADER

/******************************************************************************/
