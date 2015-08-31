/*******************************************************************************
 * thrill/api/source_node.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SOURCE_NODE_HEADER
#define THRILL_API_SOURCE_NODE_HEADER

#include <thrill/api/dia_node.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType>
class SourceNode : public DIANode<ValueType>
{
public:
    SourceNode(Context& ctx,
               const std::vector<std::shared_ptr<DIABase> >& parents,
               StatsNode* stats_node)
        : DIANode<ValueType>(ctx, parents, stats_node)
    { }

    //! SourceNodes generally do not Execute, they only PushData.
    void Execute() override { }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SOURCE_NODE_HEADER

/******************************************************************************/
