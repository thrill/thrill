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
    using Super = DIANode<ValueType>;

    SourceNode(Context& ctx,
               const std::vector<std::shared_ptr<DIABase> >& parents,
               StatsNode* stats_node)
        : DIANode<ValueType>(ctx, parents, stats_node) {
        // SourceNode are kept by default: they usually read files or databases
        // on PushData(), which should not be consumed.
        Super::consume_on_push_data_ = false;
    }

    //! SourceNodes generally do not Execute, they only PushData.
    void Execute() override { }

    //! Print error when trying to set consume to true.
    void SetConsume(bool consume) final {
        if (consume)
            die("You cannot set a SourceNode to consume its data.");
    }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SOURCE_NODE_HEADER

/******************************************************************************/
