/*******************************************************************************
 * thrill/api/collapse.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_COLLAPSE_HEADER
#define THRILL_API_COLLAPSE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A LOpNode which performs a chain of local operations.  LOp nodes are used for
 * caching local operation results and assignment operations.
 *
 * \tparam ParentStack Function chain, which contains the chained lambdas between
 * the last and this DIANode.
 */
template <typename ValueType, typename ParentDIARef>
class CollapseNode : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;
    using Super::result_file_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     *
     * \param parent Parent DIARef.
     */
    CollapseNode(const ParentDIARef& parent,
                 const std::string& stats_tag,
                 StatsNode* stats_node)
        : DIANode<ValueType>(parent.ctx(), { parent.node() }, stats_tag, stats_node)
    {
        auto propagate_fn = [=](ValueType input) {
                                this->PushItem(input);
                            };
        auto lop_chain = parent.stack().push(propagate_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Virtual destructor for a LOpNode.
    virtual ~CollapseNode() { }

    /*!
     * Pushes elements to next node.
     * Can be skipped for LOps.
     */
    void Execute() final { }

    void PushData() final { }

    void Dispose() final { }

    /*!
     * Returns "[CollapseNode]" and its id as a string.
     * \return "[CollapseNode]"
     */
    std::string ToString() final {
        return "[CollapseNode] Id: " + result_file_.ToString();
    }
};

template <typename ValueType, typename Stack>
auto DIARef<ValueType, Stack>::Collapse() const {
    assert(IsValid());

    // Create new LOpNode. Transfer stack from rhs to LOpNode. Build new
    // DIARef with empty stack and LOpNode
    using LOpChainNode = CollapseNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("LOp", DIANodeType::COLLAPSE);
    auto shared_node
        = std::make_shared<LOpChainNode>(*this, "", stats_node);
    auto lop_stack = FunctionStack<ValueType>();

    return DIARef<ValueType, decltype(lop_stack)>(
        shared_node, lop_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_COLLAPSE_HEADER

/******************************************************************************/
