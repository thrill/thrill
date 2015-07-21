/*******************************************************************************
 * c7a/api/lop_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_LOP_NODE_HEADER
#define C7A_API_LOP_NODE_HEADER

#include <c7a/api/dia_node.hpp>

#include <string>
#include <vector>

namespace c7a {
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
template <typename ValueType, typename ParentStack>
class LOpNode : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;
    using Super::result_file_;
    using ParentInput = typename ParentStack::Input;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param parent Parent DIANode.
     * \param lop_stack Function chain with all lambdas between the parent and this node
     */
    LOpNode(Context& ctx,
            const std::shared_ptr<DIANode<ParentInput> >& parent,
            const ParentStack& lop_stack, const std::string& stats_tag)
        : DIANode<ValueType>(ctx, { parent }, stats_tag)
    {
        auto save_fn =
            [=](ValueType input) {
                for (const std::function<void(ValueType)>& func : this->callbacks_)
                    func(input);
            };
        auto lop_chain = lop_stack.push(save_fn).emit();
        parent->RegisterChild(lop_chain);
    }

    //! Virtual destructor for a LOpNode.
    virtual ~LOpNode() { }

    /*!
     * Pushes elements to next node.
     * Can be skipped for LOps.
     */
    void Execute() override { }

    /*!
     * Returns "[LOpNode]" and its id as a string.
     * \return "[LOpNode]"
     */
    std::string ToString() override {
        return "[LOpNode] Id: " + result_file_.ToString();
    }
};

template <typename ValueType, typename Stack>
template <typename AnyStack>
DIARef<ValueType, Stack>::DIARef(const DIARef<ValueType, AnyStack>& rhs) {
    // Create new LOpNode. Transfer stack from rhs to LOpNode. Build new
    // DIARef with empty stack and LOpNode
    using LOpChainNode = LOpNode<ValueType, AnyStack>;

    LOG0 << "WARNING: cast to DIARef creates LOpNode instead of inline chaining.";
    LOG0 << "Consider whether you can use auto instead of DIARef.";

    auto shared_node
        = std::make_shared<LOpChainNode>(rhs.node()->context(),
                                         rhs.node(),
                                         rhs.stack(), "");
    node_ = std::move(shared_node);
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
