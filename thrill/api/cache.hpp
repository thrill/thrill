/*******************************************************************************
 * c7a/api/cache.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_CACHE_HEADER
#define C7A_API_CACHE_HEADER

#include <c7a/api/dia.hpp>
#include <c7a/api/dia_node.hpp>
#include <c7a/data/file.hpp>

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
template <typename ValueType, typename ParentDIARef>
class CacheNode : public DIANode<ValueType>
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
    CacheNode(const ParentDIARef& parent,
              const std::string& stats_tag,
              StatsNode* stats_node)
        : DIANode<ValueType>(parent.ctx(), { parent.node() }, stats_tag, stats_node)
    {
        auto save_fn =
            [=](const ValueType& input) {
                writer_(input);
            };
        auto lop_chain = parent.stack().push(save_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Virtual destructor for a LOpNode.
    virtual ~CacheNode() { }

    /*!
     * Pushes elements to next node.
     * Can be skipped for LOps.
     */
    void Execute() final {
        // Push local elements to children
        writer_.Close();
    }

    void PushData() final {
        data::File::Reader reader = file_.GetReader();
        for (size_t i = 0; i < file_.NumItems(); ++i) {
            this->PushElement(reader.Next<ValueType>());
        }
    }

    void Dispose() final { }

    /*!
     * Returns "[CacheNode]" and its id as a string.
     * \return "[CacheNode]"
     */
    std::string ToString() final {
        return "[CacheNode] Id: " + result_file_.ToString();
    }

private:
    //! Local data file
    data::File file_;
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ = file_.GetWriter();
};

template <typename ValueType, typename Stack>
template <typename AnyStack>
DIARef<ValueType, Stack>::DIARef(const DIARef<ValueType, AnyStack>& rhs) {
    // Create new LOpNode. Transfer stack from rhs to LOpNode. Build new
    // DIARef with empty stack and LOpNode
    using LOpChainNode = CacheNode<ValueType, DIARef>;

    LOG0 << "WARNING: cast to DIARef creates LOpNode instead of inline chaining.";
    LOG0 << "Consider whether you can use auto instead of DIARef.";

    auto shared_node
        = std::make_shared<LOpChainNode>(rhs, "");
    node_ = std::move(shared_node);
}

template <typename ValueType, typename Stack>
auto DIARef<ValueType, Stack>::Cache() const {
    // Create new LOpNode. Transfer stack from rhs to LOpNode. Build new
    // DIARef with empty stack and LOpNode
    using LOpChainNode = CacheNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("LOp", NodeType::CACHE);
    auto shared_node
        = std::make_shared<LOpChainNode>(*this, "", stats_node);
    auto lop_stack = FunctionStack<ValueType>();

    return DIARef<ValueType, decltype(lop_stack)>(
        shared_node, lop_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_CACHE_HEADER

/******************************************************************************/
