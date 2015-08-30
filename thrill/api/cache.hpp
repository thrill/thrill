/*******************************************************************************
 * thrill/api/cache.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_CACHE_HEADER
#define THRILL_API_CACHE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/data/file.hpp>

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
class CacheNode : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;

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

    void PushData(bool consume) final {
        data::File::Reader reader = file_.GetReader(consume);
        for (size_t i = 0; i < file_.num_items(); ++i) {
            this->PushItem(reader.Next<ValueType>());
        }
    }

    void Dispose() final { }

    /*!
     * Returns "[CacheNode]" and its id as a string.
     * \return "[CacheNode]"
     */
    std::string ToString() final {
        return "[CacheNode] Id: " + std::to_string(this->id());
    }

private:
    //! Local data file
    data::File file_ { context_.GetFile() };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ = file_.GetWriter();
};

template <typename ValueType, typename Stack>
template <typename AnyStack>
DIARef<ValueType, Stack>::DIARef(const DIARef<ValueType, AnyStack>& rhs) {
    assert(IsValid());

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
    assert(IsValid());

    // Create new LOpNode. Transfer stack from rhs to LOpNode. Build new
    // DIARef with empty stack and LOpNode
    using LOpChainNode = CacheNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("LOp", DIANodeType::CACHE);
    auto shared_node
        = std::make_shared<LOpChainNode>(*this, "", stats_node);
    auto lop_stack = FunctionStack<ValueType>();

    return DIARef<ValueType, decltype(lop_stack)>(
        shared_node, lop_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_CACHE_HEADER

/******************************************************************************/
