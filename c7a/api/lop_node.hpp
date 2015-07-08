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
 * \tparam ParentType ParentType type of the Reduce operation
 *
 * \tparam LOpStack Function chain, which contains the chained lambdas between
 * the last and this DIANode.
 */
template <typename ParentType, typename LOpStack>
class LOpNode : public DIANode<ParentType>
{
public:
    using Super = DIANode<ParentType>;
    using Super::context_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param parent Parent DIANode.
     * \param lop_stack Function chain with all lambdas between the parent and this node
     */
    LOpNode(Context& ctx,
            DIANode<ParentType>* parent,
            LOpStack& lop_stack)
        : DIANode<ParentType>(ctx, { parent }),
          lop_stack_(lop_stack)
    { }

    //! Virtual destructor for a LOpNode.
    virtual ~LOpNode() { }

    /*!
     * Actually executes the local operations.
     */
    void Execute() override {
        // Execute LOpChain
        data::DIAId pid = this->get_parents()[0]->get_data_id();
        // //get data from data manager
        auto it = context_.get_data_manager().template GetIterator<ParentType>(pid);

        std::vector<ParentType> elements;
        auto save_fn = [&elements](ParentType input) {
                           elements.push_back(input);
                       };
        auto lop_chain = lop_stack_.push(save_fn).emit();

        // loop over input
        while (it.HasNext()) {
            lop_chain(it.Next());
        }

        // Emit new elements
        auto emit = context_.get_data_manager().
                    template GetLocalEmitter<ParentType>(DIABase::data_id_);
        for (auto elem : elements) {
            emit(elem);
        }
    }

    /*!
     * Returns "[LOpNode]" and its id as a string.
     * \return "[LOpNode]"
     */
    std::string ToString() override {
        return "[LOpNode] Id: " + DIABase::data_id_.ToString();
    }

private:
    //! Local stack
    LOpStack lop_stack_;
};

} // namespace api
} // namespace c7a

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
