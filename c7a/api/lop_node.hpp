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

//! \addtogroup api Interface
//! \{

/*!
 * A LOpNode which performs a chain of local operations.  LOp nodes are used for
 * caching local operation results and assignment operations.
 *
 * \tparam Input Input type of the Reduce operation
 *
 * \tparam LOpStack Function chain, which contains the chained lambdas between
 * the last and this DIANode.
 */
template <typename Input, typename LOpStack>
class LOpNode : public DIANode<Input>
{
public:
    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param parent Parent DIANode.
     * \param lop_stack Function chain with all lambdas between the parent and this node
     */
    LOpNode(Context& ctx,
            DIANode<Input>* parent,
            LOpStack& lop_stack)
        : DIANode<Input>(ctx, { parent }),
          lop_stack_(lop_stack)
    { }

    //! Virtual destructor for a LOpNode.
    virtual ~LOpNode() { }

    /*!
     * Actually executes the local operations.
     */
    void execute() override {
        // Execute LOpChain
        data::DIAId pid = this->get_parents()[0]->get_data_id();
        // //get data from data manager
        data::BlockIterator<Input> it = (this->context_).get_data_manager().template GetIterator<Input>(pid);

        std::vector<Input> elements;
        auto save_fn = [&elements](Input input) {
                           elements.push_back(input);
                       };
        auto lop_chain = lop_stack_.push(save_fn).emit();

        // loop over input
        while (it.HasNext()) {
            lop_chain(it.Next());
        }

        // Emit new elements
        data::BlockEmitter<Input> emit = (this->context_).get_data_manager().template GetLocalEmitter<Input>(DIABase::data_id_);
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

} // namespace c7a

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
