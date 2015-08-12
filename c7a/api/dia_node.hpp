/*******************************************************************************
 * c7a/api/dia_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DIA_NODE_HEADER
#define C7A_API_DIA_NODE_HEADER

#include <c7a/api/dia_base.hpp>
#include <c7a/common/stats.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType>
struct CallbackPair {
    void operator () (const ValueType& elem) {
        cb_(elem);
    }

    std::function<void(const ValueType&)> cb_;
    NodeType                              type_;
};

/*!
 * A DIANode is a typed node representing and operation in c7a. It is the super
 * class for all operation nodes and stores the state of the operation. The type
 * of a DIANode is the type, in which the DIA is after the last global barrier
 * in the operation (between MainOp and PostOp).
 *
 * \tparam ValueType Type of the DIA between MainOp and PostOp
 */
template <typename ValueType>
class DIANode : public DIABase
{
public:
    /*!
     * Default constructor for a DIANode.
     */
    DIANode() { }

    /*!
     * Constructor for a DIANode, which sets references to the
     * parent nodes. Calls the constructor of DIABase with the same parameters.
     *
     * \param ctx Reference to Context, which holds references to data and
     * network.
     *
     * \param parents Reference to parents of this node, which have to be
     * computed previously
     */
    DIANode(Context& ctx,
            const std::vector<std::shared_ptr<DIABase> >& parents,
            const std::string stats_tag,
            StatsNode* stats_node)
        : DIABase(ctx, parents, stats_tag, stats_node)
    { }

    //! Virtual destructor for a DIANode.
    virtual ~DIANode() { }

    /*!
     * Enables children to push their "folded" function chains to their parent.
     * This way the parent can push all its result elements to each of the
     * children.  This procedure enables the minimization of IO-accesses.
     *
     * \param callback Callback function from the child including all
     * locally processable operations between the parent and child.
     */
    void RegisterChild(const CallbackPair<ValueType>& callback) {
        this->callbacks_.push_back(callback);
    }

    void UnregisterChilds() final {
        callbacks_.erase(
            std::remove_if(
                callbacks_.begin(), callbacks_.end(),
                [](const auto& cb) { return cb.type_ != NodeType::COLLAPSE; }),
            callbacks_.end());
    }

    std::vector<CallbackPair<ValueType> > & callbacks() {
        return callbacks_;
    }

    void callback_functions(std::vector<std::function<void(const ValueType&)> >& cbs) {
        for (auto& cb_pair : callbacks_) cbs.push_back(cb_pair.cb_);
    }

    void PushElement(const ValueType& elem) {
        for (auto& callback : this->callbacks_) {
            callback(elem);
        }
    }

protected:
    //! Callback functions from the child nodes.
    std::vector<CallbackPair<ValueType> > callbacks_;
};

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_DIA_NODE_HEADER

/******************************************************************************/
