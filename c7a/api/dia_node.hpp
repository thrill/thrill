/*******************************************************************************
 * c7a/api/dia_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/
#ifndef C7A_API_DIA_NODE_HEADER
#define C7A_API_DIA_NODE_HEADER

#include <string>
#include <vector>

#include "dia_base.hpp"
#include "context.hpp"
#include "../data/data_manager.hpp"

namespace c7a {
//! \addtogroup api Interface
//! \{

/*!
 * Possible states a DIANode can be in.
 */
enum kState {
    NEW,        /*!< The DIANode has not been computed yet. */
    CALCULATED, /*!< The DIANode has been calculated but not explicitly cached.
                  Data might be available or has to be recalculated when needed */
    CACHED,     /*!< The DIANode is cached and it's data can be accessed */
    DISPOSED    /*!< The DIANode is disposed by the user, needs to be recomputed when accessed */
};

/*!
 * A DIANode is a typed node representing and operation in c7a. It is the super class for
 * all operation nodes and stores the state of the operation. The type of a DIANode is the type,
 * in which the DIA is after the last global barrier in the operation (between MainOp and PostOp).
 *
 * \tparam T Type of the DIA between MainOp and PostOp
 */
template <typename T>
class DIANode : public DIABase
{
public:
    /*!
     * Default constructor for a DIANode.
     */
    DIANode() { }

    /*!
     * Constructor for a DIANode, which sets references to the DataManager and parent nodes. Calls the constructor of DIABase
     * with the same parameters.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     *
     * \param parents Reference to parents of this node, which have to be computed previously
     */
    DIANode(Context& ctx, const DIABaseVector& parents)
        : DIABase(ctx, parents)
    { }

    //! Virtual destructor for a DIANode.
    virtual ~DIANode() { }

    //! ToString-method. Returns DIANode and it's state as a string.
    std::string ToString() override
    {
        std::string str;
        str = std::string("[DIANode/State:") + state_string_() + "]";
        return str;
    }

protected:
    //! State of the DIANode. State is NEW on creation.
    kState state_ = NEW;

    //!Returns the state of this DIANode as a string. Used by ToString.
    std::string state_string_()
    {
        switch (state_) {
        case NEW:
            return "NEW";
        case CALCULATED:
            return "CALCULATED";
        case CACHED:
            return "CACHED";
        case DISPOSED:
            return "DISPOSED";
        default:
            return "UNDEFINED";
        }
    }
};
} // namespace c7a

//! \}

#endif // !C7A_API_DIA_NODE_HEADER

/******************************************************************************/
