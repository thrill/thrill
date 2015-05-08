/*******************************************************************************
 * c7a/api/lop_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_API_LOP_NODE_HEADER
#define C7A_API_LOP_NODE_HEADER

#include "dia_node.hpp"

namespace c7a {

template <typename T, typename LOpFunction>
class LOpNode : public DIANode<T>
{
public:
    LOpNode(data::DataManager& data_manager,
            const std::vector<DIABase*>& parents,
            LOpFunction lop_function)
        : DIANode<T>(data_manager, parents),
          lop_function_(lop_function) { }
    virtual ~LOpNode() { }

    void execute() { }

    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[LOpNode]");
        return str;
    }

private:
    LOpFunction lop_function_;
};

} // namespace c7a

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
