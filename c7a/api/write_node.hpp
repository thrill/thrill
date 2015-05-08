/*******************************************************************************
 * c7a/api/write_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_NODE_HEADER
#define C7A_API_WRITE_NODE_HEADER

namespace c7a {

template <typename T, typename WriteFunction>
class WriteNode : public ActionNode<T>
{
public:
    WriteNode(data::DataManager& data_manager,
              const std::vector<DIABase*>& parents,
              WriteFunction write_function)
        : ActionNode<T>(data_manager, parents), write_function_(write_function)
    { }

    virtual ~WriteNode() { }

    void execute() { }

    std::string toString() override
    {
        std::string str
            = std::string("[WriteNode]");
        return str;
    }

private:
    WriteFunction write_function_;
};

} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
