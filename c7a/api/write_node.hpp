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

template <typename Input, typename Output, typename WriteFunction>
class WriteNode : public ActionNode<Output>
{
public:
    WriteNode(data::DataManager& data_manager,
              DIANode<Input>* parent,
              WriteFunction write_function)
        : ActionNode<Output>(data_manager, { parent }), 
          write_function_(write_function) 
    { }

    virtual ~WriteNode() { }

    void execute() { }

    std::string toString() override
    {
        std::string str
            = std::string("[WriteNode] Id: ") + std::to_string(DIABase::data_id_);
        return str;
    }

private:
    WriteFunction write_function_;
};

} // namespace c7a

#endif // !C7A_API_WRITE_NODE_HEADER

/******************************************************************************/
