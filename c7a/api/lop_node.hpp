/*******************************************************************************
 * c7a/api/lop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_LOP_NODE_HEADER
#define C7A_API_LOP_NODE_HEADER

#include "dia_node.hpp"

namespace c7a {

template <typename T, typename LOpStack>
class LOpNode : public DIANode<T> {
public:
    LOpNode(Context& ctx,
            const DIABaseVector& parents, 
            LOpStack& lop_stack) 
        : DIANode<T>(ctx, parents), 
        lop_stack_(lop_stack) {
        };

    virtual ~LOpNode() {}

    void execute() override {
        // Execute LOpChain
        data::DIAId pid = this->get_parents()[0]->get_data_id();
        // //get data from data manager
        data::BlockIterator<T> it = (this->context_).get_data_manager().template GetLocalBlocks<T>(pid);

        std::vector<T> elements;
        auto save_fn = [&elements](T input) {
                elements.push_back(input);
            };
        auto lop_chain = lop_stack_.push(save_fn).emit();

        // loop over input
        while (it.HasNext()) {
            lop_chain(it.Next());
        }

        // Emit new elements
        data::BlockEmitter<T> emit = (this->context_).get_data_manager().template GetLocalEmitter<T>(DIABase::data_id_);
        for (auto elem : elements) {
            emit(elem);
        }
    };

    std::string ToString() override {
        // Create string
        std::string str 
            = std::string("[LOpNode] Id: ") + std::to_string(DIABase::data_id_);
        return str;
    }

private:
    LOpStack lop_stack_;
};

} // namespace c7a

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
