/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#include<vector>


class DIABase {
public: 

    DIABase() {
        std::vector<DIABase> vec;
        parents_ = vec;
    }

    DIABase(std::vector<DIABase> parents) : parents_(parents) {}

    virtual ~DIABase() {}

    void execute() {}

    std::vector<DIABase> & get_childs() {
        return childs_; 
    }

    std::vector<DIABase> & get_parents() {
        return parents_; 
    }

    void add_child(DIABase child) {
        childs_.push_back(child);
    }

protected: 
    std::vector<DIABase> childs_;
    std::vector<DIABase> parents_;
};


/******************************************************************************/
