/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/
#pragma once

#include <vector>
#include <string>
#include "../data/data_manager.hpp"

namespace c7a {

class DIABase {
public:

    typedef std::vector<DIABase*> DIABaseVector;

    DIABase(data::DataManager &data_manager, const DIABaseVector& parents)
        : data_manager_(data_manager), parents_(parents)
    {}

    virtual ~DIABase() {}

    virtual void execute() {}

    virtual std::string ToString() { return "DIABase"; }

    const DIABaseVector& get_childs() {
        return childs_;
    }

    const DIABaseVector & get_parents() {
        return parents_;
    }

    data::DataManager & get_data_manager() {
        return data_manager_;
    }

    void add_child(DIABase* child) {
        childs_.push_back(child);
    }

protected:
    data::DataManager &data_manager_;
    DIABaseVector childs_, parents_;
};

} // namespace c7a

/******************************************************************************/
