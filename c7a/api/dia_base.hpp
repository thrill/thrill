/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#include<vector>


class DIABase {
public:

    typedef std::vector<DIABase*> DIABaseVector;

    DIABase(const DIABaseVector& parents)
        : parents_(parents)
    {}

    virtual ~DIABase() {}

    void execute() {}

    const DIABaseVector& get_childs() {
        return childs_;
    }

    const DIABaseVector & get_parents() {
        return parents_;
    }

    void add_child(DIABase* child) {
        childs_.push_back(child);
    }

protected:
    DIABaseVector childs_, parents_;
};


/******************************************************************************/
