#include "gtest/gtest.h"
#include "c7a/api/dia_base.hpp"
#include "c7a/engine/stage_builder.hpp"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

using namespace c7a::data;


TEST(DIAAllocate, ForReal) {
    class TNode {
    public:
        TNode(DataManager & dm) : dm_(dm) {
            dm_.AllocateDIA();
        };
        TNode CreateChildNode() {
            return TNode(dm_);
        } 
    protected:
        DataManager & dm_;
    };

    class TContext {
    public:
        TContext() : dm_() {};
        TNode CreateNode() {
            return TNode(dm_);
        }
    protected:
        DataManager dm_;
    };

    

    TContext a;
    TNode b = a.CreateNode();
    TNode c = b.CreateChildNode();
    TNode d = c.CreateChildNode();
    TNode e = c.CreateChildNode();
    TNode f = e.CreateChildNode();
    TNode g = f.CreateChildNode();

}
