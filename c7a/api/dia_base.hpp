/*******************************************************************************
 * c7a/api/dia_base.hpp
 *
 * Untyped super class of DIANode. Used to build the execution graph.
 ******************************************************************************/
#pragma once

#include <vector>
#include <string>
#include "../data/data_manager.hpp"


namespace c7a {

/*!
 * The DIABase is the untyped super class of DIANode. DIABases are used to build
 * the execution graph, which is used to execute the computation.

 * Each DIABase knows it's parents and children. Parents are node which have to
 * computed previously, children are nodes which have this node as a parent.

 * Additionally, a DIABase has a reference to the DataManager, which can give
 * iterators to actual data.
 */
class DIABase {
public:

    typedef std::vector<DIABase*> DIABaseVector;

    /*!
     * The constructor for a DIABase. Sets the @param data_manager and the
     * associated DIAId. 
     *
     * Sets the parents for this node and adds this node as a child for 
     * each parent.
     *
     * \param data_manager DataManager, which gives iterators to data
     *
     * \param parents Parents of this node, which have to be computed previously
     */
    DIABase(data::DataManager &data_manager, const DIABaseVector& parents)
        : data_manager_(data_manager), parents_(parents) {
        for (auto parent : parents_) {
            parent->add_child(this);
        }
        data_id_ = data_manager_.AllocateDIA();
    }

    //!Virtual destructor for a DIABase.
    virtual ~DIABase() {}

    //!Virtual execution method. Triggers actual computation in sub-classes.
    virtual void execute() = 0;

    //!Virtual ToString method. Returns the type of node in sub-classes.
    virtual std::string ToString() = 0;

    //! Returns the childs of this DIABase.
    //! \return A vector of all childs
    const DIABaseVector& get_childs() {
        return childs_;
    }

    //! Returns the parents of this DIABase.
    //! \return A vector of all parents
    const DIABaseVector & get_parents() {
        return parents_;
    }

    //! Returns the DataManager of this DIABase.
    //! \return The DataManager of this DIABase.
    data::DataManager & get_data_manager() {
        return data_manager_;
    }

    //! Adds a child to the vector of childs. This method is called in the constructor.
    //! \param child The child to add.
    void add_child(DIABase* child) {
        childs_.push_back(child);
    }

    data::DIAId get_data_id() {
        return data_id_;
    }

protected:
    //! DataManager, which can give iterators to data.
    data::DataManager & data_manager_;
    //! Unique ID of this DIABase.
    data::DIAId data_id_;
    //! Childs and parents of this DIABase.
    DIABaseVector childs_, parents_;
};

} // namespace c7a

/******************************************************************************/
