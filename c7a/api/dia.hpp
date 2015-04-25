/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_DIA_HEADER
#define C7A_API_DIA_HEADER

#include <functional>
#include <vector>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unordered_map>
#include <string>

#include "function_traits.hpp"
#include "lop_node.hpp"
#include "reduce_node.hpp"

// Distributed immutable array
template <typename T>
class DIA {
public:
    DIA() : data_() { }

    DIA(DIANode<T> node) : data_(), my_node_(node) { }

    DIA(const std::vector<T>& init_data, DIANode<T> node) : data_(init_data), my_node_(node) { }

    DIA(const DIA& other) : data_(other.data_) { }

    explicit DIA(const std::vector<T>& init_data) : data_(init_data) { }

    explicit DIA(DIA&& other) : DIA()
    {
        swap(*this, other);
    }

    virtual ~DIA() { }

    struct Identity {
        template <typename U>
        constexpr auto operator () (U&& v) const noexcept
        ->decltype(std::forward<U>(v))
        {
            return std::forward<U>(v);
        }
    };

    friend void swap(DIA& first, DIA& second)
    {
        using std::swap;
        swap(first.data_, second.data_);
    }

    DIA& operator = (DIA rhs)
    {
        swap(*this, rhs);
        return *this;
    }

    //! Map each element of the current DIA to a list of new elements.
    //! The list for an element can be empty.
    //
    //! \param flatmap_fn Maps a single element of type T to a list of elements of type U.
    //
    //! \return Resulting DIA containing lists of elements of type U.
    template <typename flatmap_fn_t>
    auto FlatMap(const flatmap_fn_t &flatmap_fn) {
        // Create new LOpNode with flatmap_fn
        // Compose functions
        // Link Parent(s)
        // Link Child(s)
        // Return new DIARef
        static_assert(FunctionTraits<flatmap_fn_t>::arity == 2, "error");
        //using flatmap_arg_t
        //          = typename FunctionTraits<flatmap_fn_t>::template arg<0>;
        using emit_fn_t
                  = typename FunctionTraits<flatmap_fn_t>::template arg<1>;
        using emit_arg_t
                  = typename FunctionTraits<emit_fn_t>::template arg<0>;

        std::vector<DIABase> parents{my_node_};
        LOpNode<emit_arg_t,flatmap_fn_t> l_node(parents, FLATMAP, flatmap_fn);
      
        return DIA(l_node);
    }

//! Hash elements of the current DIA onto buckets and reduce each bucket to a single value.
    //
    //! \param key_extr Hash function to get a key for each element.
    //! \param reduce_fn Reduces the elements (of type T) of each bucket to a single value of type U.
    //
    //! \return Resulting DIA containing elements of type U.
    template<typename key_extr_fn_t, typename reduce_fn_t>
    auto Reduce(const key_extr_fn_t& key_extr, const reduce_fn_t& reduce_fn) {
        static_assert(FunctionTraits<key_extr_fn_t>::arity == 1, "error");
        static_assert(FunctionTraits<reduce_fn_t>::arity == 2, "error");

        //using key_t = typename FunctionTraits<key_extr_fn_t>::result_type;

        std::vector<DIABase> parents{my_node_};
        ReduceNode<T,key_extr_fn_t,reduce_fn_t> rd_node(parents, key_extr, reduce_fn);

        return DIA(rd_node);
    }

    size_t Size() {
        return data_.size();
    }

    //! Allow direct data access. This is EVIL!
    //
    //! \return Interal data.
    const std::vector<T> & evil_get_data() const
    {
        return data_;
    }

    std::string NodeString() {
        return my_node_.ToString();
    }

private:
    std::vector<T> data_;
    DIANode<T> my_node_;
};

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
