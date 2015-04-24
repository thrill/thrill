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

// Distributed immutable array
template <typename T>
class DIA {
public:
    DIA() : data_() { }

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
        using flatmap_arg_t
                  = typename FunctionTraits<flatmap_fn_t>::template arg<0>;
        using emit_fn_t
                  = typename FunctionTraits<flatmap_fn_t>::template arg<1>;
        using emit_arg_t
                  = typename FunctionTraits<emit_fn_t>::template arg<0>;
        std::vector<emit_arg_t> output;

        std::vector<DIABase*> parents{my_node_};
        LOpNode<flatmap_fn_t> l_node(parents, FLATMAP, flatmap_fn);

        // std::function<void(emit_arg_t)> emit_fn =
        //     [&output](emit_arg_t new_element) {
        //         output.push_back(new_element);
        //     };

        // for (auto element : data_) {
        //     flatmap_fn(element, emit_fn);
        // }

        // return DIA<emit_arg_t>(output);
    }

    //! Allow direct data access. This is EVIL!
    //
    //! \return Interal data.
    const std::vector<T> & evil_get_data() const
    {
        return data_;
    }

private:
    std::vector<T> data_;
    DIANode<T>* my_node_;
};

#endif // !C7A_API_DIA_HEADER

/******************************************************************************/
