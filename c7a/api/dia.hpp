#include <functional>
#include <vector>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unordered_map>
#include <string>

#include "function_traits.hpp"

// Distributed Immutable Array
template <typename T>
class DIA {
public:
    DIA() : data_() {}

    DIA(const DIA& other) : data_(other.data_) {}

    explicit DIA(const std::vector<T>& init_data) : data_(init_data) {}

    explicit DIA(DIA&& other) : DIA() {
        swap(*this, other);
    }

    virtual ~DIA() {};

    struct Identity {
        template<typename U>
        constexpr auto operator()(U&& v) const noexcept 
            -> decltype(std::forward<U>(v)) {
            return std::forward<U>(v);
        }
    };

    friend void swap(DIA& first, DIA& second) {
        using std::swap;
        swap(first.data_, second.data_);
    }

    DIA& operator=(DIA rhs) {
        swap(*this, rhs);
        return *this;
    }

    //! Get the element at a certain index.
    //
    //! \return Element at index \a index.
    T At(size_t index) {
        return data_.at(index);
    }

    //! Print each element in the current DIA.
    //
    //! \param print_fn Converts each element to printable output.
    template <typename print_fn_t = Identity>
    void Print(print_fn_t print_fn = print_fn_t()) {
        for (const T& element : data_) {
            std::cout << print_fn(element) << std::endl;
        }
    }

    //! Map each element of the current DIA to a new element.
    //
    //! \param map_fn Maps an element of type T to an element of type U.
    //
    //! \return Resulting DIA containing elements of type U.
    template <typename map_fn_t>
    auto Map(const map_fn_t& map_fn) {
        static_assert(FunctionTraits<map_fn_t>::arity == 1, "error");
        using map_result_t = typename FunctionTraits<map_fn_t>::result_type;
        std::vector<map_result_t> output;
        for (auto element : data_) {
            output.push_back(map_fn(element));
        }

        return DIA<map_result_t>(output);
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
        static_assert(FunctionTraits<reduce_fn_t>::arity == 1, "error");
        using reduce_result_t 
            = typename FunctionTraits<reduce_fn_t>::result_type;
        using key_t = typename FunctionTraits<key_extr_fn_t>::result_type;

        std::vector<reduce_result_t> output;
        std::vector<key_t> keys;
        std::unordered_multimap<key_t, T> buckets;

        for (auto element : data_) {
            key_t key = key_extr(element);
            if (!buckets.count(key)) {
                keys.push_back(key);
            }
            buckets.insert(std::pair<key_t, T>(key, element));
        }

        for (auto key : keys) {
            std::vector<T> bucket;
            auto range = buckets.equal_range(key);
            for_each (
                range.first,
                range.second,
                [&bucket](std::pair<key_t, T> element) {
                        bucket.push_back(element.second);
                    }
            );
            output.push_back(reduce_fn(bucket));
        }

        return DIA<reduce_result_t>(output);
    }

    //! Map each element of the current DIA to a list of new elements.
    //! The list for an element can be empty.
    //
    //! \param flatmap_fn Maps a single element of type T to a list of elements of type U.
    //
    //! \return Resulting DIA containing lists of elements of type U.
    template <typename flatmap_fn_t>
    auto FlatMap(const flatmap_fn_t& flatmap_fn) {
        static_assert(FunctionTraits<flatmap_fn_t>::arity == 2, "error");
        using flatmap_arg_t 
            = typename FunctionTraits<flatmap_fn_t>::template arg<0>;
        using emit_fn_t 
            = typename FunctionTraits<flatmap_fn_t>::template arg<1>;
        using emit_arg_t 
            = typename FunctionTraits<emit_fn_t>::template arg<0>;
        std::vector<emit_arg_t> output;

        std::function<void(emit_arg_t)> emit_fn = 
            [&output](emit_arg_t new_element) {
                output.push_back(new_element);
            };

        for (auto element : data_) {
            flatmap_fn(element, emit_fn);
        }

        return DIA<emit_arg_t>(output);
    }

    //! Zip the current DIA of type T with another DIA of type U.
    //
    //! \param second DIA to zip with.
    //! \param zip_fn Maps an element of type T and an element of type U 
    //!               to an element of type V.
    //
    //! \return Resulting DIA containing elements of type V.
    template<typename zip_fn_t>
    auto Zip(DIA<typename FunctionTraits<zip_fn_t>::template arg<1> > second,
             const zip_fn_t& zip_fn) {
        using zip_result_t = typename FunctionTraits<zip_fn_t>::result_type;
        std::vector<zip_result_t> output;

        std::size_t index = 0;
        for(auto element : data_) {
            output.push_back(zip_fn(At(index), second.At(index)));
            index++;
        }

        return DIA<zip_result_t>(output);
    }

    //! Allow direct data access. This is EVIL!
    //
    //! \return Interal data.
    const std::vector<T>& evil_get_data() const {
        return data_;
    }

private:
    std::vector<T> data_;
};

