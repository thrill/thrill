/*******************************************************************************
 * thrill/data/serialization.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_SERIALIZATION_HEADER
#define THRILL_DATA_SERIALIZATION_HEADER

#include <array>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*************** Base Template and Callable Serialize/Deserialize *************/

template <typename Archive, typename T, class Enable = void>
struct Serialization;

/******************* Serialization of plain old data types ********************/

template <typename Archive, typename T>
struct Serialization<Archive, T,
                     typename std::enable_if<
                         // a POD, but not a pointer
                         std::is_pod<T>::value&& !std::is_pointer<T>::value
                         >::type>
{
    static void Serialize(const T& x, Archive& ar) {
        ar.template Put<T>(x);
    }
    static T Deserialize(Archive& ar) {
        return ar.template Get<T>();
    }
    static const bool   is_fixed_size = true;
    static const size_t fixed_size = sizeof(T);
};

/********************** Serialization of strings ******************************/

template <typename Archive>
struct Serialization<Archive, std::string>
{
    static void Serialize(const std::string& x, Archive& ar) {
        ar.PutString(x);
    }
    static std::string Deserialize(Archive& ar) {
        return ar.GetString();
    }
    static const bool   is_fixed_size = false;
    static const size_t fixed_size = 0;
};

/*********************** Serialization of pairs *******************************/

template <typename Archive, typename U, typename V>
struct Serialization<Archive, std::pair<U, V> >
{
    static void Serialize(const std::pair<U, V>& x, Archive& ar) {
        Serialization<Archive, U>::Serialize(x.first, ar);
        Serialization<Archive, V>::Serialize(x.second, ar);
    }
    static std::pair<U, V> Deserialize(Archive& ar) {
        U u = Serialization<Archive, U>::Deserialize(ar);
        V v = Serialization<Archive, V>::Deserialize(ar);
        return std::pair<U, V>(std::move(u), std::move(v));
    }
    static const bool   is_fixed_size =
        (Serialization<Archive, U>::is_fixed_size &&
         Serialization<Archive, V>::is_fixed_size);
    static const size_t fixed_size =
        (Serialization<Archive, U>::fixed_size +
         Serialization<Archive, V>::fixed_size);
};

//! \addtogroup data_internal Data Internals
//! \{

/*********************** Serialization of tuples ******************************/

namespace detail {

//-------------------------- tuple serializer --------------------------------//
// serialize the (|tuple| - RevIndex)-th element in the tuple
// and call recursively to serialize the next element:
// (|tuple| - (RevIndex - 1))
// for simplicity we talk about the k-th element
template <typename Archive, size_t RevIndex, typename ... Args>
struct TupleSerialization {

    static const size_t Index = sizeof ... (Args) - RevIndex;
    // type of k-th element
    using ThisElemType =
              typename std::tuple_element<Index, std::tuple<Args ...> >::type;

    static void         Serialize(const std::tuple<Args ...>& x, Archive& ar) {
        // serialize k-th element
        Serialization<Archive, ThisElemType>::Serialize(std::get<Index>(x), ar);
        // recursively serialize (k+1)-th element
        TupleSerialization<Archive, RevIndex - 1, Args ...>::Serialize(x, ar);
    }

    static const bool   is_fixed_size
        = Serialization<Archive, ThisElemType>::is_fixed_size
          && TupleSerialization<Archive, RevIndex - 1, Args ...>::is_fixed_size;

    static const size_t fixed_size =
        Serialization<Archive, ThisElemType>::fixed_size
        + TupleSerialization<Archive, RevIndex - 1, Args ...>::fixed_size;
};

// Base case when RevIndex == 0
template <typename Archive, typename ... Args>
struct TupleSerialization<Archive, 0, Args ...>{
    static void Serialize(const std::tuple<Args ...>&, Archive&) {
        // Doesn't do anything
    }
    static const bool   is_fixed_size = true;
    static const size_t fixed_size = 0;
};

//-------------------------- tuple deserializer ------------------------------//
template <typename Archive, int RevIndex, typename T, typename ... Args>
struct TupleDeserializer { };

// deserialize the (|tuple| - RevIndex)-th element in the tuple and call
// recursively to serialize the next element: (|tuple| - (RevIndex - 1)) for
// simplicity we talk about the k-th element
template <typename Archive, int RevIndex, typename T, typename ... Args>
struct TupleDeserializer<Archive, RevIndex, std::tuple<T, Args ...> >{
    static std::tuple<T, Args ...> Deserialize(Archive& ar) {
        // deserialize the k-th element and put it in a tuple
        auto head = std::make_tuple(Serialization<Archive, T>::Deserialize(ar));
        // deserialize all elements i, i > k and concat the tuples
        return std::tuple_cat(
            head, TupleDeserializer<
                Archive, RevIndex - 1, std::tuple<Args ...> >::Deserialize(ar));
    }
};

// Base Case when RevIndex == 0
template <typename Archive>
struct TupleDeserializer<Archive, 0, std::tuple<> >{
    static std::tuple<> Deserialize(Archive&) {
        return std::make_tuple();
    }
};

} // namespace detail

//! \}

//--------------------- tuple de-/serializer interface -----------------------//
template <typename Archive, typename ... Args>
struct Serialization<Archive, std::tuple<Args ...> >
{
    static void Serialize(const std::tuple<Args ...>& x, Archive& ar) {
        detail::TupleSerialization<
            Archive, sizeof ... (Args), Args ...>::Serialize(x, ar);
    }
    static std::tuple<Args ...> Deserialize(Archive& ar) {
        return detail::TupleDeserializer<
            Archive, sizeof ... (Args), std::tuple<Args ...> >::Deserialize(ar);
    }

    static const bool   is_fixed_size = detail::TupleSerialization<
        Archive, sizeof ... (Args), Args ...>::is_fixed_size;
    static const size_t fixed_size = detail::TupleSerialization<
        Archive, sizeof ... (Args), Args ...>::fixed_size;
};

/*********************** Serialization of vector ******************************/

template <typename Archive, typename T>
struct Serialization<Archive, std::vector<T> >
{
    static void Serialize(const std::vector<T>& x, Archive& ar) {
        ar.PutVarint(x.size());
        for (typename std::vector<T>::const_iterator it = x.begin();
             it != x.end(); ++it)
            Serialization<Archive, T>::Serialize(*it, ar);
    }
    static std::vector<T> Deserialize(Archive& ar) {
        size_t size = ar.GetVarint();
        std::vector<T> out;
        for (size_t i = 0; i != size; ++i)
            out.emplace_back(Serialization<Archive, T>::Deserialize(ar));
        return out;
    }
    static const bool   is_fixed_size = false;
    static const size_t fixed_size = 0;
};

/*********************** Serialization of array *******************************/

template <typename Archive, typename T, size_t N>
struct Serialization<Archive, std::array<T, N> >
{
    static void Serialize(const std::array<T, N>& x, Archive& ar) {
        for (typename std::array<T, N>::const_iterator it = x.begin();
             it != x.end(); ++it)
            Serialization<Archive, T>::Serialize(*it, ar);
    }
    static std::array<T, N> Deserialize(Archive& ar) {
        std::array<T, N> out;
        for (size_t i = 0; i != N; ++i)
            out[i] = std::move(Serialization<Archive, T>::Deserialize(ar));
        return out;
    }
    static const bool   is_fixed_size = Serialization<Archive, T>::is_fixed_size;
    static const size_t fixed_size = N * Serialization<Archive, T>::fixed_size;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !THRILL_DATA_SERIALIZATION_HEADER

/******************************************************************************/
