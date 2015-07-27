/*******************************************************************************
 * c7a/data/serializer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SERIALIZER_HEADER
#define C7A_DATA_SERIALIZER_HEADER

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
struct Serializer
{ };

//! Serialize the type to std::string
template <typename Archive, typename T>
inline void Serialize(const T& x, Archive& ar) {
    Serializer<Archive, T>::Serialize(x, ar);
}

//! Deserialize the std::string to the given type
template <typename Archive, typename T>
inline T Deserialize(Archive& ar) {
    return Serializer<Archive, T>::Deserialize(ar);
}

/******************* Serialization of plain old data types ********************/
template <typename Archive, typename T>
struct Serializer<Archive, T,
                  typename std::enable_if<std::is_pod<T>::value>::type>
{
    static void Serialize(const T& x, Archive& ar) {
        ar.template Put<T>(x);
    }
    static T Deserialize(Archive& ar) {
        return ar.template Get<T>();
    }
    static const bool fixed_size = true;
};

/********************** Serialization of strings ******************************/
template <typename Archive>
struct Serializer<Archive, std::string>
{
    static void Serialize(const std::string& x, Archive& ar) {
        ar.PutString(x);
    }
    static std::string Deserialize(Archive& ar) {
        return ar.GetString();
    }
    static const bool fixed_size = false;
};

/*********************** Serialization of pairs *******************************/
template <typename Archive, typename U, typename V>
struct Serializer<Archive, std::pair<U, V> >
{
    static void Serialize(const std::pair<U, V>& x, Archive& ar) {
        Serializer<Archive, U>::Serialize(x.first, ar);
        Serializer<Archive, V>::Serialize(x.second, ar);
    }
    static std::pair<U, V> Deserialize(Archive& ar) {
        U u = Serializer<Archive, U>::Deserialize(ar);
        V v = Serializer<Archive, V>::Deserialize(ar);
        return std::pair<U, V>(std::move(u), std::move(v));
    }
    static const bool fixed_size = (Serializer<Archive, U>::fixed_size &&
                                    Serializer<Archive, V>::fixed_size);
};

/*********************** Serialization of tuples ******************************/

namespace detail {

//-------------------------- tuple serializer --------------------------------//
// serialize the (|tuple| - RevIndex)-th element in the tuple
// and call recursively to serialize the next element:
// (|tuple| - (RevIndex - 1))
// for simplicity we talk about the k-th element
template <typename Archive, size_t RevIndex, typename ... Args>
struct TupleSerializer {

    static const size_t Index = sizeof ... (Args) - RevIndex;
    // type of k-th element
    using ThisElemType =
              typename std::tuple_element<Index, std::tuple<Args ...> >::type;

    static void         Serialize(const std::tuple<Args ...>& x, Archive& ar) {
        // serialize k-th element
        Serializer<Archive, ThisElemType>::Serialize(std::get<Index>(x), ar);
        // recursively serialize (k+1)-th element
        TupleSerializer<Archive, RevIndex - 1, Args ...>::Serialize(x, ar);
    }

    static const bool   fixed_size
        = Serializer<Archive, ThisElemType>::fixed_size
          && TupleSerializer<Archive, RevIndex - 1, Args ...>::fixed_size;
};

// Base case when RevIndex == 0
template <typename Archive, typename ... Args>
struct TupleSerializer<Archive, 0, Args ...>{
    static void Serialize(const std::tuple<Args ...>&, Archive&) {
        // Doesn't do anything
    }
    static const bool fixed_size = true;
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
        auto head = std::make_tuple(Serializer<Archive, T>::Deserialize(ar));
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

//--------------------- tuple de-/serializer interface -----------------------//
template <typename Archive, typename ... Args>
struct Serializer<Archive, std::tuple<Args ...> >
{
    static void Serialize(const std::tuple<Args ...>& x, Archive& ar) {
        detail::TupleSerializer<
            Archive, sizeof ... (Args), Args ...>::Serialize(x, ar);
    }
    static std::tuple<Args ...> Deserialize(Archive& ar) {
        return detail::TupleDeserializer<
            Archive, sizeof ... (Args), std::tuple<Args ...> >::Deserialize(ar);
    }

    static const bool fixed_size = detail::TupleSerializer<
        Archive, sizeof ... (Args), Args ...>::fixed_size;
};

/*********************** Serialization of vector ******************************/

template <typename Archive, typename T>
struct Serializer<Archive, std::vector<T> >
{
    static void Serialize(const std::vector<T>& x, Archive& ar) {
        ar.PutVarint(x.size());
        for (typename std::vector<T>::const_iterator it = x.begin();
             it != x.end(); ++it)
            Serializer<Archive, T>::Serialize(*it, ar);
    }
    static std::vector<T> Deserialize(Archive& ar) {
        size_t size = ar.GetVarint();
        std::vector<T> out;
        for (size_t i = 0; i != size; ++i)
            out.emplace_back(Serializer<Archive, T>::Deserialize(ar));
        return out;
    }
    static const bool fixed_size = false;
};

/*********************** Serialization of array *******************************/

template <typename Archive, typename T, size_t N>
struct Serializer<Archive, std::array<T, N> >
{
    static void Serialize(const std::array<T, N>& x, Archive& ar) {
        for (typename std::array<T, N>::const_iterator it = x.begin();
             it != x.end(); ++it)
            Serializer<Archive, T>::Serialize(*it, ar);
    }
    static std::array<T, N> Deserialize(Archive& ar) {
        std::array<T, N> out;
        for (size_t i = 0; i != N; ++i)
            out[i] = std::move(Serializer<Archive, T>::Deserialize(ar));
        return out;
    }
    static const bool fixed_size = Serializer<Archive, T>::fixed_size;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SERIALIZER_HEADER

/******************************************************************************/
