/*******************************************************************************
 * c7a/data/serializer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SERIALIZER_HEADER
#define C7A_DATA_SERIALIZER_HEADER

#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

template <typename Archive, typename T, class Enable = void>
struct Serializer
{ };

/******************* Serialization of plain old data types ********************/
template <typename Archive, typename T>
struct Serializer<Archive, T,
                  typename std::enable_if<std::is_pod<T>::value>::type>
{
    static void Serialize(const T& x, Archive& a) {
        a.template Put<T>(x);
    }
    static T Deserialize(Archive& a) {
        return a.template Get<T>();
    }
    static const bool fixed_size = true;
};

/********************** Serialization of strings ******************************/
template <typename Archive>
struct Serializer<Archive, std::string>
{
    static void Serialize(const std::string& x, Archive& a) {
        a.PutString(x);
    }
    static std::string Deserialize(Archive& a) {
        return a.GetString();
    }
    static const bool fixed_size = false;
};

/*********************** Serialization of pairs *******************************/
template <typename Archive, typename U, typename V>
struct Serializer<Archive, std::pair<U, V> >
{
    static void Serialize(const std::pair<U, V>& x, Archive& a) {
        Serializer<Archive, U>::Serialize(x.first, a);
        Serializer<Archive, V>::Serialize(x.second, a);
    }
    static std::pair<U, V> Deserialize(Archive& a) {
        U u = Serializer<Archive, U>::Deserialize(a);
        V v = Serializer<Archive, V>::Deserialize(a);
        return std::pair<U, V>(std::move(u), std::move(v));
    }
    static const bool fixed_size = (Serializer<Archive, U>::fixed_size &&
                                    Serializer<Archive, V>::fixed_size);
};

/*********************** Serialization of tuples ******************************/

namespace serializer {

//-------------------------- tuple serializer --------------------------------//
// serialize the (|tuple| - RevIndex)-th element in the tuple
// and call recursively to serialize the next element:
// (|tuple| - (RevIndex - 1))
// for simplicity we talk about the k-th element
template <typename Archive, size_t RevIndex, typename ... Args>
struct TupleSerializer {

    static const size_t Index = sizeof ... (Args) - RevIndex;
    // type of k-th element
    using ThisElemType = typename std::tuple_element<Index, std::tuple<Args ...> >::type;

    static void         Serialize(const std::tuple<Args ...>& x, Archive& a) {
        // serialize k-th element
        Serializer<Archive, ThisElemType>::Serialize(std::get<Index>(x), a);
        // recursively serialize (k+1)-th element
        TupleSerializer<Archive, RevIndex - 1, Args ...>::Serialize(x, a);
    }

    static const bool   fixed_size = Serializer<Archive, ThisElemType>::fixed_size
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

// deserialize the (|tuple| - RevIndex)-th element in the tuple
// and call recursively to serialize the next element: (|tuple| - (RevIndex - 1))
// for simplicity we talk about the k-th element
template <typename Archive, int RevIndex, typename T, typename ... Args>
struct TupleDeserializer<Archive, RevIndex, std::tuple<T, Args ...> >{
    static std::tuple<T, Args ...> Deserialize(Archive& a) {
        // deserialize the k-th element and put it in a tuple
        auto head = std::make_tuple(Serializer<Archive, T>::Deserialize(a));
        // deserialize all elements i, i > k and concat the tuples
        return std::tuple_cat(head, TupleDeserializer<Archive, RevIndex - 1, std::tuple<Args ...> >::Deserialize(a));
    }
};

// Base Case when RevIndex == 0
template <typename Archive>
struct TupleDeserializer<Archive, 0, std::tuple<> >{
    static std::tuple<> Deserialize(Archive&) {
        return std::make_tuple();
    }
};

} // namespace serializer

//--------------------- tuple de-/serializer interface -----------------------//
template <typename Archive, typename ... Args>
struct Serializer<Archive, std::tuple<Args ...> >
{
    static void Serialize(const std::tuple<Args ...>& x, Archive& a) {
        serializer::TupleSerializer<Archive, sizeof ... (Args), Args ...>::Serialize(x, a);
    }
    static std::tuple<Args ...> Deserialize(Archive& a) {
        return serializer::TupleDeserializer<Archive, sizeof ... (Args), std::tuple<Args ...> >::Deserialize(a);
    }

    static const bool fixed_size = serializer::TupleSerializer<Archive, sizeof ... (Args), Args ...>::fixed_size;
};

/*********************** Call Serialize/Deserialize ***************************/

//! Serialize the type to std::string
template <typename Archive, typename T>
inline void Serialize(const T& x, Archive& a) {
    Serializer<Archive, T>::Serialize(x, a);
}

//! Deserialize the std::string to the given type
template <typename Archive, typename T>
inline T Deserialize(Archive& a) {
    return Serializer<Archive, T>::Deserialize(a);
}

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SERIALIZER_HEADER

/******************************************************************************/
