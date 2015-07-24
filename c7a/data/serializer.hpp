/*******************************************************************************
 * c7a/data/serializer.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SERIALIZER_HEADER
#define C7A_DATA_SERIALIZER_HEADER

#include <cassert>
#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include <c7a/common/logger.hpp>

#include <c7a/data/binary.hpp>
#include <tests/data/serializer_objects.hpp>
// #include <cereal/archives/binary.hpp>

#include <cereal/details/traits.hpp>

namespace c7a {
namespace data {

//! \namespace namespace to hide the implementations of serializers
namespace serializers {

static const bool debug = false;

template <typename Archive, typename T, class Enable = void>
struct Impl
{ };

/*************** Serialization of plain old data types *****************/
template <typename Archive, typename T>
struct Impl<Archive, T,
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

/******************* Serialization strings *********************/
template <typename Archive>
struct Impl<Archive, std::string>
{
    static void Serialize(const std::string& x, Archive& a) {
        a.PutString(x);
    }
    static std::string Deserialize(Archive& a) {
        return a.GetString();
    }
    static const bool fixed_size = false;
};

/***************** Serialization of pairs *************************/
template <typename Archive, typename U, typename V>
struct Impl<Archive, std::pair<U, V> >
{
    static void Serialize(const std::pair<U, V>& x, Archive& a) {
        Impl<Archive, U>::Serialize(x.first, a);
        Impl<Archive, V>::Serialize(x.second, a);
    }
    static std::pair<U, V> Deserialize(Archive& a) {
        U u = Impl<Archive, U>::Deserialize(a);
        V v = Impl<Archive, V>::Deserialize(a);
        return std::pair<U, V>(std::move(u), std::move(v));
    }
    static const bool fixed_size = (Impl<Archive, U>::fixed_size &&
                                    Impl<Archive, V>::fixed_size);
};

/****************** Serialization of tuples **************************/

//-------------- tuple serializer -------------//

// serialize the (|tuple| - RevIndex)-th element in the tuple
// and call recursively to serialize the next element: (|tuple| - (RevIndex - 1))
// for simplicity we talk about the k-th element
template <typename Archive, size_t RevIndex, typename ... Args>
struct TupleSerializer {
    static const size_t Index = sizeof ... (Args) - RevIndex;
    // type of k-th element
    using ThisElemType = typename std::tuple_element<Index, std::tuple<Args ...> >::type;
    static void         Serialize(const std::tuple<Args ...>& x, Archive& a) {
        sLOG0 << "Now serializing" << (sizeof ... (Args) - RevIndex);
        // serialize k-th element
        Impl<Archive, ThisElemType>::Serialize(std::get<Index>(x), a);
        // recursively serialize (k+1)-th element
        TupleSerializer<Archive, RevIndex - 1, Args ...>::Serialize(x, a);
    }
    static const bool   fixed_size = Impl<Archive, ThisElemType>::fixed_size && TupleSerializer<Archive, RevIndex - 1, Args ...>::fixed_size;
};

// Base case when RevIndex == 0
template <typename Archive, typename ... Args>
struct TupleSerializer<Archive, 0, Args ...>{
    static void Serialize(const std::tuple<Args ...>&, Archive&) {
        // Doesn't do anything
    }
    static const bool fixed_size = true;
};

//-------------- tuple deserializer -------------//

template <typename Archive, int RevIndex, typename T, typename ... Args>
struct TupleDeserializer { };

// deserialize the (|tuple| - RevIndex)-th element in the tuple
// and call recursively to serialize the next element: (|tuple| - (RevIndex - 1))
// for simplicity we talk about the k-th element
template <typename Archive, int RevIndex, typename T, typename ... Args>
struct TupleDeserializer<Archive, RevIndex, std::tuple<T, Args ...> >{
    static std::tuple<T, Args ...> Deserialize(Archive& a) {
        // deserialize the k-th element and put it in a tuple
        auto head = std::make_tuple(Impl<Archive, T>::Deserialize(a));
        // deserialize all elements i, i > k and concat the tuples
        return std::tuple_cat(head, TupleDeserializer<Archive, RevIndex - 1, std::tuple<Args ...> >::Deserialize(a));
    }
};

// Base Case when RevIndex == 1
template <typename Archive, typename T>
struct TupleDeserializer<Archive, 1, std::tuple<T> >{
    static std::tuple<T> Deserialize(Archive& a) {
        // deserialize the last element
        return std::make_tuple(Impl<Archive, T>::Deserialize(a));
    }
};

//------------ tuple de-/serializer interface -------------//

template <typename Archive, typename ... Args>
struct Impl<Archive, std::tuple<Args ...> >
{
    static void Serialize(const std::tuple<Args ...>& x, Archive& a) {
        TupleSerializer<Archive, sizeof ... (Args), Args ...>::Serialize(x, a);
    }
    static std::tuple<Args ...> Deserialize(Archive& a) {
        return TupleDeserializer<Archive, sizeof ... (Args), std::tuple<Args ...> >::Deserialize(a);
    }

    static const bool fixed_size = TupleSerializer<Archive, sizeof ... (Args), Args ...>::fixed_size;
};

/******************** Use cereal if serialization function is given **********************/

template <typename Archive, typename T>
struct Impl<Archive, T, typename std::enable_if<
                cereal::traits::is_input_serializable<T, Archive>::value&&
                !std::is_pod<T>::value
                >::type>
{
    static void Serialize(const T& t, Archive& a) {
        LOG << "Type T is " << typeid(T).name();
        c7aOutputArchive_cp<Archive> oarchive(a); // Create an output archive
        oarchive(t);                                   // Write the data to the archive
    }

    static T Deserialize(Archive& a) {
        c7aInputArchive_cp<Archive> iarchive(a);  // Create an output archive
        T res;
        iarchive(res);                                 // Read the data from the archive
        return res;
    }
    static const bool fixed_size = false;
};

/***************** Call Serialize/Deserialize *************************/

} // namespace serializers

//! Serialize the type to std::string
template <typename Archive, typename T>
inline void Serialize(const T& x, Archive& a) {
    serializers::Impl<Archive, T>::Serialize(x, a);
}

//! Deserialize the std::string to the given type
template <typename Archive, typename T>
inline T Deserialize(Archive& a) {
    return serializers::Impl<Archive, T>::Deserialize(a);
}

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SERIALIZER_HEADER

/******************************************************************************/
