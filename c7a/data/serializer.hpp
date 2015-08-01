/*******************************************************************************
 * c7a/data/serializer.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_SERIALIZER_HEADER
#define C7A_DATA_SERIALIZER_HEADER

#include <c7a/common/logger.hpp>
#include <cassert>
#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

//cereal
#include <c7a/data/serialization_cereal.hpp>
#include <cereal/details/traits.hpp>

namespace c7a {
namespace data {

//! \namespace to hide the implementations of serializers
namespace serializers {

static const bool debug = false;

template <typename Archive, typename T, class Enable = void>
struct Serializer
{ };

/******************* Serialization of plain old data types ********************/
template <typename Archive, typename T>
struct Serializer<Archive, T,
                  typename std::enable_if<std::is_pod<T>::value>::type>
{
    //! serializes plain old data structures and writes into given archive
    static void Serialize(const T& x, Archive& a) {
        a.template Put<T>(x);
    }
    //! deserializes a plain old data structure from a given archive
    static T Deserialize(Archive& a) {
        return a.template Get<T>();
    }
    static const bool fixed_size = true;
};

/********************** Serialization of strings ******************************/
template <typename Archive>
struct Serializer<Archive, std::string>
{
    //! serializes a string and writes into given archive
    static void Serialize(const std::string& x, Archive& a) {
        a.PutString(x);
    }
    //! deserializes a string from a given archive
    static std::string Deserialize(Archive& a) {
        return a.GetString();
    }
    static const bool fixed_size = false;
};

/*********************** Serialization of pairs *******************************/
template <typename Archive, typename U, typename V>
struct Serializer<Archive, std::pair<U, V> >
{
    /* !
     * serializes a pair by serializing its elements
     * and writes into given archive
     */
    static void Serialize(const std::pair<U, V>& x, Archive& a) {
        Serializer<Archive, U>::Serialize(x.first, a);
        Serializer<Archive, V>::Serialize(x.second, a);
    }
    /* !
     * deserializes a pair by serializing its elements
     * and writes into given archive
     */
    static std::pair<U, V> Deserialize(Archive& a) {
        U u = Serializer<Archive, U>::Deserialize(a);
        V v = Serializer<Archive, V>::Deserialize(a);
        return std::pair<U, V>(std::move(u), std::move(v));
    }
    static const bool fixed_size = (Serializer<Archive, U>::fixed_size &&
                                    Serializer<Archive, V>::fixed_size);
};

/*********************** Serialization of tuples ******************************/
/*!
 * Serializes the (|tuple| - RevIndex)-th element in the tuple
 * and recursively serializes the next element: (|tuple| - (RevIndex - 1)).
 * For simplicity we talk about the k-th element.
 */
template <typename Archive, size_t RevIndex, typename ... Args>
struct TupleSerializer {

    static const size_t Index = sizeof ... (Args) - RevIndex;
    // type of k-th element
    using ThisElemType = typename std::tuple_element<Index, std::tuple<Args ...> >::type;

    static void         Serialize(const std::tuple<Args ...>& x, Archive& a) {
        sLOG << "Now serializing" << (sizeof ... (Args) - RevIndex);
        // serialize k-th element
        Serializer<Archive, ThisElemType>::Serialize(std::get<Index>(x), a);
        // recursively serialize (k+1)-th element
        TupleSerializer<Archive, RevIndex - 1, Args ...>::Serialize(x, a);
    }

    static const bool   fixed_size = Serializer<Archive, ThisElemType>::fixed_size
                                     && TupleSerializer<Archive, RevIndex - 1, Args ...>::fixed_size;
};

/*!
 * Base case for serialization of tuples after all elements have already been
 * serialized. Doesn't do anything except for ending the recursion.
 */
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

/*!
 * Deserialize the (|tuple| - RevIndex)-th element in the tuple
 * and recursively deserializes the next element: (|tuple| - (RevIndex - 1)).
 * For simplicity we talk about the k-th element
 */
template <typename Archive, int RevIndex, typename T, typename ... Args>
struct TupleDeserializer<Archive, RevIndex, std::tuple<T, Args ...> >{
    static std::tuple<T, Args ...> Deserialize(Archive& a) {
        // deserialize the k-th element and put it in a tuple
        auto head = std::make_tuple(Serializer<Archive, T>::Deserialize(a));
        // deserialize all elements i, i > k and concat the tuples
        return std::tuple_cat(head, TupleDeserializer<Archive, RevIndex - 1, std::tuple<Args ...> >::Deserialize(a));
    }
};

/*!
 * Base case for deserialization of tuples after all elements have already been
 * deserialized. Doesn't do anything except for ending the recursion.
 */
template <typename Archive>
struct TupleDeserializer<Archive, 0, std::tuple<> >{
    static std::tuple<> Deserialize(Archive&) {
        return std::make_tuple();
    }
};

//--------------------- tuple de-/serializer interface -----------------------//
template <typename Archive, typename ... Args>
struct Serializer<Archive, std::tuple<Args ...> >
{
    static void Serialize(const std::tuple<Args ...>& x, Archive& a) {
        TupleSerializer<Archive, sizeof ... (Args), Args ...>::Serialize(x, a);
    }
    static std::tuple<Args ...> Deserialize(Archive& a) {
        return TupleDeserializer<Archive, sizeof ... (Args), std::tuple<Args ...> >::Deserialize(a);
    }

    static const bool fixed_size = TupleSerializer<Archive, sizeof ... (Args), Args ...>::fixed_size;
};

/************** Use cereal if serialization function is given *****************/
template <typename Archive, typename T>
struct Serializer<Archive, T, typename std::enable_if<
                      cereal::traits::is_input_serializable<T, Archive>::value&&
                      !std::is_pod<T>::value
                      >::type>
{
    //! serializes an object by using cereal with the c7a-cereal archive
    static void Serialize(const T& t, Archive& a) {
        LOG << "Type T is " << typeid(T).name();
        serialization_cereal::c7aOutputArchive<Archive> oarchive(a); // Create an output archive
        oarchive(t);                                                 // Write the data to the archive
    }

    //! deserializes an object by using cereal with the c7a-cereal archive
    static T Deserialize(Archive& a) {
        serialization_cereal::c7aInputArchive<Archive> iarchive(a); // Create an output archive
        T res;
        iarchive(res);                                              // Read the data from the archive
        return res;
    }
    static const bool fixed_size = false;
};

/*********************** Call Serialize/Deserialize ***************************/

} // namespace serializers

//! Serialize the type to std::string
template <typename Archive, typename T>
inline void Serialize(const T& x, Archive& a) {
    serializers::Serializer<Archive, T>::Serialize(x, a);
}

//! Deserialize the std::string to the given type
template <typename Archive, typename T>
inline T Deserialize(Archive& a) {
    return serializers::Serializer<Archive, T>::Deserialize(a);
}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SERIALIZER_HEADER

/******************************************************************************/
