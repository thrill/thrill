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

#include <string>
#include <cstring>
#include <utility>
#include <cassert>
#include <tuple>
#include <type_traits>

#include <c7a/common/logger.hpp>

#include <tests/data/serializer_objects.hpp>
#include <c7a/data/serializer_cereal_archive.hpp>
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
template <typename Archive, int N, typename ... Args>
struct TupleSerializer {
    using ThisElemType = typename std::tuple_element<N, std::tuple<Args ...> >::type;
    static void       Serialize(const std::tuple<Args ...>& x, Archive& a) {
        sLOG << "Now serializing" << std::get<N>(x);
        Impl<Archive, ThisElemType>::Serialize(std::get<N>(x), a);
        TupleSerializer<Archive, N + 1, Args ...>::Serialize(x, a);
    }
    static const bool fixed_size = Impl<Archive, ThisElemType>::fixed_size && TupleSerializer<Archive, N + 1, Args ...>::fixed_size;
};

// Base case
template <typename Archive, typename ... Args>
struct TupleSerializer<Archive, sizeof ... (Args), Args ...>{
    static void Serialize(const std::tuple<Args ...>&, Archive&) {
        // Doesn't do anything
    }
    static const bool fixed_size = true;
};

template <typename Archive, int N, typename ... Args>
struct TupleDeserializer {
    using ThisElemType = typename std::tuple_element<N, std::tuple<Args ...> >::type;
    static void Deserialize(std::tuple<Args ...>& t, Archive& a) {
        // deserialize and fill the result tuple
        std::get<N>(t) = Impl<Archive, ThisElemType>::Deserialize(a);
        TupleDeserializer<Archive, N + 1, Args ...>::Deserialize(t, a);
    }
};

// Base Case
template <typename Archive, typename ... Args>
struct TupleDeserializer<Archive, sizeof ... (Args), Args ...>{
    static void Deserialize(std::tuple<Args ...>&, Archive&) {
        // Doesn't do anything
    }
};

template <typename Archive, typename ... Args>
struct Impl<Archive, std::tuple<Args ...> >
{
    static void Serialize(const std::tuple<Args ...>& x, Archive& a) {
        TupleSerializer<Archive, 0, Args ...>::Serialize(x, a);
    }
    static std::tuple<Args ...> Deserialize(Archive& a) {
        std::tuple<Args ...> r;
        TupleDeserializer<Archive, 0, Args ...>::Deserialize(r, a);
        return r;
    }

    static const bool fixed_size = TupleSerializer<Archive, 0, Args ...>::fixed_size;
};

/******************** Use cereal if serialization function is given **********************/

//?????

template <typename Archive, typename T>
struct Impl<Archive, T, typename std::enable_if<
                cereal::traits::is_input_serializable<T, Archive>::value&&
                !std::is_pod<T>::value
                >::type>
{
    static void Serialize(const T& t, Archive& a) {
        LOG << "Type T is " << typeid(T).name();
        cereal::c7aOutputArchive<Archive> oarchive(a); // Create an output archive
        oarchive(t);                                   // Write the data to the archive
    }

    static T Deserialize(Archive& a) {
        cereal::c7aInputArchive<Archive> iarchive(a);  // Create an output archive
        TestCerealObject2 res;
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

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SERIALIZER_HEADER

/******************************************************************************/
