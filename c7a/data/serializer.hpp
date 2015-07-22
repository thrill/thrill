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

//TODO DELETE
#include <iostream>
#include <tests/data/serializer_objects.hpp>
// #include <build/c7a/proto/test_serialize_object.pb.h>
//TODO REMOVE CEREAL
#include <cereal/archives/c7a.hpp>
#include <cereal/archives/binary.hpp>
#include <sstream>

namespace c7a {
namespace data {

//! \namespace namespace to hide the implementations of serializers
namespace serializers {

template <typename Archive, typename T, class Enable = void>
struct Impl
{ };

template <typename Archive, typename T>
struct Impl<Archive, T,
            typename std::enable_if<std::is_pod<T>::value>::type>
{
    static void Serialize(const T& x, Archive& a) {
        // LOG << "is_pod serialization";
        a.template Put<T>(x);
    }
    static T Deserialize(Archive& a) {
        return a.template Get<T>();
    }
    static const bool fixed_size = true;
};

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

template <typename Archive>
struct Impl<Archive, struct TestCerealObject2>
{
    static void       Serialize(const struct TestCerealObject2& t, Archive& a) {
        cereal::c7aOutputArchive<Archive> oarchive(a); // Create an output archive
        oarchive(t);                                   // Write the data to the archive
    }

    static TestCerealObject2 Deserialize(Archive& a) {
        cereal::c7aInputArchive<Archive> iarchive(a);  // Create an output archive
        TestCerealObject2 res;
        iarchive(res);                                 // Read the data from the archive
        return res;
    }
    static const bool fixed_size = false;
};
}       // namespace serializers

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
