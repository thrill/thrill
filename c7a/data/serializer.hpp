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


//TODO DELETE
#include <iostream>


//TODO(ts) this copies data. That is bad and makes me sad.

namespace c7a {
namespace data {

//! internal representation of data elements
typedef std::string Blob;

//! \namespace namespace to hide the implementations of serializers
namespace serializers {

template <class T>
struct Impl;

//! identity serializer from string to string
template <>
struct Impl<std::string>{
    static std::string Serialize(const std::string& x) {
        return x;
    }

    static std::string Deserialize(const std::string& x) {
        return x;
    }
};

//! serializer for int
template <>
struct Impl<int>{
    static std::string Serialize(const int& x) {
        return std::to_string(x);
    }

    static int Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for long
template <>
struct Impl<long>{
    static std::string Serialize(const long& x) {
        return std::to_string(x);
    }

    static long Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for long long
template <>
struct Impl<long long>{
    static std::string Serialize(const long long& x) {
        return std::to_string(x);
    }

    static long long Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for unsigned
template <>
struct Impl<unsigned>{
    static std::string Serialize(const unsigned& x) {
        return std::to_string(x);
    }

    static unsigned Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for unsigned long
template <>
struct Impl<unsigned long>{
    static std::string Serialize(const unsigned long& x) {
        return std::to_string(x);
    }

    static unsigned long Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for unsigned long long
template <>
struct Impl<unsigned long long>{
    static std::string Serialize(const unsigned long long& x) {
        return std::to_string(x);
    }

    static unsigned long long Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for float
template <>
struct Impl<float>{
    static std::string Serialize(const float& x) {
        return std::to_string(x);
    }

    static float Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};

//! serializer for double
template <>
struct Impl<double>{
    static std::string Serialize(const double& x) {
        return std::to_string(x);
    }

    static double Deserialize(const std::string& x) {
        return std::stod(x);
    }
};

//! serializer for long double
template <>
struct Impl<long double>{
    static std::string Serialize(const long double& x) {
        return std::to_string(x);
    }

    static long double Deserialize(const std::string& x) {
        return std::stoi(x);
    }
};



//! serializer for (string, int) tuples
template <>
struct Impl<std::pair<std::string, int> >{
    static std::string Serialize(const std::pair<std::string, int>& x) {
        std::size_t len = sizeof(int) + x.first.size();
        char result[len];
        std::memcpy(result, &(x.second), sizeof(int));
        std::memcpy(result + sizeof(int), x.first.c_str(), x.first.size());
        return std::string(result, len);
    }
    //TODO(tb): What exactly happens with c_string. What is thiiiiiis?
    static std::pair<std::string, int> Deserialize(const std::string& x) {
        int i;
        std::size_t str_len = x.size() - sizeof(int);
        std::memcpy(&i, x.c_str(), sizeof(int));
        std::string s(x, sizeof(int), str_len);
        return std::pair<std::string, int>(s, i);
    }
};

// TODO(cn): do we have clusternodes working on 32 and 64bit systems at the same time??
//! serializer for pairs
template <typename T1, typename T2>
struct Impl<std::pair<T1, T2>> {
    static std::string Serialize(const std::pair<T1, T2>& x) {
        if( x.first.size() > UINT_MAX ) {
            //TODO ERROR
        }
        unsigned int len_t1 = static_cast<unsigned int>(x.first.size());
        std::string t1 = serializers::Impl<T1>::Serialize(x.first);
        std::string t2 = serializers::Impl<T2>::Serialize(x.second);

        std::size_t len = t1.size() + t2.size() + sizeof(unsigned int);
        char result[len];
        std::memcpy(result, &len_t1, sizeof(unsigned int));
        std::memcpy(result + sizeof(unsigned int), t1.c_str(), t1.size());
        std::memcpy(result + sizeof(unsigned int) + x.first.size(), t2.c_str(), t2.size());
        return std::string(result, len);
    }
    static std::pair<T1, T2> Deserialize(const std::string& x) {
        unsigned int len_t1;
        std::memcpy(&len_t1, x.c_str(), sizeof(unsigned int));
        std::size_t len_t2 = x.size() - sizeof(unsigned int) - len_t1;

        std::string t1_str = x.substr(sizeof(unsigned int), len_t1);
        std::string t2_str = x.substr(sizeof(unsigned int) + len_t1, len_t2);

        T1 t1 = serializers::Impl<T1>::Deserialize(t1_str);
        T1 t2 = serializers::Impl<T2>::Deserialize(t2_str);

        return std::pair<T1, T2>(t1, t2);
    }
};

//! binary serializer for any integral type, usable as template.
template <typename Type>
struct GenericImpl {
    static std::string Serialize(const Type& v) {
        return std::string(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    static Type        Deserialize(const std::string& s) {
        assert(s.size() == sizeof(Type));
        return Type(*reinterpret_cast<const Type*>(s.data()));
    }
};

template <>
struct Impl<std::pair<int, int> >: public GenericImpl<std::pair<int, int> >
{ };

} // namespace serializers

//! Serialize the type to std::string
template <class T>
inline std::string Serialize(const T& x) {
    return serializers::Impl<T>::Serialize(x);
}

//! Deserialize the std::string to the given type
template <class T>
inline T Deserialize(const std::string& x) {
    return serializers::Impl<T>::Deserialize(x);
}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_SERIALIZER_HEADER

/******************************************************************************/
