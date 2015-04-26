#pragma once
#include <string>
#include <cstring> //string copy


//TODO this copies data. That is bad and makes me sad.

namespace c7a {
namespace data {

//! internal representation of data elements
typedef std::string Blob;

//! \namepsace namespace to hide the implementations of serializers
namespace serializers {
    template<class T>
    struct Impl;

    //! identity serializer from string to string
    template <>
    struct Impl<std::string> {
        static std::string Serialize(const std::string& x) {
            return x;
        }

        static std::string Deserialize(const std::string &x) {
            return x;
        }
    };

    //! serializer for int
    template<>
        struct Impl<int> {
        static std::string Serialize(const int& x) {
            return std::to_string(x);
        }

        static int Deserialize(const std::string& x) {
            return std::stoi(x);
        }
    };
    
    //! serializer for double
    template<>
        struct Impl<double> {
        static std::string Serialize(const double& x) {
            return std::to_string(x);
        }

        static double Deserialize(const std::string& x) {
            return std::stod(x);
        }
    };

    //! serializer for double
    template<>
        struct Impl<double> {
        static std::string Serialize(const double& x) {
            return std::to_string(x);
        }

        static double Deserialize(const std::string& x) {
            return std::stod(x);
        }
    };

    //! serializer for (string, int) tuples
    template <>
    struct Impl<std::pair<std::string, int>> {
        static std::string Serialize(const std::pair<std::string, int>& x) {
            std::size_t len = sizeof(int) + x.first.size();
            char result[len];
            std::memcpy(result, &(x.second), sizeof(int));
            std::memcpy(result + sizeof(int), x.first.c_str(),
                        sizeof(x.first.size()));
            return std::string(result, len);
        }

        static std::pair<std::string, int> Deserialize(const std::string& x) {
            int i;
            std::size_t str_len = x.size() - sizeof(int);
            std::memcpy(&i, x.c_str(), sizeof(int));
            std::string s(x, sizeof(int), str_len);
            return std::pair<std::string, int>(s, i);
        }
    };
}

//! Serialize the type to std::string
template<class T>
inline std::string Serialize(const T& x) {
    return serializers::Impl<T>::Serialize(x);
}

//! Deserialize the std::string to the given type
template<class T>
inline T Deserialize(const std::string& x) {
    return serializers::Impl<T>::Deserialize(x);
}
}
}
