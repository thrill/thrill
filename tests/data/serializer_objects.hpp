/*******************************************************************************
 * tests/data/serializer_objects.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_TESTS_DATA_SERIALIZER_OBJECTS_HEADER
#define C7A_TESTS_DATA_SERIALIZER_OBJECTS_HEADER
#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>
#include <string>
#include <unordered_map>

namespace c7a {
namespace data {

// struct ProtobufObject {
//     TestSerializeObject(int bla, int blu) : bla_(bla), blu_(blu) { }
//     int bla_;
//     int blu_;
// };

//! Test struct with ints that provides a cereal interface
struct CerealObject3 {
    int x_, y_, z_;

    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_);
    }
};

//! Test struct with ints and another struct that provides a cereal interface
struct CerealObject2 {
    CerealObject2() { }
    CerealObject2(int x, int y, int z) : x_(x), y_(y), z_(z) {
        tco.x_ = x_;
        tco.y_ = y_;
        tco.z_ = z_;
    }

    int           x_, y_, z_;
    CerealObject3 tco;

    // This method lets cereal know which data members to serialize
    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_, tco);
    }
};

/*!
 * Test struct with more advanced data structures (string, vector)
 * that provides a cereal interface
 */
struct CerealObject
{
    uint8_t                  x, y;
    float                    z;
    std::string              a;
    std::vector<std::string> b;

    template <class Archive>
    void serialize(Archive& ar) {
        ar(x, y, z, a, b);
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_TESTS_DATA_SERIALIZER_OBJECTS_HEADER

/******************************************************************************/
