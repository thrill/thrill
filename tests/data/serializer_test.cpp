/*******************************************************************************
 * tests/data/serializer_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/logger.hpp>
#include <c7a/data/serializer.hpp>
#include <c7a/data/file.hpp>
#include <tests/data/serializer_objects.hpp>

#include <utility>
#include <string>
#include <typeinfo>

#include "gtest/gtest.h"

using namespace c7a::data;

static const bool debug = true;

// TEST(Serializer, StringSerializeDeserialize) {
//     std::string foo = "foo";
//     auto fooserial = Deserialize<std::string>(Serialize<std::string>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, IntSerializeDeserialize) {
//     int foo = -123;
//     auto fooserial = Deserialize<int>(Serialize<int>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, LongSerializeDeserialize) {
//     long foo = -123;
//     auto fooserial = Deserialize<long>(Serialize<long>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, LongLongSerializeDeserialize) {
//     long long foo = -123;
//     auto fooserial = Deserialize<long long>(Serialize<long long>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, UnsignedSerializeDeserialize) {
//     unsigned int foo = 2154910440;
//     auto fooserial = Deserialize<unsigned int>(Serialize<unsigned int>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
//     ASSERT_EQ(sizeof(foo), sizeof(fooserial));
// }

// TEST(Serializer, UnsignedLongSerializeDeserialize) {
//     unsigned long foo = 123;
//     auto fooserial = Deserialize<unsigned long>(Serialize<unsigned long>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, UnsignedLongLongSerializeDeserialize) {
//     unsigned long long foo = 123;
//     auto fooserial = Deserialize<unsigned long long>(Serialize<unsigned long long>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, FloatSerializeDeserialize) {
//     float foo = 123.123;
//     auto fooserial = Deserialize<float>(Serialize<float>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, DoubleSerializeDeserialize) {
//     double foo = 123.123;
//     auto fooserial = Deserialize<double>(Serialize<double>(foo));
//     ASSERT_DOUBLE_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, LongDoubleSerializeDeserialize) {
//     long double foo = 123.123;
//     auto fooserial = Deserialize<long double>(Serialize<long double>(foo));
//     ASSERT_DOUBLE_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, SizeTSerializeDeserialize) {
//     size_t foo = 123;
//     auto fooserial = Deserialize<size_t>(Serialize<size_t>(foo));
//     ASSERT_EQ(foo, fooserial);
//     ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
// }

// TEST(Serializer, StringIntPairSerializeDeserialize) {
//     auto t = std::make_pair("foo", 123);
//     auto serialized = Serialize<std::pair<std::string, int> >(t);
//     auto result = Deserialize<std::pair<std::string, int> >(serialized);
//     ASSERT_EQ(std::get<0>(t), std::get<0>(result));
//     ASSERT_EQ(std::get<1>(t), std::get<1>(result));
// }

// TEST(Serializer, IntString_Pair_SerializeDeserialize_Test) {
//     auto t1 = 3;
//     auto t2 = "4";
//     auto t = std::make_pair(t1, t2);

//     auto serialized = Serialize<std::pair<int, std::string> >(t);
//     auto result = Deserialize<std::pair<int, std::string> >(serialized);

//     ASSERT_EQ(std::get<0>(t), std::get<0>(result));
//     ASSERT_EQ(std::get<1>(t), std::get<1>(result));
// }

// TEST(Serializer, StringString_Pair_SerializeDeserialize_Test) {
//     auto t1 = "first";
//     auto t2 = "second";
//     auto t = std::make_pair(t1, t2);

//     auto serialized = Serialize<std::pair<std::string, std::string> >(t);
//     auto result = Deserialize<std::pair<std::string, std::string> >(serialized);

//     ASSERT_EQ(std::get<0>(t), std::get<0>(result));
//     ASSERT_EQ(std::get<1>(t), std::get<1>(result));
// }

// TEST(Serializer, IntInt_Pair_SerializeDeserialize_Test) {
//     auto t1 = 3;
//     auto t2 = 4;
//     auto t = std::make_pair(t1, t2);

//     auto serialized = Serialize<std::pair<int, int> >(t);
//     auto result = Deserialize<std::pair<int, int> >(serialized);

//     ASSERT_EQ(std::get<0>(t), std::get<0>(result));
//     ASSERT_EQ(std::get<1>(t), std::get<1>(result));
// }

// TEST(Serializer, Tuple_SerializeDeserialize_Test) {
//     auto t = std::make_tuple(3, "foo", 5.5);

//     auto serialized = Serialize<std::tuple<int, std::string, float> >(t);
//     auto result = Deserialize<std::tuple<int, std::string, float> >(serialized);
//     ASSERT_EQ(std::get<0>(t), std::get<0>(result));
//     ASSERT_EQ(std::get<1>(t), std::get<1>(result));
//     ASSERT_EQ(std::get<2>(t), std::get<2>(result));
// }

// TEST(Serializer, TuplePair_SerializeDeserialize_Test) {
//     auto p = std::make_pair(-4.673, "string");
//     auto t = std::make_tuple(3, "foo", 5.5, p);

//     auto serialized = Serialize<std::tuple<int, std::string, float, std::pair<float, std::string> > >(t);
//     auto result = Deserialize<std::tuple<int, std::string, float, std::pair<float, std::string> > >(serialized);
//     ASSERT_EQ(std::get<0>(t), std::get<0>(result));
//     ASSERT_EQ(std::get<1>(t), std::get<1>(result));
//     ASSERT_EQ(std::get<2>(t), std::get<2>(result));
//     ASSERT_FLOAT_EQ(std::get<3>(t).first, std::get<3>(result).first);
//     ASSERT_EQ(std::get<3>(t).second, std::get<3>(result).second);
// }

// TEST(Serializer, ProtoBuf_Test) {
//     serializers::TestSerializeObject t(1, 2);
//     auto serialized = Serialize<serializers::TestSerializeObject>(t);
//     auto result = Deserialize<serializers::TestSerializeObject>(serialized);
//     ASSERT_EQ(t.bla_, result.bla_);
//     ASSERT_EQ(t.blu_, result.blu_);
// }

// TEST(Serializer, Cereal_Test) {
//     serializers::TestCerealObject t;
//     t.x_ = 1;
//     t.y_ = 2;
//     t.z_ = 3;
//     auto serialized = Serialize<serializers::TestCerealObject>(t);
//     auto result = Deserialize<serializers::TestCerealObject>(serialized);
//     ASSERT_EQ(t.x_, result.x_);
//     ASSERT_EQ(t.y_, result.y_);
//     ASSERT_EQ(t.z_, result.z_);
//     LOG << "X: " << result.x_;
//     LOG << "Y: " << result.y_;
//     LOG << "Z: " << result.z_;
// }

TEST(Serializer, Cereal_Archive_Test) {
    c7a::data::File f;
    serializers::TestCerealObject2 t(1, 2, 3);

    {
        auto w = f.GetWriter();
        Serialize<decltype(w), serializers::TestCerealObject2>(t, w);
    }

    auto r = f.GetReader();
    auto res = Deserialize<decltype(r), serializers::TestCerealObject2>(r);
    ASSERT_EQ(t.x_, res.x_);
    ASSERT_EQ(t.y_, res.y_);
    ASSERT_EQ(t.z_, res.z_);
    ASSERT_EQ(t.tco.z_, res.tco.z_);
    sLOG << res.x_ << res.y_ << res.z_;
}

/******************************************************************************/
