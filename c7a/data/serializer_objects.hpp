#pragma once

namespace c7a {
namespace data {
namespace serializers {

struct TestSerializeObject;

struct TestSerializeObject {
    TestSerializeObject(int bla, int blu) : bla_(bla), blu_(blu) { }
    int bla_;
    int blu_;
};
}
}
}
