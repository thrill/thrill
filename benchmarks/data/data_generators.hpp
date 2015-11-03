/*******************************************************************************
 * benchmarks/data/data_generators.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_BENCHMARKS_DATA_DATA_GENERATORS_HEADER
#define THRILL_BENCHMARKS_DATA_DATA_GENERATORS_HEADER

#include <thrill/common/functional.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

template <typename Type>
class Generator;

template <>
class Generator<size_t>
{
public:
    explicit Generator(size_t bytes,
                       size_t = 0 /* min_size */, size_t = 0 /* max_size */)
        : size_((bytes + sizeof(size_t) - 1) / sizeof(size_t)),
          bytes_(size_ * sizeof(size_t)) { }

    bool HasNext() const { return size_ > 0; }

    size_t Next() {
        assert(size_ > 0);
        --size_;
        return index_++;
    }

    size_t TotalBytes() const { return bytes_; }

private:
    size_t size_;
    size_t index_ = 42;
    size_t bytes_;
};

template <>
class Generator<std::string>
{
public:
    explicit Generator(size_t bytes, size_t min_size = 0, size_t max_size = 0)
        : bytes_(bytes), remain_(bytes),
          uniform_dist_(min_size, max_size) { }

    bool HasNext() const { return remain_ > 0; }

    std::string Next() {
        size_t next_size = std::min<size_t>(uniform_dist_(randomness_), remain_);
        remain_ -= next_size;
        return std::string(next_size, 'f');
    }

    size_t TotalBytes() const { return bytes_; }

private:
    ssize_t bytes_;
    ssize_t remain_;

    // init randomness
    std::default_random_engine randomness_ { std::random_device { } () };
    std::uniform_int_distribution<size_t> uniform_dist_;
};

/******************************************************************************/

template <size_t Index, typename ... Types>
struct TupleGenerator {
    static bool   HasNext(const std::tuple<Generator<Types>...>& t) {
        return std::get<Index - 1>(t).HasNext() &&
               TupleGenerator<Index - 1, Types ...>::HasNext(t);
    }
    static size_t TotalBytes(const std::tuple<Generator<Types>...>& t) {
        return std::get<Index - 1>(t).TotalBytes() +
               TupleGenerator<Index - 1, Types ...>::TotalBytes(t);
    }
};

template <typename ... Types>
struct TupleGenerator<0, Types ...>{
    static bool HasNext(const std::tuple<Generator<Types>...>&) {
        return true;
    }
    static size_t TotalBytes(const std::tuple<Generator<Types>...>&) {
        return 0;
    }
};

template <size_t ... Is, typename ... Types>
auto TupleGeneratorNext(std::tuple<Generator<Types>...>&t,
                        common::index_sequence<Is ...>)
{
    return std::make_tuple(std::get<Is>(t).Next() ...);
}

template <typename ... Types>
class Generator<std::tuple<Types ...> >
{
public:
    explicit Generator(size_t bytes, size_t min_size = 0, size_t max_size = 0)
        : gen_(Generator<Types>(bytes, min_size, max_size) ...) { }

    bool HasNext() const {
        return TupleGenerator<sizeof ... (Types), Types ...>::HasNext(gen_);
    }

    std::tuple<Types ...> Next() {
        const size_t Size = sizeof ... (Types);
        return TupleGeneratorNext(gen_, common::make_index_sequence<Size>{ });
    }

    size_t TotalBytes() const {
        return TupleGenerator<sizeof ... (Types), Types ...>::TotalBytes(gen_);
    }

private:
    std::tuple<Generator<Types>...> gen_;
};

/******************************************************************************/

using Tuple = std::pair<std::string, int>;
using Triple = std::tuple<std::string, int, std::string>;

template <typename Type>
std::vector<Type> generate(size_t bytes, size_t min_size = 0, size_t max_size = 0);

template <>
std::vector<std::string> generate(size_t bytes, size_t min_size, size_t max_size) {
    std::vector<std::string> result;
    size_t remaining = bytes;

    // init randomness
    std::default_random_engine randomness(std::random_device { } ());
    std::uniform_int_distribution<size_t> uniform_dist(min_size, max_size);

    while (remaining > 0) {
        size_t next_size = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size;
        result.emplace_back(std::string(next_size, 'f'));
    }
    return result;
}

template <>
std::vector<Tuple> generate(size_t bytes, size_t min_size, size_t max_size) {
    std::vector<Tuple> result;
    size_t remaining = bytes;

    // init randomness
    std::default_random_engine randomness(std::random_device { } ());
    std::uniform_int_distribution<size_t> uniform_dist(min_size, max_size);

    while (remaining > 0) {
        remaining -= sizeof(int);
        size_t next_size = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size;
        result.push_back(Tuple(std::string(next_size, 'f'), 42));
    }
    return result;
}

template <>
std::vector<Triple> generate(size_t bytes, size_t min_size, size_t max_size) {
    std::vector<Triple> result;
    size_t remaining = bytes;

    // init randomness
    std::default_random_engine randomness(std::random_device { } ());
    std::uniform_int_distribution<size_t> uniform_dist(min_size, max_size);

    while (remaining > 0) {
        remaining -= sizeof(int);
        size_t next_size1 = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size1;
        size_t next_size2 = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size2;
        result.push_back(Triple(std::string(next_size1, 'f'), 42,
                                std::string(next_size2, 'g')));
    }
    return result;
}

//! Generates random integers in the whole int-range
template <>
std::vector<int> generate(size_t bytes, size_t /*min_size*/, size_t /*max_size*/) {
    assert(bytes % sizeof(int) == 0);
    std::vector<int> result;
    result.reserve((bytes + sizeof(int) - 1) / sizeof(int));

    for (size_t current = 0; current < bytes; current += sizeof(int)) {
        result.emplace_back(static_cast<int>(42 + current));
    }
    return result;
}

//! Generates random integers in the whole size_t-range
template <>
std::vector<size_t> generate(size_t bytes, size_t /*min_size*/, size_t /*max_size*/) {
    assert(bytes % sizeof(size_t) == 0);
    std::vector<size_t> result;
    result.reserve((bytes + sizeof(size_t) - 1) / sizeof(size_t));

    for (size_t current = 0; current < bytes; current += sizeof(size_t)) {
        result.emplace_back(static_cast<int>(42 + current));
    }
    return result;
}

#endif // !THRILL_BENCHMARKS_DATA_DATA_GENERATORS_HEADER

/******************************************************************************/
