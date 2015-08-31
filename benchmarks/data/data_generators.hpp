/*******************************************************************************
 * benchmarks/data/data_generators.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
    explicit Generator(size_t bytes)
        : size_((bytes + sizeof(size_t) - 1) / sizeof(size_t)) { }

    bool HasNext() const { return size_ > 0; }

    size_t Next() {
        assert(size_ > 0);
        --size_;
        return index_++;
    }

protected:
    size_t size_;
    size_t index_ = 42;
};

template <>
class Generator<std::string>
{
public:
    explicit Generator(size_t bytes)
        : bytes_(bytes) { }

    bool HasNext() const { return bytes_ > 0; }

    std::string Next() {
        size_t next_size = std::min<size_t>(uniform_dist_(randomness_), bytes_);
        bytes_ -= next_size;
        return std::string('f', next_size);
    }

protected:
    ssize_t bytes_;

    // init randomness
    std::default_random_engine randomness_ { std::random_device()() };
    std::uniform_int_distribution<size_t> uniform_dist_ { 1, 100 };
};

/******************************************************************************/

template <size_t Index, typename ... Types>
struct TupleGenerator {
    static bool HasNext(const std::tuple<Generator<Types>...>& t) {
        return std::get<Index - 1>(t).HasNext() &&
               TupleGenerator<Index - 1, Types ...>::HasNext(t);
    }
};

template <typename ... Types>
struct TupleGenerator<0, Types ...>{
    static bool HasNext(const std::tuple<Generator<Types>...>&) {
        return true;
    }
};

template <std::size_t ... Is, typename ... Types>
auto TupleGeneratorNext(std::tuple<Generator<Types>...>&t,
                        common::index_sequence<Is ...>)
{
    return std::make_tuple(std::get<Is>(t).Next() ...);
}

template <typename ... Types>
class Generator<std::tuple<Types ...> >
{
public:
    explicit Generator(size_t bytes)
        : gen_(Generator<Types>(bytes) ...) { }

    bool HasNext() const {
        return TupleGenerator<sizeof ... (Types), Types ...>::HasNext(gen_);
    }

    std::tuple<Types ...> Next() {
        const size_t Size = sizeof ... (Types);
        return TupleGeneratorNext(gen_, common::make_index_sequence<Size>{ });
    }

protected:
    std::tuple<Generator<Types>...> gen_;
};

/******************************************************************************/

using Tuple = std::pair<std::string, int>;
using Triple = std::tuple<std::string, int, std::string>;

template <typename Type>
std::vector<Type> generate(size_t bytes, size_t min_size, size_t max_size);

template <>
std::vector<std::string> generate(size_t bytes, size_t min_size, size_t max_size) {
    std::vector<std::string> result;
    size_t remaining = bytes;

    // init randomness
    std::default_random_engine randomness({ std::random_device()() });
    std::uniform_int_distribution<size_t> uniform_dist(min_size, max_size);

    while (remaining > 0) {
        size_t next_size = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size;
        result.emplace_back(std::string('f', next_size));
    }
    return result;
}

template <>
std::vector<Tuple> generate(size_t bytes, size_t min_size, size_t max_size) {
    std::vector<Tuple> result;
    size_t remaining = bytes;

    // init randomness
    std::default_random_engine randomness({ std::random_device()() });
    std::uniform_int_distribution<size_t> uniform_dist(min_size, max_size);

    while (remaining > 0) {
        remaining -= sizeof(int);
        size_t next_size = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size;
        result.push_back(Tuple(std::string('f', next_size), 42));
    }
    return result;
}

template <>
std::vector<Triple> generate(size_t bytes, size_t min_size, size_t max_size) {
    std::vector<Triple> result;
    size_t remaining = bytes;

    // init randomness
    std::default_random_engine randomness({ std::random_device()() });
    std::uniform_int_distribution<size_t> uniform_dist(min_size, max_size);

    while (remaining > 0) {
        remaining -= sizeof(int);
        size_t next_size1 = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size1;
        size_t next_size2 = std::min(uniform_dist(randomness), remaining);
        remaining -= next_size2;
        result.push_back(Triple(std::string('f', next_size1), 42, std::string('g', next_size2)));
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
        result.emplace_back(42 + current);
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
        result.emplace_back(42 + current);
    }
    return result;
}

#endif // !THRILL_BENCHMARKS_DATA_DATA_GENERATORS_HEADER

/******************************************************************************/
