/*******************************************************************************
 * tests/common/splay_tree_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/splay_tree.hpp>

#include <gtest/gtest.h>

#include <deque>
#include <iterator>
#include <random>
#include <set>
#include <vector>

using namespace thrill;

void compare(const common::SplayzTree<size_t>& tree,
             const std::set<size_t>& check) {
    std::vector<size_t> preorder;
    tree.traverse_preorder(
        [&](const size_t& t) { preorder.push_back(t); });

    std::vector<size_t> check_vec(check.begin(), check.end());
    ASSERT_EQ(check_vec, preorder);
}

TEST(SplayTree, Test1) {

    using Tree = common::SplayzTree<size_t>;

    Tree tree;
    std::set<size_t> check;

    for (size_t i = 0; i < 100; i++) {
        size_t value = (541 * i) & 1023;
        tree.insert(value);
        check.insert(value);
    }

    compare(tree, check);

    for (size_t i = 0; i < 100; i++) {
        size_t value = (541 * i) & 1023;
        tree.erase(value);
        check.erase(value);

        compare(tree, check);
    }

    for (size_t i = 0; i < 100; i++) {
        size_t value = (541 * i) & 1023;
        tree.insert(value);
        check.insert(value);
    }

    std::vector<size_t> check_vec(check.begin(), check.end());

    for (size_t i = 0; i <= 100; i++) {
        const Tree::Node* t = tree.rank(i);

        if (t != nullptr)
            ASSERT_EQ(check_vec[i], t->key);
        else
            ASSERT_TRUE(i >= check_vec.size());
    }

    for (size_t i = 0; i < 1000; i = i + 20) {
        const Tree::Node* t = tree.find(i);
        ASSERT_EQ(check.count(i) == 1, t->key == i);
    }

    for (size_t i = 0; i < 100; i++) {
        tree.erase((541 * i) & 1023);
    }
}

TEST(SplayTree, Random) {

    using Tree = common::SplayzTree<size_t>;

    Tree tree;
    std::deque<size_t> check;

    std::default_random_engine rng(1234);

    size_t limit = 1000;
    for (size_t i = 0; i < limit; ++i) {
        size_t op = rng() % 2;
        if (op == 0 && limit > i + check.size())
        {
            size_t v = rng();
            tree.insert(v);
            check.insert(std::lower_bound(check.begin(), check.end(), v), v);
        }
        else if (check.size())
        {
            size_t idx = rng() % check.size();
            auto it = check.begin() + idx;

            ASSERT_TRUE(tree.exists(*it));
            tree.erase(*it);
            check.erase(it);
        }

        std::deque<size_t> preorder;
        tree.traverse_preorder(
            [&](const size_t& t) { preorder.push_back(t); });

        ASSERT_EQ(check, preorder);
    }
}

/******************************************************************************/
