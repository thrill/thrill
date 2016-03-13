/*******************************************************************************
 * thrill/common/splay_tree.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SPLAY_TREE_HEADER
#define THRILL_COMMON_SPLAY_TREE_HEADER

/*
  Adapted as a Generic Data structure with Template C++ by AG

           An implementation of top-down splaying with sizes
             D. Sleator <sleator@cs.cmu.edu>, January 1994.

  This extends top-down-splay.c to maintain a size field in each node.  This is
  the number of nodes in the subtree rooted there.  This makes it possible to
  efficiently compute the rank of a key.  (The rank is the number of nodes to
  the left of the given key.)  It it also possible to quickly find the node of a
  given rank.  Both of these operations are illustrated in the code below.  The
  remainder of this introduction is taken from top-down-splay.c.

  "Splay trees", or "self-adjusting search trees" are a simple and efficient
  data structure for storing an ordered set.  The data structure consists of a
  binary tree, with no additional fields.  It allows searching, insertion,
  deletion, deletemin, deletemax, splitting, joining, and many other operations,
  all with amortized logarithmic performance.  Since the trees adapt to the
  sequence of requests, their performance on real access patterns is typically
  even better.  Splay trees are described in a number of texts and papers
  [1,2,3,4].

  The code here is adapted from simple top-down splay, at the bottom of page 669
  of [2].  It can be obtained via anonymous ftp from spade.pc.cs.cmu.edu in
  directory /usr/sleator/public.

  The chief modification here is that the splay operation works even if the item
  being splayed is not in the tree, and even if the tree root of the tree is
  nullptr.  So the line:

                              t = splay(i, t);

  causes it to search for item with key i in the tree rooted at t.  If it's
  there, it is splayed to the root.  If it isn't there, then the node put at the
  root is the last one before nullptr that would have been reached in a normal
  binary search for i.  (It's a neighbor of i in the tree.)  This allows many
  other operations to be easily implemented, as shown below.

  [1] "Data Structures and Their Algorithms", Lewis and Denenberg,
       Harper Collins, 1991, pp 243-251.
  [2] "Self-adjusting Binary Search Trees" Sleator and Tarjan,
       JACM Volume 32, No 3, July 1985, pp 652-686.
  [3] "Data Structure and Algorithm Analysis", Mark Weiss,
       Benjamin Cummins, 1992, pp 119-130.
  [4] "Data Structures, Algorithms, and Performance", Derick Wood,
       Addison-Wesley, 1993, pp 367-375
*/

#include <cassert>
#include <iostream>

namespace thrill {
namespace common {

/******************************************************************************/
// Free Splay Tree methods with tree sizes

template <typename Tree>
inline size_t splayz_size(Tree* x) {
    return (x == nullptr) ? 0 : x->size;
}

//! Splay using the key i (which may or may not be in the tree.)  The starting
//! root is t, and the tree used is defined by rat size fields are maintained.
template <typename Key, typename Tree>
inline Tree * splayz(const Key& k, Tree* t) {
    Tree N, * l, * r, * y;
    size_t root_size, l_size, r_size;

    if (t == nullptr) return t;
    N.left = N.right = nullptr;
    l = r = &N;
    root_size = splayz_size(t);
    l_size = r_size = 0;

    for ( ; ; ) {
        if (k < t->key) {
            if (t->left == nullptr) break;

            if (k < t->left->key) {
                y = t->left;                     /* rotate right */
                t->left = y->right;
                y->right = t;
                t->size = splayz_size(t->left) + splayz_size(t->right) + 1;
                t = y;
                if (t->left == nullptr) break;
            }
            r->left = t;                           /* link right */
            r = t;
            t = t->left;
            r_size += 1 + splayz_size(r->right);
        }
        else if (k > t->key) {
            if (t->right == nullptr) break;

            if (k > t->right->key) {
                y = t->right;                    /* rotate left */
                t->right = y->left;
                y->left = t;
                t->size = splayz_size(t->left) + splayz_size(t->right) + 1;
                t = y;
                if (t->right == nullptr) break;
            }
            l->right = t;                          /* link left */
            l = t;
            t = t->right;
            l_size += 1 + splayz_size(l->left);
        }
        else {
            break;
        }
    }

    // Now l_size and r_size are the sizes of the left and right trees we just
    // built.
    l_size += splayz_size(t->left);
    r_size += splayz_size(t->right);
    t->size = l_size + r_size + 1;

    l->right = r->left = nullptr;

    /* The following two loops correct the size fields of the right path  */
    /* from the left child of the root and the right path from the left   */
    /* child of the root.                                                 */
    for (y = N.right; y != nullptr; y = y->right) {
        y->size = l_size;
        l_size -= 1 + splayz_size(y->left);
    }
    for (y = N.left; y != nullptr; y = y->left) {
        y->size = r_size;
        r_size -= 1 + splayz_size(y->right);
    }

    /* assemble */
    l->right = t->left;
    r->left = t->right;
    t->left = N.right;
    t->right = N.left;

    return t;
}

//! Insert key i into the tree t, if it is not already there.  Return a pointer
//! to the resulting tree.
template <typename Key, typename Tree>
inline Tree * splayz_insert(const Key& k, Tree* t) {

    Tree* new_node;

    if (t != nullptr) {
        t = splayz(k, t);
        if (k == t->key) {
            return t; /* it's already there */
        }
    }
    new_node = new Tree();

    if (t == nullptr) {
        new_node->left = new_node->right = nullptr;
    }
    else if (k < t->key) {
        new_node->left = t->left;
        new_node->right = t;
        t->left = nullptr;
        t->size = 1 + splayz_size(t->right);
    }
    else {
        new_node->right = t->right;
        new_node->left = t;
        t->right = nullptr;
        t->size = 1 + splayz_size(t->left);
    }
    new_node->key = k;
    new_node->size =
        1 + splayz_size(new_node->left) + splayz_size(new_node->right);
    return new_node;
}

//! Erases i from the tree if it's there.  Return a pointer to the resulting
//! tree.
template <typename Key, typename Tree>
inline Tree * splayz_erase(const Key& k, Tree* t) {
    Tree* x;
    size_t tsize;

    if (t == nullptr) return nullptr;
    tsize = t->size;
    t = splayz(k, t);
    if (k == t->key) {
        /* found it */
        if (t->left == nullptr) {
            x = t->right;
        }
        else {
            x = splayz(k, t->left);
            x->right = t->right;
        }
        delete t;
        if (x != nullptr) {
            x->size = tsize - 1;
        }
        return x;
    }
    else {
        /* It wasn't there */
        return t;
    }
}

//! Returns a pointer to the node in the tree with the given rank.  Returns
//! nullptr if there is no such node.  Does not change the tree.  To guarantee
//! logarithmic behavior, the node found here should be splayed to the root.
template <typename Tree>
inline Tree * splayz_rank(size_t r, Tree* t) {
    size_t lsize;
    if (r >= splayz_size(t)) return nullptr;
    for ( ; ; ) {
        lsize = splayz_size(t->left);
        if (r < lsize) {
            t = t->left;
        }
        else if (r > lsize) {
            r = r - lsize - 1;
            t = t->right;
        }
        else {
            return t;
        }
    }
}

//! traverse the tree in preorder
template <typename Tree, typename Functor>
inline void splayz_traverse_preorder(const Functor& f, const Tree* t) {
    if (t == nullptr) return;
    splayz_traverse_preorder(f, t->left);
    f(*t);
    splayz_traverse_preorder(f, t->right);
}

// print the tree
template <typename Tree>
inline void splayz_print(Tree* t, size_t d = 0) {
    if (t == nullptr) return;
    splayz_print(t->right, d + 1);
    for (size_t i = 0; i < d; i++) std::cout << "  ";
    std::cout << t->key << "(" << t->size << ")" << std::endl;
    splayz_print(t->left, d + 1);
}

/******************************************************************************/
// Splay Tree with tree sizes

template <typename Key>
class SplayzTree
{
public:
    struct Node {
        Node   * left = nullptr, * right = nullptr;
        /* maintained to be the number of nodes rooted here */
        size_t size;
        Key    key;
    };

    Node* root_ = nullptr;

    void insert(const Key& k) {
        root_ = splayz_insert(k, root_);
    }

    void erase(const Key& k) {
        root_ = splayz_erase(k, root_);
    }

    const Node * rank(size_t i) const {
        return splayz_rank(i, root_);
    }

    Node * find(const Key& k) {
        return (root_ = splayz(k, root_));
    }

    template <typename Functor>
    void traverse_preorder(const Functor& f) const {
        splayz_traverse_preorder([&f](const Node& n) { f(n.key); }, root_);
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SPLAY_TREE_HEADER

/******************************************************************************/
