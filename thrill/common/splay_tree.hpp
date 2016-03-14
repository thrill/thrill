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
// splay -- Free Splay Tree methods without tree sizes

//! print the tree
template <typename Tree>
inline void splay_print(Tree* t, size_t d = 0) {
    if (t == nullptr) return;
    splay_print(t->right, d + 1);
    for (size_t i = 0; i < d; i++) std::cout << "  ";
    std::cout << *t << std::endl;
    splay_print(t->left, d + 1);
}

//! check the tree order, recursively calculate min and max elements
template <typename Tree, typename Compare>
inline void splay_check(
    const Tree* t,
    const Tree*& out_tmin, const Tree*& out_tmax,
    const Compare& cmp) {

    if (t == nullptr) return;

    const Tree* tmin = nullptr, * tmax = nullptr;
    splay_check(t->left, out_tmin, tmax, cmp);
    splay_check(t->right, tmin, out_tmax, cmp);

    if (tmax) assert(cmp(tmax, t));
    if (tmin) assert(cmp(t, tmin));
}

//! check the tree order
template <typename Tree, typename Compare>
inline void splay_check(const Tree* t, const Compare& cmp) {
    if (t == nullptr) return;
    const Tree* tmin = nullptr, * tmax = nullptr;
    splay_check(t, tmin, tmax, cmp);
}

//! Splay using the key i (which may or may not be in the tree.)  The starting
//! root is t, and the tree used is defined by rat size fields are maintained.
template <typename Key, typename Tree, typename Compare>
inline Tree * splay(const Key& k, Tree* t, const Compare& cmp) {
    Tree* N_left = nullptr, * N_right = nullptr;
    Tree* l = nullptr, * r = nullptr;

    if (t == nullptr) return t;

    for ( ; ; ) {
        if (cmp(k, t)) {
            if (t->left == nullptr) break;

            if (cmp(k, t->left)) {
                /* rotate right */
                Tree* y = t->left;
                t->left = y->right;
                y->right = t;
                t = y;
                if (t->left == nullptr) break;
            }
            /* link right */
            (r ? r->left : N_left) = t;
            r = t;
            t = t->left;
        }
        else if (cmp(t, k)) {
            if (t->right == nullptr) break;

            if (cmp(t->right, k)) {
                /* rotate left */
                Tree* y = t->right;
                t->right = y->left;
                y->left = t;
                t = y;
                if (t->right == nullptr) break;
            }
            /* link left */
            (l ? l->right : N_right) = t;
            l = t;
            t = t->right;
        }
        else {
            break;
        }
    }

    (l ? l->right : N_right) = (r ? r->left : N_left) = nullptr;

    /* assemble */
    (l ? l->right : N_right) = t->left;
    (r ? r->left : N_left) = t->right;
    t->left = N_right;
    t->right = N_left;

    return t;
}

//! Insert key i into the tree t, if it is not already there.  Before calling
//! this method, one *MUST* call splay() to rotate the tree to the right
//! position. Return a pointer to the resulting tree.
template <typename Tree, typename Compare>
inline Tree * splay_insert(Tree* nn, Tree* t, const Compare& cmp) {
    if (t == nullptr) {
        nn->left = nn->right = nullptr;
    }
    else if (cmp(nn, t)) {
        nn->left = t->left;
        nn->right = t;
        t->left = nullptr;
    }
    else {
        nn->right = t->right;
        nn->left = t;
        t->right = nullptr;
    }
    return nn;
}

//! Erases i from the tree if it's there.  Return a pointer to the resulting
//! tree.
template <typename Key, typename Tree, typename Compare>
inline Tree * splay_erase(const Key& k, Tree*& t, const Compare& cmp) {
    if (t == nullptr) return nullptr;
    t = splay(k, t, cmp);
    /* k == t->key ? */
    if (!cmp(k, t) && !cmp(t, k)) {
        /* found it */
        Tree* r = t;
        if (t->left == nullptr) {
            t = t->right;
        }
        else {
            Tree* x = splay(k, t->left, cmp);
            x->right = t->right;
            t = x;
        }
        return r;
    }
    else {
        /* It wasn't there */
        return nullptr;
    }
}

//! Erases i from the tree if it's there.  Return a pointer to the resulting
//! tree.
template <typename Tree, typename Compare>
inline Tree * splay_erase_top(Tree*& t, const Compare& cmp) {
    if (t == nullptr) return nullptr;
    /* found it */
    Tree* r = t;
    if (t->left == nullptr) {
        t = t->right;
    }
    else {
        Tree* x = splay(r, t->left, cmp);
        x->right = t->right;
        t = x;
    }
    return r;
}

//! traverse the tree in preorder
template <typename Tree, typename Functor>
inline void splay_traverse_preorder(const Functor& f, const Tree* t) {
    if (t == nullptr) return;
    splay_traverse_preorder(f, t->left);
    f(*t);
    splay_traverse_preorder(f, t->right);
}

/******************************************************************************/
// splayz -- Free Splay Tree methods with tree sizes

template <typename Tree>
inline size_t splayz_size(Tree* x) {
    return (x == nullptr) ? 0 : x->size;
}

//! print the tree
template <typename Tree>
inline void splayz_print(Tree* t, size_t d = 0) {
    if (t == nullptr) return;
    splayz_print(t->right, d + 1);
    for (size_t i = 0; i < d; i++) std::cout << "  ";
    std::cout << *t << std::endl;
    splayz_print(t->left, d + 1);
}

//! check the tree order, recursively calculate min and max elements
template <typename Tree, typename Compare>
inline void splayz_check(
    const Tree* t,
    const Tree*& out_tmin, const Tree*& out_tmax, size_t& out_size,
    const Compare& cmp) {

    if (t == nullptr) return;

    const Tree* tmin = nullptr, * tmax = nullptr;
    size_t left_size = 0, right_size = 0;
    splayz_check(t->left, out_tmin, tmax, left_size, cmp);
    splayz_check(t->right, tmin, out_tmax, right_size, cmp);

    if (tmax) assert(cmp(tmax, t));
    if (tmin) assert(cmp(t, tmin));
    assert(t->size == left_size + 1 + right_size);
    out_size = t->size;
}

//! check the tree order
template <typename Tree, typename Compare>
inline void splayz_check(const Tree* t, const Compare& cmp) {
    if (t == nullptr) return;
    const Tree* tmin = nullptr, * tmax = nullptr;
    size_t size = 0;
    splayz_check(t, tmin, tmax, size, cmp);
}

//! Splay using the key i (which may or may not be in the tree.)  The starting
//! root is t, and the tree used is defined by rat size fields are maintained.
template <typename Key, typename Tree, typename Compare>
inline Tree * splayz(const Key& k, Tree* t, const Compare& cmp) {
    Tree* N_left = nullptr, * N_right = nullptr;
    Tree* l = nullptr, * r = nullptr;
    size_t l_size = 0, r_size = 0;

    if (t == nullptr) return t;

    for ( ; ; ) {
        if (cmp(k, t)) {
            if (t->left == nullptr) break;

            if (cmp(k, t->left)) {
                /* rotate right */
                Tree* y = t->left;
                t->left = y->right;
                y->right = t;
                t->size = splayz_size(t->left) + splayz_size(t->right) + 1;
                t = y;
                if (t->left == nullptr) break;
            }
            /* link right */
            (r ? r->left : N_left) = t;
            r = t;
            t = t->left;
            r_size += 1 + splayz_size(r->right);
        }
        else if (cmp(t, k)) {
            if (t->right == nullptr) break;

            if (cmp(t->right, k)) {
                /* rotate left */
                Tree* y = t->right;
                t->right = y->left;
                y->left = t;
                t->size = splayz_size(t->left) + splayz_size(t->right) + 1;
                t = y;
                if (t->right == nullptr) break;
            }
            /* link left */
            (l ? l->right : N_right) = t;
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

    (l ? l->right : N_right) = (r ? r->left : N_left) = nullptr;

    /* The following two loops correct the size fields of the right path  */
    /* from the left child of the root and the right path from the left   */
    /* child of the root.                                                 */
    for (Tree* y = N_right; y != nullptr; y = y->right) {
        y->size = l_size;
        l_size -= 1 + splayz_size(y->left);
    }
    for (Tree* y = N_left; y != nullptr; y = y->left) {
        y->size = r_size;
        r_size -= 1 + splayz_size(y->right);
    }

    /* assemble */
    (l ? l->right : N_right) = t->left;
    (r ? r->left : N_left) = t->right;
    t->left = N_right;
    t->right = N_left;

    return t;
}

//! Insert key i into the tree t, if it is not already there.  Before calling
//! this method, one *MUST* call splayz() to rotate the tree to the right
//! position. Return a pointer to the resulting tree.
template <typename Tree, typename Compare>
inline Tree * splayz_insert(Tree* nn, Tree* t, const Compare& cmp) {
    if (t == nullptr) {
        nn->left = nn->right = nullptr;
    }
    else if (cmp(nn, t)) {
        nn->left = t->left;
        nn->right = t;
        t->left = nullptr;
        t->size = 1 + splayz_size(t->right);
    }
    else {
        nn->right = t->right;
        nn->left = t;
        t->right = nullptr;
        t->size = 1 + splayz_size(t->left);
    }
    nn->size = 1 + splayz_size(nn->left) + splayz_size(nn->right);
    return nn;
}

//! Erases i from the tree if it's there.  Return a pointer to the resulting
//! tree.
template <typename Key, typename Tree, typename Compare>
inline Tree * splayz_erase(const Key& k, Tree*& t, const Compare& cmp) {
    if (t == nullptr) return nullptr;
    size_t tsize = t->size;
    t = splayz(k, t, cmp);
    /* k == t->key ? */
    if (!cmp(k, t) && !cmp(t, k)) {
        /* found it */
        Tree* r = t;
        if (t->left == nullptr) {
            t = t->right;
        }
        else {
            Tree* x = splayz(k, t->left, cmp);
            x->right = t->right;
            t = x;
        }
        if (t != nullptr) {
            t->size = tsize - 1;
        }
        return r;
    }
    else {
        /* It wasn't there */
        return nullptr;
    }
}

//! Erases i from the tree if it's there.  Return a pointer to the resulting
//! tree.
template <typename Tree, typename Compare>
inline Tree * splayz_erase_top(Tree*& t, const Compare& cmp) {
    if (t == nullptr) return nullptr;
    size_t tsize = t->size;
    /* found it */
    Tree* r = t;
    if (t->left == nullptr) {
        t = t->right;
    }
    else {
        Tree* x = splayz(r, t->left, cmp);
        x->right = t->right;
        t = x;
    }
    if (t != nullptr) {
        t->size = tsize - 1;
    }
    return r;
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
        explicit Node(const Key& k) : key(k) { }
        Node() = default;
    };

    struct NodeCompare {
        bool operator () (const Node* n, const Key& k) const {
            return n->key < k;
        }
        bool operator () (const Key& k, const Node* n) const {
            return k < n->key;
        }
        bool operator () (const Node* a, const Node* b) const {
            return a->key < b->key;
        }
    };

    Node* root_ = nullptr;

    //! insert key into tree if it does not exist, returns true if inserted.
    bool insert(const Key& k) {
        NodeCompare cmp;
        if (root_ != nullptr) {
            root_ = splayz(k, root_, cmp);
            /* k == t->key ? */
            if (!cmp(k, root_) && !cmp(root_, k)) {
                /* it's already there */
                return false;
            }
        }
        Node* nn = new Node(k);
        root_ = splayz_insert(nn, root_, cmp);
        return true;
    }

    bool erase(const Key& k) {
        Node* out = splayz_erase(k, root_, NodeCompare());
        if (!out) return false;
        delete out;
        return true;
    }

    bool exists(const Key& k) {
        root_ = splayz(k, root_, NodeCompare());
        return root_->key == k;
    }

    const Node * rank(size_t i) const {
        return splayz_rank(i, root_);
    }

    Node * find(const Key& k) {
        return (root_ = splayz(k, root_, NodeCompare()));
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
