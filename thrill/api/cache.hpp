/*******************************************************************************
 * thrill/api/cache.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_CACHE_HEADER
#define THRILL_API_CACHE_HEADER

#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/data/file.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! Common super class for all CacheNodes, used for dynamic_cast type check in
//! Cache().
template <typename ValueType>
class CacheNodeBase : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;

    template <typename ParentDIA>
    explicit CacheNodeBase(const ParentDIA& parent)
        : Super(parent.ctx(), "Cache", { parent.id() }, { parent.node() }) { }
};

/*!
 * A DOpNode which caches all items in an external file.
 *
 * \ingroup api_layer
 */
template <typename ValueType, typename ParentDIA>
class CacheNode final : public CacheNodeBase<ValueType>
{
public:
    using Super = CacheNodeBase<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     */
    explicit CacheNode(const ParentDIA& parent)
        : Super(parent) {

        auto save_fn = [this](const ValueType& input) {
                           writer_.Put(input);
                       };
        auto lop_chain = parent.stack().push(save_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!ParentDIA::stack_empty) return false;
        assert(file_.num_items() == 0);
        file_ = file.Copy();
        return true;
    }

    void StopPreOp(size_t /* id */) final {
        // Push local elements to children
        writer_.Close();
    }

    void Execute() final { }

    void PushData(bool consume) final {
        this->PushFile(file_, consume);
    }

    void Dispose() final {
        file_.Clear();
    }

private:
    //! Local data file
    data::File file_ { context_.GetFile(this) };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ { file_.GetWriter() };
};

template <typename ValueType, typename Stack>
DIA<ValueType> DIA<ValueType, Stack>::Cache() const {
    assert(IsValid());

#if !defined(_MSC_VER)
    // skip if this is already a CacheNode. MSVC messes this up.
    if (stack_empty &&
        dynamic_cast<CacheNodeBase<ValueType>*>(node_.get()) != nullptr) {
        return *this;
    }
#endif
    return DIA<ValueType>(
        common::MakeCounting<api::CacheNode<ValueType, DIA> >(*this));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_CACHE_HEADER

/******************************************************************************/
