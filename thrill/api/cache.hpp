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

/*!
 * A DOpNode which caches all items in an external file.
 *
 * \ingroup api_layer
 */
template <typename ValueType>
class CacheNode final : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     */
    template <typename ParentDIA>
    explicit CacheNode(const ParentDIA& parent)
        : Super(parent.ctx(), "Cache", { parent.id() }, { parent.node() }),
          parent_stack_empty_(ParentDIA::stack_empty)
    {
        auto save_fn = [this](const ValueType& input) {
                           writer_.Put(input);
                       };
        auto lop_chain = parent.stack().push(save_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOGC(common::g_debug_push_file)
                << "Cache rejected File from parent "
                << "due to non-empty function stack.";
            return false;
        }
        assert(file_.num_items() == 0);
        file_ = file.Copy();
        return true;
    }

    void StopPreOp(size_t /* parent_index */) final {
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

    size_t NumItems() const {
        return file_.num_items();
    }

private:
    //! Local data file
    data::File file_ { context_.GetFile(this) };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ { file_.GetWriter() };
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;
};

template <typename ValueType, typename Stack>
DIA<ValueType> DIA<ValueType, Stack>::Cache() const {
    assert(IsValid());

#if !defined(_MSC_VER)
    // skip Cache if this is already a CacheNode. MSVC messes this up.
    if (stack_empty &&
        dynamic_cast<CacheNode<ValueType>*>(node_.get()) != nullptr) {
        // return Collapse instead, automatically eliminates CollapseNode since
        // the stack is empty.
        return Collapse();
    }
#endif
    return DIA<ValueType>(
        tlx::make_counting<api::CacheNode<ValueType> >(*this));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_CACHE_HEADER

/******************************************************************************/
