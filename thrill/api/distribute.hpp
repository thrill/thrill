/*******************************************************************************
 * thrill/api/distribute.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DISTRIBUTE_HEADER
#define THRILL_API_DISTRIBUTE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/logger.hpp>

#include <vector>

namespace thrill {
namespace api {

//! \ingroup api_layer
//! \{

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class DistributeNode final : public SourceNode<ValueType>
{
public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    DistributeNode(Context& ctx,
                   const std::vector<ValueType>& in_vector,
                   size_t source_id)
        : Super(ctx, "Distribute"),
          in_vector_(in_vector),
          source_id_(source_id)
    { }

    DistributeNode(Context& ctx,
                   std::vector<ValueType>&& in_vector,
                   size_t source_id)
        : Super(ctx, "Distribute"),
          in_vector_(std::move(in_vector)),
          source_id_(source_id)
    { }

    //! Executes the scatter operation: source sends out its data.
    void Execute() final {

        std::vector<data::CatStream::Writer> emitters = stream_->GetWriters();

        if (context_.my_rank() == source_id_)
        {
            size_t in_size = in_vector_.size();

            for (size_t w = 0; w < emitters.size(); ++w) {

                common::Range local =
                    common::CalculateLocalRange(in_size, emitters.size(), w);

                for (size_t i = local.begin; i < local.end; ++i) {
                    emitters[w].Put(in_vector_[i]);
                }
            }
        }
    }

    void PushData(bool consume) final {
        data::CatStream::CatReader readers = stream_->GetCatReader(consume);

        while (readers.HasNext()) {
            this->PushItem(readers.Next<ValueType>());
        }
    }

    void Dispose() final {
        std::vector<ValueType>().swap(in_vector_);
        stream_.reset();
    }

private:
    //! Vector pointer to read elements from.
    std::vector<ValueType> in_vector_;
    //! source worker id, which sends vector
    size_t source_id_;

    data::CatStreamPtr stream_ { context_.GetNewCatStream(this) };
};

/*!
 * Distribute is a Source DOp, which scatters the vector data from the
 * source_id to all workers, partitioning equally, and returning the data in a
 * DIA.
 *
 * \ingroup dia_sources
 */
template <typename ValueType>
auto Distribute(
    Context& ctx,
    const std::vector<ValueType>& in_vector, size_t source_id = 0) {

    using DistributeNode = api::DistributeNode<ValueType>;

    auto node = tlx::make_counting<DistributeNode>(
        ctx, in_vector, source_id);

    return DIA<ValueType>(node);
}

/*!
 * Distribute is a Source DOp, which scatters the vector data from the
 * source_id to all workers, partitioning equally, and returning the data in a
 * DIA.
 *
 * \ingroup dia_sources
 */
template <typename ValueType>
auto Distribute(
    Context& ctx,
    std::vector<ValueType>&& in_vector, size_t source_id = 0) {

    using DistributeNode = api::DistributeNode<ValueType>;

    auto node = tlx::make_counting<DistributeNode>(
        ctx, std::move(in_vector), source_id);

    return DIA<ValueType>(node);
}

} // namespace api

//! imported from api namespace
using api::Distribute;

} // namespace thrill

#endif // !THRILL_API_DISTRIBUTE_HEADER

/******************************************************************************/
