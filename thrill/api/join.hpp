/*******************************************************************************
 * thrill/api/join.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_JOIN_HEADER
#define THRILL_API_JOIN_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

//TODO: Move this outside of reduce:
#include <thrill/core/reduce_functional.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {


template <typename ValueType, typename FirstDIA, typename SecondDIA,
		  typename KeyExtractor1, typename KeyExtractor2,
		  typename JoinFunction>
class JoinNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;


	using InputTypeFirst = typename FirstDIA::ValueType;
	using InputTypeSecond = typename SecondDIA::ValueType;

	using Key = typename common::FunctionTraits<KeyExtractor1>::result_type;


public:
    /*!
     * Constructor for a JoinNode.
     */
	JoinNode(const FirstDIA& parent1, const SecondDIA& parent2,
			 const KeyExtractor1& key_extractor1,
			 const KeyExtractor2& key_extractor2,
			 const JoinFunction& join_function)
		: Super(parent1.ctx(), "Join",
			{parent1.id(), parent2.id()},
			{parent1.node(), parent2.node()}),
		key_extractor1_(key_extractor1),
		key_extractor2_(key_extractor2),
		join_function_(join_function),
		hash_stream1_(parent1.ctx().GetNewCatStream(this)),
		hash_writers1_(hash_stream1_->GetWriters()),
		hash_stream2_(parent2.ctx().GetNewCatStream(this)),
		hash_writers2_(hash_stream2_->GetWriters())
	{

		auto pre_op_fn1 = [this](const InputTypeFirst& input) {
			PreOp1(input);
		};

		auto pre_op_fn2 = [this](const InputTypeSecond& input) {
			PreOp2(input);
		};

		auto lop_chain1 = parent1.stack().push(pre_op_fn1).fold();
		auto lop_chain2 = parent2.stack().push(pre_op_fn2).fold();
		parent1.node()->AddChild(this, lop_chain1);
		parent2.node()->AddChild(this, lop_chain2);
	}


	void PreOp1(const InputTypeFirst& input) {
		hash_writers1_[
			core::ReduceByHash<Key>()(key_extractor1_(input),
									  context_.num_workers(), 0 , 0).partition_id].Put(input);
	}

	void PreOp2(const InputTypeSecond& input) {
		hash_writers2_[
			core::ReduceByHash<Key>()(key_extractor2_(input),
									  context_.num_workers(), 0, 0).partition_id].Put(input);
	}

	void Execute() final {
		for (size_t i = 0; i < hash_writers1_.size(); ++i) {
			hash_writers1_[i].Close();
			hash_writers2_[i].Close();
		}
		MainOp();
	}

	void PushData(bool/* consume */) final {

	}

	void Dispose() final {

	}

	private:
	std::deque<data::File> files1_;
	
	std::deque<data::File> files2_;

	KeyExtractor1 key_extractor1_;
	
	KeyExtractor2 key_extractor2_;
	
	JoinFunction join_function_;

	data::CatStreamPtr hash_stream1_;
	std::vector<data::Stream::Writer> hash_writers1_;
	
	data::CatStreamPtr hash_stream2_;
	std::vector<data::Stream::Writer> hash_writers2_;

	//! Receive elements from other workers.
	void MainOp() {
		data::CatStream::CatReader reader1_ = 
			hash_stream1_->GetCatReader(/* consume */true);
		
		data::CatStream::CatReader reader2_ =
			hash_stream2_->GetCatReader(/* consume */true);

		size_t capacity = DIABase::mem_limit_ / sizeof(InputTypeFirst) / 2;

		RecieveItems<InputTypeFirst>(capacity, reader1_, files1_);

		capacity = DIABase::mem_limit_ / sizeof(InputTypeSecond) / 2;
		
		RecieveItems<InputTypeSecond>(capacity, reader2_, files2_);

	}

	DIAMemUse ExecuteMemUse() final {
        return DIAMemUse::Max();
    }

	template <typename ItemType>
	void RecieveItems(size_t capacity, data::CatStream::CatReader& reader,
					  std::deque<data::File>& files) {

		std::vector<ItemType> vec;
		vec.reserve(capacity);
		
		while (reader.HasNext()) {
			if (!mem::memory_exceeded && vec.size() < capacity) {
				vec.push_back(reader.template Next<InputTypeFirst>());
			} else {
				SortAndWriteToFile(vec, files);
			}
		}

		if (vec.size())
			SortAndWriteToFile(vec, files);
	}

	template <typename ItemType>
	void SortAndWriteToFile(
        std::vector<ItemType>& vec, std::deque<data::File>& files) {
        // advice block pool to write out data if necessary
        context_.block_pool().AdviseFree(vec.size() * sizeof(ValueType));

		std::sort(vec.begin(), vec.end());

        files.emplace_back(context_.GetFile(this));
        auto writer = files.back().GetWriter();
        for (const ItemType& elem : vec) {
            writer.Put(elem);
        }
        writer.Close();

        vec.clear();
    }
 };

template <typename ValueType, typename Stack>
template <typename KeyExtractor1,
		  typename KeyExtractor2,
		  typename JoinFunction,
          typename SecondDIA>
auto DIA<ValueType, Stack>::InnerJoinWith(const SecondDIA& second_dia,
										  const KeyExtractor1& key_extractor1,
										  const KeyExtractor2& key_extractor2,
										  const JoinFunction& join_function) const {

		assert(IsValid());
		assert(second_dia.IsValid());

    static_assert(
		std::is_convertible<
		ValueType,
		typename common::FunctionTraits<KeyExtractor1>::template arg<0>
		>::value,
		"Key Extractor 1 has the wrong input type");

    static_assert(
		std::is_convertible<
	    typename SecondDIA::ValueType,
		typename common::FunctionTraits<KeyExtractor2>::template arg<0>
		>::value,
		"Key Extractor 2 has the wrong input type");

	static_assert(
		std::is_convertible<
		typename common::FunctionTraits<KeyExtractor1>::result_type,
		typename common::FunctionTraits<KeyExtractor2>::result_type
		>::value,
		"Keys have different types");

	static_assert(
		std::is_convertible<
		ValueType,
		typename common::FunctionTraits<JoinFunction>::template arg<0>
		>::value,
		"Join Function has wrong input type in argument 0");

	static_assert(
		std::is_convertible<
		typename SecondDIA::ValueType,
		typename common::FunctionTraits<JoinFunction>::template arg<1>
		>::value,
		"Join Function has wrong input type in argument 1");
   
	using JoinResult
              = typename common::FunctionTraits<JoinFunction>::result_type;

    using JoinNode = api::JoinNode<JoinResult, DIA, SecondDIA,
								   KeyExtractor1, KeyExtractor2,
								   JoinFunction>;

    auto node = common::MakeCounting<JoinNode>(
		*this, second_dia, key_extractor1, key_extractor2, join_function);

	return DIA<JoinResult>(node);
}

//! \}

} // namespace api

} // namespace thrill

#endif // !THRILL_API_JOIN_HEADER

/******************************************************************************/
