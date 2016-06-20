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
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/file.hpp>

// TODO: Move this outside of reduce:
#include <thrill/core/reduce_functional.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

/*!
 * Performs an inner join between two DIAs. The key from each DIA element is
 * hereby extracted with a key extractor function. All pairs of elements with
 * equal keys from both  DIAs are then joined with the join function.
 *
 * \tparam KeyExtractor1 Type of the key_extractor1 function. This is a
 * function ValueType to the key type.
 *
 * \tparam KeyExtractor2 Type of the key_extractor2 function. This is a
 * function from SecondDIA::ValueType to the key type.
 *
 * \tparam JoinFunction Type of the join_function. This is a function
 * from ValueType and SecondDIA::ValueType to the type of the output DIA.
 *
 * \param SecondDIA Other DIA joined with this DIA.
 *
 * \param key_extractor1 Key extractor for first DIA
 *
 * \param key_extractor2 Key extractor for second DIA
 *
 * \param join_function Join function applied to all equal key pairs
 *
 * \ingroup dia_dops
 */
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
                { parent1.id(), parent2.id() },
                { parent1.node(), parent2.node() }),
          key_extractor1_(key_extractor1),
          key_extractor2_(key_extractor2),
          join_function_(join_function),
          hash_stream1_(parent1.ctx().GetNewMixStream(this)),
          hash_writers1_(hash_stream1_->GetWriters()),
          hash_stream2_(parent2.ctx().GetNewMixStream(this)),
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

    void Execute() final {
        for (size_t i = 0; i < hash_writers1_.size(); ++i) {
            hash_writers1_[i].Close();
            hash_writers2_[i].Close();
        }
        MainOp();
    }

    void PushData(bool consume) final {

        auto compare_function_1 = [this](InputTypeFirst in1,
                                         InputTypeFirst in2) {
                                      return key_extractor1_(in1)
                                             < key_extractor1_(in2);
                                  };

        auto compare_function_2 = [this](InputTypeSecond in1,
                                         InputTypeSecond in2) {
                                      return key_extractor2_(in1) <
                                             key_extractor2_(in2);
                                  };

        // no possible join results when at least one data set is empty
        if (!files1_.size() || !files2_.size()) {
            return;
        }

        //! Merge files when there are too many for the merge tree.
        MergeFiles<InputTypeFirst>(files1_, compare_function_1);
        MergeFiles<InputTypeSecond>(files2_, compare_function_2);

        size_t merge_degree1, prefetch1, merge_degree2, prefetch2;
        std::tie(merge_degree1, prefetch1) = MaxMergeDegreePrefetch(files1_);
        std::tie(merge_degree2, prefetch2) = MaxMergeDegreePrefetch(files2_);

        // construct output merger of remaining Files
        std::vector<data::File::Reader> seq1;
        seq1.reserve(files1_.size());
        for (size_t t = 0; t < files1_.size(); ++t)
            seq1.emplace_back(files1_[t].GetReader(consume, 0));
        StartPrefetch(seq1, prefetch1);

        auto puller1 = core::make_multiway_merge_tree<InputTypeFirst>(
            seq1.begin(), seq1.end(), compare_function_1);
        InputTypeFirst ele1;
        bool puller1_done = false;

        if (puller1.HasNext())
            ele1 = puller1.Next();
        else
            puller1_done = true;

        std::vector<data::File::Reader> seq2;
        seq2.reserve(files2_.size());
        for (size_t t = 0; t < files2_.size(); ++t)
            seq2.emplace_back(files2_[t].GetReader(consume, 0));
        StartPrefetch(seq2, prefetch2);

        auto puller2 = core::make_multiway_merge_tree<InputTypeSecond>(
            seq2.begin(), seq2.end(), compare_function_2);
        InputTypeSecond ele2;
        bool puller2_done = false;
        if (puller2.HasNext())
            ele2 = puller2.Next();
        else
            puller2_done = true;

        //! cache for elements with equal keys, cartesian product of both caches
        //! are joined with the join_function
        std::vector<InputTypeFirst> equal_keys1;
        std::vector<InputTypeSecond> equal_keys2;

        while (!puller1_done && !puller2_done) {
            //! find elements with equal key
            if (key_extractor1_(ele1) < key_extractor2_(ele2)) {
                if (puller1.HasNext()) {
                    ele1 = puller1.Next();
                }
                else {
                    puller1_done = true;
                    break;
                }
            }
            else if (key_extractor2_(ele2) < key_extractor1_(ele1)) {
                if (puller2.HasNext()) {
                    ele2 = puller2.Next();
                }
                else {
                    puller2_done = true;
                    break;
                }
            }
            else {
                bool external1 = false;
                bool external2 = false;
                equal_keys1.clear();
                equal_keys2.clear();
                std::tie(puller1_done, external1) =
                    AddEqualKeysToVec(equal_keys1, puller1, ele1,
                                      key_extractor1_, join_file1_);

                std::tie(puller2_done, external2) =
                    AddEqualKeysToVec(equal_keys2, puller2, ele2,
                                      key_extractor2_, join_file2_);

                JoinAllElements(equal_keys1, external1, equal_keys2, external2);
            }
        }
    }

    void Dispose() final {
        files1_.clear();
        files2_.clear();
    }

	
private:

    //! files for sorted datasets
    std::deque<data::File> files1_;
    std::deque<data::File> files2_;

    //! user-defined functions
    KeyExtractor1 key_extractor1_;
    KeyExtractor2 key_extractor2_;
    JoinFunction join_function_;

    //! data streams for inter-worker communication of DIA elements
    data::MixStreamPtr hash_stream1_;
    std::vector<data::Stream::Writer> hash_writers1_;
    data::MixStreamPtr hash_stream2_;
    std::vector<data::Stream::Writer> hash_writers2_;

    //! Receive elements from other workers, create pre-sorted files
    void MainOp() {
        data::MixStream::MixReader reader1_ =
            hash_stream1_->GetMixReader(/* consume */ true);

        data::MixStream::MixReader reader2_ =
            hash_stream2_->GetMixReader(/* consume */ true);

        size_t capacity = DIABase::mem_limit_ / sizeof(InputTypeFirst) / 2;

        RecieveItems<InputTypeFirst>(capacity, reader1_, files1_, key_extractor1_);

        capacity = DIABase::mem_limit_ / sizeof(InputTypeSecond) / 2;

        RecieveItems<InputTypeSecond>(capacity, reader2_, files2_, key_extractor2_);
    }

    void PreOp1(const InputTypeFirst& input) {
        hash_writers1_[
            core::ReduceByHash < Key > ()(key_extractor1_(input),
                                          context_.num_workers(),
                                          0, 0).partition_id].Put(input);
    }

    void PreOp2(const InputTypeSecond& input) {
        hash_writers2_[
            core::ReduceByHash < Key > ()(key_extractor2_(input),
                                          context_.num_workers(),
                                          0, 0).partition_id].Put(input);
    }

	template <typename ItemType>
	size_t JoinCapacity() {
		return DIABase::mem_limit_ / sizeof(ItemType) / 4;
	}

	 /*!
     * Adds all elements from merge tree to a data::File, potentially to external memory,
	 * afterwards sets the first_element pointer to the first element with a different key.
     *
     * \param puller Input merge tree
     *
     * \param first_element First element with target key
     *
     * \param key_extractor Key extractor function
	 *
	 * \param writer File writer
     *
	 * \param Key target key
	 *
     * \return Pair of bools, first bool indicates whether the merge tree is
     * emptied, second bool indicates whether external memory was needed (always true, when
	 * this method was called).
     */
    template <typename ItemType, typename KeyExtractor, typename MergeTree>
    std::pair<bool, bool> AddEqualKeysToFile(MergeTree& puller,
											 ItemType& first_element,
											 const KeyExtractor& key_extractor,
											 data::File::Writer& writer,
											 Key key) {
		ItemType next_element; 
		if (puller.HasNext()) {
            next_element = puller.Next();
        }
        else {
            return std::make_pair(true, true);
        }
		
        while (key_extractor(next_element) == key) {
			writer.Put(next_element);
			if (puller.HasNext()) {
                next_element = puller.Next();
            }
            else {
                return std::make_pair(true, true);
            }
		}
		
        first_element = next_element;
        return std::make_pair(false, true);
	}

    /*!
     * Adds all elements from merge tree to a vector, afterwards sets the first_element
     * pointer to the first element with a different key.
     *
     * \param vec target vector
     *
     * \param puller Input merge tree
     *
     * \param first_element First element with target key
     *
     * \param key_extractor Key extractor function
	 *
	 * \param file_ptr Pointer to a data::File
     *
     * \return Pair of bools, first bool indicates whether the merge tree is
     * emptied, second bool indicates whether external memory was needed.
     */
    template <typename ItemType, typename KeyExtractor, typename MergeTree>
    std::pair<bool, bool> AddEqualKeysToVec(std::vector<ItemType>& vec,
                                            MergeTree& puller,
                                            ItemType& first_element,
                                            const KeyExtractor& key_extractor,
											data::FilePtr& file_ptr) {

        vec.push_back(first_element);
        ItemType next_element;
        Key key = key_extractor(first_element);
		
		
        size_t capacity = JoinCapacity<ItemType>();

        if (puller.HasNext()) {
            next_element = puller.Next();
        }
        else {
            return std::make_pair(true, false);
        }
		
        while (key_extractor(next_element) == key) {
			
            if (!mem::memory_exceeded && vec.size() < capacity) {
				vec.push_back(next_element);
			} else {
				file_ptr = context_.GetFilePtr(this);
				data::File::Writer writer = file_ptr->GetWriter();
				for (const ItemType& item : vec) {
					writer.Put(item);
				}
				writer.Put(next_element);
				//! vec is very large when this happens, swap with empty vector to free the mem
				std::vector<ItemType>().swap(vec);

				return AddEqualKeysToFile(puller, first_element, key_extractor, writer, key);
			}
            if (puller.HasNext()) {
                next_element = puller.Next();
            }
            else {
                return std::make_pair(true, false);
            }
        }

        first_element = next_element;
        return std::make_pair(false, false);
    }

    DIAMemUse ExecuteMemUse() final {
        return DIAMemUse::Max();
    }

    DIAMemUse PushDataMemUse() final {
        return DIAMemUse::Max();
    }

    /*!
     * Recieve all elements from a stream and write them to files sorted by key.
     */
    template <typename ItemType, typename KeyExtractor>
    void RecieveItems(size_t capacity, data::MixStream::MixReader& reader,
                      std::deque<data::File>& files, const KeyExtractor& key_extractor) {

        std::vector<ItemType> vec;
        vec.reserve(capacity);

        while (reader.HasNext()) {
            if (!mem::memory_exceeded && vec.size() < capacity) {
                vec.push_back(reader.template Next<ItemType>());
            }
            else {
                SortAndWriteToFile(vec, files, key_extractor);
            }
        }

        if (vec.size())
			SortAndWriteToFile(vec, files, key_extractor);
    }

    //! calculate maximum merging degree from available memory and the number of
    //! files. additionally calculate the prefetch size of each File.
    std::pair<size_t, size_t> MaxMergeDegreePrefetch(std::deque<data::File>& files) {
        // 1/4 of avail_blocks in api::Sort, as Join has two mergers and two vectors of 
		// data to join
        size_t avail_blocks = DIABase::mem_limit_ / data::default_block_size / 4;
        if (files.size() >= avail_blocks) {
            // more files than blocks available -> partial merge of avail_blocks
            // Files with prefetch = 0, which is one read Block per File.
            return std::make_pair(avail_blocks, 0u);
        }
        else {
            // less files than available Blocks -> split blocks equally among
            // Files.
            return std::make_pair(
                files.size(),
                std::min<size_t>(16u, (avail_blocks / files.size()) - 1));
        }
    }

    /*!
     * Merge files when there are too many for the merge tree to handle
     */
    template <typename ItemType, typename CompareFunction>
    void MergeFiles(std::deque<data::File>& files, CompareFunction compare_function) {

        size_t merge_degree, prefetch;

        // merge batches of files if necessary
        while (files.size() > MaxMergeDegreePrefetch(files).first)
        {
            std::tie(merge_degree, prefetch) = MaxMergeDegreePrefetch(files);

            sLOG1 << "Partial multi-way-merge of"
                  << merge_degree << "files with prefetch" << prefetch;

            // create merger for first merge_degree_ Files
            std::vector<data::File::ConsumeReader> seq;
            seq.reserve(merge_degree);

            for (size_t t = 0; t < merge_degree; ++t)
                seq.emplace_back(files[t].GetConsumeReader(0));

            StartPrefetch(seq, prefetch);

            auto puller = core::make_multiway_merge_tree<ItemType>(
                seq.begin(), seq.end(), compare_function);

            // create new File for merged items
            files.emplace_back(context_.GetFile(this));
            auto writer = files.back().GetWriter();

            while (puller.HasNext()) {
                writer.Put(puller.Next());
            }
            writer.Close();

            // this clear is important to release references to the files.
            seq.clear();

            // remove merged files
            files.erase(files.begin(), files.begin() + merge_degree);
        }
    }

	

	data::FilePtr join_file1_;
	data::FilePtr join_file2_;	

    /*!
     * Joins all elements in cartesian product of both vectors. Uses files when
     * one of the data sets is too large to fit in memory. (indicated by
     * 'external' bools)
     */
    void JoinAllElements(const std::vector<InputTypeFirst>& vec1, bool external1,
                         const std::vector<InputTypeSecond>& vec2, bool external2) {

        if (!external1 && !external2) {
            for (auto const& join1 : vec1) {
                for (auto const& join2 : vec2) {
                    assert(key_extractor1_(join1) == key_extractor2_(join2));
                    this->PushItem(join_function_(join1, join2));
                }
            }
            return;
        }
        if (external1 && !external2) {
            LOG1 << "Thrill: Warning: Too many equal keys for main memory "
                 << "in first DIA";

			data::File::Reader reader = join_file1_->GetReader(/*consume*/ true);

			while (reader.HasNext()) {
				InputTypeFirst join1 = reader.template Next<InputTypeFirst>();
				for (auto const& join2 : vec2) {
                    assert(key_extractor1_(join1) == key_extractor2_(join2));
                    this->PushItem(join_function_(join1, join2));
				}
			}

            return;
        }

        if (!external1 && external2) {
            LOG1 << "Thrill: Warning: Too many equal keys for main memory "
                 << "in second DIA";

			data::File::Reader reader = join_file2_->GetReader(/*consume*/ true);

			while (reader.HasNext()) {
				InputTypeSecond join2 = reader.template Next<InputTypeSecond>();
				for (auto const& join1 : vec1) {
                    assert(key_extractor1_(join1) == key_extractor2_(join2));
                    this->PushItem(join_function_(join1, join2));
				}
			}

            return;
        }

        if (external1 && external2) {
            LOG1 << "Thrill: Warning: Too many equal keys for main memory "
                 << "in both DIAs. This is very slow.";

			size_t capacity = JoinCapacity<InputTypeFirst>();

			std::vector<InputTypeFirst> temp_vec;
			temp_vec.reserve(capacity);

			//! file 2 needs to be read multiple times 
			data::File::Reader reader1 = join_file1_->GetReader(/*consume*/ true);

			while (reader1.HasNext()) {

				for (size_t i = 0; i < capacity && reader1.HasNext() && !mem::memory_exceeded; ++i) {
					temp_vec.push_back(reader1.template Next<InputTypeFirst>());
				}
				
				data::File::Reader reader2 = join_file2_->GetReader(/*consume*/ false);
				
				size_t test = 0;
				while (reader2.HasNext()) {
					++test;
					InputTypeSecond join2 = reader2.template Next<InputTypeSecond>();
					for (auto const& join1 : temp_vec) {
						assert(key_extractor1_(join1) == key_extractor2_(join2));
						this->PushItem(join_function_(join1, join2));
					}
				}
				temp_vec.clear();
			}

			//! non-consuming reader, need to clear now
			join_file2_->Clear();

			return;
        }
    }

    /*!
     * Sorts all elements in a vector and writes them to a file.
     */
    template <typename ItemType, typename KeyExtractor>
    void SortAndWriteToFile(
        std::vector<ItemType>& vec, std::deque<data::File>& files,
		const KeyExtractor& key_extractor) {
        // advise block pool to write out data if necessary
        context_.block_pool().AdviseFree(vec.size() * sizeof(ValueType));

        std::sort(vec.begin(), vec.end(), [&key_extractor](ItemType i1, ItemType i2) {
				return key_extractor(i1) < key_extractor(i2);
			});

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
auto DIA<ValueType, Stack>::InnerJoinWith(const SecondDIA &second_dia,
                                          const KeyExtractor1 &key_extractor1,
                                          const KeyExtractor2 &key_extractor2,
                                          const JoinFunction &join_function) const {

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
