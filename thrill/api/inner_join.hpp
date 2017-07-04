/*******************************************************************************
 * thrill/api/inner_join.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_INNER_JOIN_HEADER
#define THRILL_API_INNER_JOIN_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/buffered_multiway_merge.hpp>
#include <thrill/core/location_detection.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <deque>
#include <functional>
#include <utility>
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
          typename JoinFunction, typename HashFunction,
          bool UseLocationDetection>
class JoinNode final : public DOpNode<ValueType>
{
private:
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using InputTypeFirst = typename FirstDIA::ValueType;
    using InputTypeSecond = typename SecondDIA::ValueType;

    //! Key type of join. must be equal to the other key extractor
    using Key = typename common::FunctionTraits<KeyExtractor1>::result_type;

    //! hash counter used by LocationDetection
    class HashCount
    {
    public:
        using HashType = size_t;
        using CounterType = uint8_t;
        using DIAIdxType = uint8_t;

        size_t hash;
        CounterType count;
        DIAIdxType dia_mask;

        static constexpr size_t counter_bits_ = 8 * sizeof(CounterType);

        HashCount operator + (const HashCount& b) const {
            assert(hash == b.hash);
            return HashCount {
                       hash,
                       common::AddTruncToType(count, b.count),
                       static_cast<DIAIdxType>(dia_mask | b.dia_mask)
            };
        }

        HashCount& operator += (const HashCount& b) {
            assert(hash == b.hash);
            count = common::AddTruncToType(count, b.count);
            dia_mask |= b.dia_mask;
            return *this;
        }

        bool operator < (const HashCount& b) const { return hash < b.hash; }

        //! method to check if this hash count should be broadcasted to all
        //! workers interested -- for InnerJoin this check if the dia_mask == 3
        //! -> hash was in both DIAs on some worker.
        bool NeedBroadcast() const {
            return dia_mask == 3;
        }

        //! Read count and dia_mask from BitReader
        template <typename BitReader>
        void ReadBits(BitReader& reader) {
            count = reader.GetBits(counter_bits_);
            dia_mask = reader.GetBits(2);
        }

        //! Write count and dia_mask to BitWriter
        template <typename BitWriter>
        void WriteBits(BitWriter& writer) const {
            writer.PutBits(count, counter_bits_);
            writer.PutBits(dia_mask, 2);
        }
    };

public:
    /*!
     * Constructor for a JoinNode.
     */
    JoinNode(const FirstDIA& parent1, const SecondDIA& parent2,
             const KeyExtractor1& key_extractor1,
             const KeyExtractor2& key_extractor2,
             const JoinFunction& join_function,
             const HashFunction& hash_function)
        : Super(parent1.ctx(), "Join",
                { parent1.id(), parent2.id() },
                { parent1.node(), parent2.node() }),
          key_extractor1_(key_extractor1),
          key_extractor2_(key_extractor2),
          join_function_(join_function),
          hash_function_(hash_function)
    {
        auto pre_op_fn1 = [this](const InputTypeFirst& input) {
                              PreOp1(input);
                          };

        auto pre_op_fn2 = [this](const InputTypeSecond& input) {
                              PreOp2(input);
                          };

        auto lop_chain1 = parent1.stack().push(pre_op_fn1).fold();
        auto lop_chain2 = parent2.stack().push(pre_op_fn2).fold();
        parent1.node()->AddChild(this, lop_chain1, 0);
        parent2.node()->AddChild(this, lop_chain2, 1);
    }

    void Execute() final {

        if (UseLocationDetection) {
            std::unordered_map<size_t, size_t> target_processors;
            size_t max_hash = location_detection_.Flush(target_processors);
            location_detection_.Dispose();

            auto file1reader = pre_file1_.GetConsumeReader();
            while (file1reader.HasNext()) {
                InputTypeFirst in1 = file1reader.template Next<InputTypeFirst>();
                auto target_processor =
                    target_processors.find(
                        hash_function_(key_extractor1_(in1)) % max_hash);
                if (target_processor != target_processors.end()) {
                    hash_writers1_[target_processor->second].Put(in1);
                }
            }

            auto file2reader = pre_file2_.GetConsumeReader();
            while (file2reader.HasNext()) {
                InputTypeSecond in2 = file2reader.template Next<InputTypeSecond>();
                auto target_processor =
                    target_processors.find(
                        hash_function_(key_extractor2_(in2)) % max_hash);
                if (target_processor != target_processors.end()) {
                    hash_writers2_[target_processor->second].Put(in2);
                }
            }
        }

        hash_writers1_.clear();
        hash_writers2_.clear();

        MainOp();
    }

    template <typename ElementType, typename CompareFunction>
    auto MakePuller(std::deque<data::File>& files,
                    std::vector<data::File::Reader>& seq,
                    CompareFunction compare_function, bool consume) {

        size_t merge_degree, prefetch;
        std::tie(merge_degree, prefetch) = MaxMergeDegreePrefetch(files);
        // construct output merger of remaining Files
        seq.reserve(files.size());
        for (size_t t = 0; t < files.size(); ++t)
            seq.emplace_back(files[t].GetReader(consume, 0));
        StartPrefetch(seq, prefetch);

        return core::make_buffered_multiway_merge_tree<ElementType>(
            seq.begin(), seq.end(), compare_function);
    }

    void PushData(bool consume) final {

        auto compare_function_1 =
            [this](const InputTypeFirst& in1, const InputTypeFirst& in2) {
                return key_extractor1_(in1) < key_extractor1_(in2);
            };

        auto compare_function_2 =
            [this](const InputTypeSecond& in1, const InputTypeSecond& in2) {
                return key_extractor2_(in1) < key_extractor2_(in2);
            };

        // no possible join results when at least one data set is empty
        if (!files1_.size() || !files2_.size())
            return;

        //! Merge files when there are too many for the merge tree.
        MergeFiles<InputTypeFirst>(files1_, compare_function_1);
        MergeFiles<InputTypeSecond>(files2_, compare_function_2);

        std::vector<data::File::Reader> seq1;
        std::vector<data::File::Reader> seq2;

        // construct output merger of remaining Files
        auto puller1 = MakePuller<InputTypeFirst>(
            files1_, seq1, compare_function_1, consume);
        auto puller2 = MakePuller<InputTypeSecond>(
            files2_, seq2, compare_function_2, consume);

        bool puller1_done = false;
        if (!puller1.HasNext())
            puller1_done = true;

        bool puller2_done = false;
        if (!puller2.HasNext())
            puller2_done = true;

        //! cache for elements with equal keys, cartesian product of both caches
        //! are joined with the join_function
        std::vector<InputTypeFirst> equal_keys1;
        std::vector<InputTypeSecond> equal_keys2;

        while (!puller1_done && !puller2_done) {
            //! find elements with equal key
            if (key_extractor1_(puller1.Top()) <
                key_extractor2_(puller2.Top())) {
                if (!puller1.Update()) {
                    puller1_done = true;
                    break;
                }
            }
            else if (key_extractor2_(puller2.Top()) <
                     key_extractor1_(puller1.Top())) {
                if (!puller2.Update()) {
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
                    AddEqualKeysToVec(equal_keys1, puller1,
                                      key_extractor1_, join_file1_);

                std::tie(puller2_done, external2) =
                    AddEqualKeysToVec(equal_keys2, puller2,
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
    HashFunction hash_function_;

    //! data streams for inter-worker communication of DIA elements
    data::MixStreamPtr hash_stream1_ { context_.GetNewMixStream(this) };
    std::vector<data::Stream::Writer> hash_writers1_ { hash_stream1_->GetWriters() };
    data::MixStreamPtr hash_stream2_ { context_.GetNewMixStream(this) };
    std::vector<data::Stream::Writer> hash_writers2_ { hash_stream2_->GetWriters() };

    //! location detection and associated files
    data::File pre_file1_ { context_.GetFile(this) };
    data::File::Writer pre_writer1_;
    data::File pre_file2_ { context_.GetFile(this) };
    data::File::Writer pre_writer2_;

    core::LocationDetection<HashCount> location_detection_ { context_, Super::id() };
    bool location_detection_initialized_ = false;

    void PreOp1(const InputTypeFirst& input) {
        size_t hash = hash_function_(key_extractor1_(input));
        if (UseLocationDetection) {
            pre_writer1_.Put(input);
            location_detection_.Insert(HashCount { hash, 1, /* dia_mask */ 1 });
        }
        else {
            hash_writers1_[hash % context_.num_workers()].Put(input);
        }
    }

    void PreOp2(const InputTypeSecond& input) {
        size_t hash = hash_function_(key_extractor2_(input));
        if (UseLocationDetection) {
            pre_writer2_.Put(input);
            location_detection_.Insert(HashCount { hash, 1, /* dia_mask */ 2 });
        }
        else {
            hash_writers2_[hash % context_.num_workers()].Put(input);
        }
    }

    //! Receive elements from other workers, create pre-sorted files
    void MainOp() {
        data::MixStream::MixReader reader1_ =
            hash_stream1_->GetMixReader(/* consume */ true);

        size_t capacity = DIABase::mem_limit_ / sizeof(InputTypeFirst) / 2;

        ReceiveItems<InputTypeFirst>(capacity, reader1_, files1_, key_extractor1_);

        data::MixStream::MixReader reader2_ =
            hash_stream2_->GetMixReader(/* consume */ true);

        capacity = DIABase::mem_limit_ / sizeof(InputTypeSecond) / 2;

        ReceiveItems<InputTypeSecond>(capacity, reader2_, files2_, key_extractor2_);
    }

    template <typename ItemType>
    size_t JoinCapacity() {
        return DIABase::mem_limit_ / sizeof(ItemType) / 4;
    }

    /*!
     * Adds all elements from merge tree to a vector, afterwards sets the first_element
     * pointer to the first element with a different key.
     *
     * \param vec target vector
     *
     * \param puller Input merge tree
     *
     * \param key_extractor Key extractor function
     *
     * \param file_ptr Pointer to a data::File
     *
     * \return Pair of bools, first bool indicates whether the merge tree is
     * emptied, second bool indicates whether external memory was needed.
     */
    template <typename ItemType, typename KeyExtractor, typename MergeTree>
    std::pair<bool, bool> AddEqualKeysToVec(
        std::vector<ItemType>& vec, MergeTree& puller,
        const KeyExtractor& key_extractor, data::FilePtr& file_ptr) {

        vec.push_back(puller.Top());
        Key key = key_extractor(puller.Top());

        size_t capacity = JoinCapacity<ItemType>();

        if (!puller.Update())
            return std::make_pair(true, false);

        while (key_extractor(puller.Top()) == key) {

            if (!mem::memory_exceeded && vec.size() < capacity) {
                vec.push_back(puller.Top());
            }
            else {
                file_ptr = context_.GetFilePtr(this);
                data::File::Writer writer = file_ptr->GetWriter();
                for (const ItemType& item : vec) {
                    writer.Put(item);
                }
                writer.Put(puller.Top());
                //! vec is very large when this happens
                //! swap with empty vector to free the memory
                std::vector<ItemType>().swap(vec);

                return AddEqualKeysToFile(puller, key_extractor, writer, key);
            }

            if (!puller.Update())
                return std::make_pair(true, false);
        }

        return std::make_pair(false, false);
    }

    /*!
     * Adds all elements from merge tree to a data::File, potentially to external memory,
     * afterwards sets the first_element pointer to the first element with a different key.
     *
     * \param puller Input merge tree
     *
     * \param key_extractor Key extractor function
     *
     * \param writer File writer
     *
     * \param key target key
     *
     * \return Pair of bools, first bool indicates whether the merge tree is
     * emptied, second bool indicates whether external memory was needed (always true, when
     * this method was called).
     */
    template <typename KeyExtractor, typename MergeTree>
    std::pair<bool, bool> AddEqualKeysToFile(
        MergeTree& puller, const KeyExtractor& key_extractor,
        data::File::Writer& writer, const Key& key) {
        if (!puller.Update()) {
            return std::make_pair(true, true);
        }

        while (key_extractor(puller.Top()) == key) {
            writer.Put(puller.Top());
            if (!puller.Update())
                return std::make_pair(true, true);
        }

        return std::make_pair(false, true);
    }

    DIAMemUse PreOpMemUse() final {
        return DIAMemUse::Max();
    }

    void StartPreOp(size_t id) final {
        LOG << *this << " running StartPreOp parent_idx=" << id;
        if (!location_detection_initialized_ && UseLocationDetection) {
            location_detection_.Initialize(DIABase::mem_limit_ / 2);
            location_detection_initialized_ = true;
        }

        auto ids = this->parent_ids();

        if (id == 0) {
            pre_writer1_ = pre_file1_.GetWriter();
        }
        if (id == 1) {
            pre_writer2_ = pre_file2_.GetWriter();
        }
    }

    void StopPreOp(size_t id) final {
        LOG << *this << " running StopPreOp parent_idx=" << id;

        if (id == 0) {
            pre_writer1_.Close();
        }
        if (id == 1) {
            pre_writer2_.Close();
        }
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
    void ReceiveItems(
        size_t capacity, data::MixStream::MixReader& reader,
        std::deque<data::File>& files, const KeyExtractor& key_extractor) {

        std::vector<ItemType> vec;
        vec.reserve(capacity);

        while (reader.HasNext()) {
            if (vec.size() < capacity) {
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
    void MergeFiles(std::deque<data::File>& files,
                    CompareFunction compare_function) {

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
    void JoinAllElements(
        const std::vector<InputTypeFirst>& vec1, bool external1,
        const std::vector<InputTypeSecond>& vec2, bool external2) {

        if (!external1 && !external2) {
            for (const InputTypeFirst& join1 : vec1) {
                for (const InputTypeSecond& join2 : vec2) {
                    assert(key_extractor1_(join1) == key_extractor2_(join2));
                    this->PushItem(join_function_(join1, join2));
                }
            }
        }
        else if (external1 && !external2) {
            LOG1 << "Thrill: Warning: Too many equal keys for main memory "
                 << "in first DIA";

            data::File::ConsumeReader reader = join_file1_->GetConsumeReader();

            while (reader.HasNext()) {
                InputTypeFirst join1 = reader.template Next<InputTypeFirst>();
                for (auto const& join2 : vec2) {
                    assert(key_extractor1_(join1) == key_extractor2_(join2));
                    this->PushItem(join_function_(join1, join2));
                }
            }
        }
        else if (!external1 && external2) {
            LOG1 << "Thrill: Warning: Too many equal keys for main memory "
                 << "in second DIA";

            data::File::ConsumeReader reader = join_file2_->GetConsumeReader();

            while (reader.HasNext()) {
                InputTypeSecond join2 = reader.template Next<InputTypeSecond>();
                for (const InputTypeFirst& join1 : vec1) {
                    assert(key_extractor1_(join1) == key_extractor2_(join2));
                    this->PushItem(join_function_(join1, join2));
                }
            }
        }
        else if (external1 && external2) {
            LOG1 << "Thrill: Warning: Too many equal keys for main memory "
                 << "in both DIAs. This is very slow.";

            size_t capacity = JoinCapacity<InputTypeFirst>();

            std::vector<InputTypeFirst> temp_vec;
            temp_vec.reserve(capacity);

            //! file 2 needs to be read multiple times
            data::File::ConsumeReader reader1 = join_file1_->GetConsumeReader();

            while (reader1.HasNext()) {

                for (size_t i = 0; i < capacity && reader1.HasNext() &&
                     !mem::memory_exceeded; ++i) {
                    temp_vec.push_back(reader1.template Next<InputTypeFirst>());
                }

                data::File::Reader reader2 = join_file2_->GetReader(/* consume */ false);

                while (reader2.HasNext()) {
                    InputTypeSecond join2 = reader2.template Next<InputTypeSecond>();
                    for (const InputTypeFirst& join1 : temp_vec) {
                        assert(key_extractor1_(join1) == key_extractor2_(join2));
                        this->PushItem(join_function_(join1, join2));
                    }
                }
                temp_vec.clear();
            }

            //! non-consuming reader, need to clear now
            join_file2_->Clear();
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

        std::sort(vec.begin(), vec.end(),
                  [&key_extractor](const ItemType& i1, const ItemType& i2) {
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

/*!
 * Performs an inner join between this DIA and the DIA given in the first
 * parameter. The  key from each DIA element is hereby extracted with a key
 * extractor function. All pairs of elements with equal keys from both DIAs are
 * then joined with the join function.
 *
 * \tparam KeyExtractor1 Type of the key_extractor1 function. This is a function
 * from FirstDIA::ValueType to the key type.
 *
 * \tparam KeyExtractor2 Type of the key_extractor2 function. This is a function
 * from SecondDIA::ValueType to the key type.
 *
 * \tparam JoinFunction Type of the join_function. This is a function from
 * ValueType and SecondDIA::ValueType to the type of the output DIA.
 *
 * \param first_dia First DIA to join.
 *
 * \param second_dia Second DIA to join.
 *
 * \param key_extractor1 Key extractor for this DIA
 *
 * \param key_extractor2 Key extractor for second DIA
 *
 * \param join_function Join function applied to all equal key pairs
 *
 * \param hash_function If necessary a hash funtion for Key
 *
 * \ingroup dia_dops
 */
template <
    bool LocationDetectionValue,
    typename FirstDIA,
    typename SecondDIA,
    typename KeyExtractor1,
    typename KeyExtractor2,
    typename JoinFunction,
    typename HashFunction =
        std::hash<typename common::FunctionTraits<KeyExtractor1>::result_type> >
auto InnerJoin(
    const LocationDetectionFlag<LocationDetectionValue>&,
    const FirstDIA& first_dia, const SecondDIA& second_dia,
    const KeyExtractor1& key_extractor1, const KeyExtractor2& key_extractor2,
    const JoinFunction& join_function,
    const HashFunction& hash_function = HashFunction()) {

    assert(first_dia.IsValid());
    assert(second_dia.IsValid());

    static_assert(
        std::is_convertible<
            typename FirstDIA::ValueType,
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
            typename FirstDIA::ValueType,
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

    using JoinNode = api::JoinNode<
              JoinResult, FirstDIA, SecondDIA, KeyExtractor1, KeyExtractor2,
              JoinFunction, HashFunction, LocationDetectionValue>;

    auto node = tlx::make_counting<JoinNode>(
        first_dia, second_dia, key_extractor1, key_extractor2, join_function,
        hash_function);

    return DIA<JoinResult>(node);
}

/*!
 * Performs an inner join between this DIA and the DIA given in the first
 * parameter. The  key from each DIA element is hereby extracted with a key
 * extractor function. All pairs of elements with equal keys from both DIAs are
 * then joined with the join function.
 *
 * \tparam KeyExtractor1 Type of the key_extractor1 function. This is a function
 * from FirstDIA::ValueType to the key type.
 *
 * \tparam KeyExtractor2 Type of the key_extractor2 function. This is a function
 * from SecondDIA::ValueType to the key type.
 *
 * \tparam JoinFunction Type of the join_function. This is a function from
 * ValueType and SecondDIA::ValueType to the type of the output DIA.
 *
 * \param first_dia First DIA to join.
 *
 * \param second_dia Second DIA to join.
 *
 * \param key_extractor1 Key extractor for this DIA
 *
 * \param key_extractor2 Key extractor for second DIA
 *
 * \param join_function Join function applied to all equal key pairs
 *
 * \param hash_function If necessary a hash funtion for Key
 *
 * \ingroup dia_dops
 */
template <
    typename FirstDIA,
    typename SecondDIA,
    typename KeyExtractor1,
    typename KeyExtractor2,
    typename JoinFunction,
    typename HashFunction =
        std::hash<typename common::FunctionTraits<KeyExtractor1>::result_type> >
auto InnerJoin(
    const FirstDIA& first_dia, const SecondDIA& second_dia,
    const KeyExtractor1& key_extractor1, const KeyExtractor2& key_extractor2,
    const JoinFunction& join_function,
    const HashFunction& hash_function = HashFunction()) {
    // forward to method _with_ location detection ON
    return InnerJoin(
        LocationDetectionTag,
        first_dia, second_dia, key_extractor1, key_extractor2,
        join_function, hash_function);
}

//! \}

} // namespace api

//! imported from api namespace
using api::InnerJoin;

} // namespace thrill

#endif // !THRILL_API_INNER_JOIN_HEADER

/******************************************************************************/
