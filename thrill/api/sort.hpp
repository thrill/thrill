/*******************************************************************************
 * thrill/api/sort.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Michael Axtmann <michael.axtmann@kit.edu>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SORT_HEADER
#define THRILL_API_SORT_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/groupby_iterator.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/data/file.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Sort operation. Sort sorts a DIA according to a
 * given compare function
 *
 * \tparam ValueType Type of DIA elements
 *
 * \tparam Stack Function stack, which contains the chained lambdas between the
 * last and this DIANode.
 *
 * \tparam CompareFunction Type of the compare function
 */
template <typename ValueType, typename ParentDIA, typename CompareFunction>
class SortNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    /*!
     * Constructor for a sort node.
     */
    SortNode(const ParentDIA& parent,
             CompareFunction compare_function)
        : Super(parent.ctx(), "Sort", { parent.id() }, { parent.node() }),
          compare_function_(compare_function)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* id */) final {
        unsorted_writer_ = unsorted_file_.GetWriter();
    }

    void PreOp(const ValueType& input) {
        unsorted_writer_.Put(input);
        local_items_++;
        // In this stage we do not know how many elements are there in total.
        // Therefore we draw samples based on current number of elements and
        // randomly replace older samples when we have too many.
        if (--sample_interval_ == 0) {
            if (samples_.size() == 0 || samples_.size() < wanted_sample_size()) {
                samples_.push_back(input);
            }
            else {
                samples_[rng_() % samples_.size()] = input;
            }
            sample_interval_ = std::max(
                size_t(1), local_items_ / wanted_sample_size());
            LOG << "SortNode::PreOp() sample_interval_=" << sample_interval_;
        }
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!ParentDIA::stack_empty) return false;

        // accept file
        unsorted_file_ = file;
        local_items_ = unsorted_file_.num_items();

        size_t pick_items = std::min(local_items_, wanted_sample_size());

        sLOG << "Pick" << pick_items << "samples by random access.";
        for (size_t i = 0; i < pick_items; ++i) {
            size_t index = rng_() % local_items_;
            sLOG << "got index[" << i << "] = " << index;
            samples_.emplace_back(
                unsorted_file_.GetItemAt<ValueType>(index));
        }

        return true;
    }

    void StopPreOp(size_t /* id */) final {
        unsorted_writer_.Close();

        LOG << "wanted_sample_size()=" << wanted_sample_size()
            << " samples.size()= " << samples_.size();
        assert(samples_.size()
               == std::min(local_items_, wanted_sample_size()));
    }

    DIAMemUse ExecuteMemUse() final {
        return DIAMemUse::Max();
    }

    //! Executes the sum operation.
    void Execute() final {
        MainOp();
    }

    DIAMemUse PushDataMemUse() final {
        if (files_.size() <= 1) {
            // direct push, no merge necessary
            return 0;
        }
        else {
            // need to perform multiway merging
            return DIAMemUse::Max();
        }
    }

    //! calculate maximum merging degree from available memory and the number of
    //! files. additionally calculate the prefetch size of each File.
    std::pair<size_t, size_t> MaxMergeDegreePrefetch() {
        size_t avail_blocks = DIABase::mem_limit_ / data::default_block_size;
        if (files_.size() >= avail_blocks) {
            // more files than blocks available -> partial merge of avail_blocks
            // Files with prefetch = 0, which is one read Block per File.
            return std::make_pair(avail_blocks, 0u);
        }
        else {
            // less files than available Blocks -> split blocks equally among
            // Files.
            return std::make_pair(
                files_.size(), (avail_blocks / files_.size()) - 1);
        }
    }

    void PushData(bool consume) final {
        if (files_.size() == 0) {
            // nothing to push
        }
        else if (files_.size() == 1) {
            this->PushFile(files_[0], consume);
        }
        else {
            size_t merge_degree, prefetch;

            // merge batches of files if necessary
            while (files_.size() > MaxMergeDegreePrefetch().first)
            {
                std::tie(merge_degree, prefetch) = MaxMergeDegreePrefetch();

                sLOG1 << "Partial multi-way-merge of"
                      << merge_degree << "files with prefetch" << prefetch;

                // create merger for first merge_degree_ Files
                std::vector<data::File::ConsumeReader> seq;
                seq.reserve(merge_degree);

                for (size_t t = 0; t < merge_degree; ++t)
                    seq.emplace_back(files_[t].GetConsumeReader(prefetch));

                auto puller = core::make_multiway_merge_tree<ValueType>(
                    seq.begin(), seq.end(), compare_function_);

                // create new File for merged items
                files_.emplace_back(context_.GetFile());
                auto writer = files_.back().GetWriter();

                while (puller.HasNext()) {
                    writer.Put(puller.Next());
                }
                writer.Close();

                // this clear is important to release references to the files.
                seq.clear();

                // remove merged files
                files_.erase(files_.begin(), files_.begin() + merge_degree);
            }

            std::tie(merge_degree, prefetch) = MaxMergeDegreePrefetch();

            sLOG1 << "Start multi-way-merge of" << files_.size() << "files"
                  << "with prefetch" << prefetch;

            // construct output merger of remaining Files
            std::vector<data::File::ConsumeReader> seq;
            seq.reserve(files_.size());

            for (size_t t = 0; t < files_.size(); ++t)
                seq.emplace_back(files_[t].GetConsumeReader(prefetch));

            auto puller = core::make_multiway_merge_tree<ValueType>(
                seq.begin(), seq.end(), compare_function_);

            while (puller.HasNext()) {
                this->PushItem(puller.Next());
            }
        }
    }

private:
    //! The comparison function which is applied to two elements.
    CompareFunction compare_function_;

    //! \name PreOp Phase
    //! {

    //! All local unsorted items before communication
    data::File unsorted_file_ { context_.GetFile() };
    //! Writer for unsorted_file_
    data::File::Writer unsorted_writer_;
    //! Number of items on this worker
    size_t local_items_ = 0;

    //! Sample vector
    std::vector<ValueType> samples_;
    //! Number of items to process before the next sample was drawn
    size_t sample_interval_ = 1;
    //! Random generator
    std::default_random_engine rng_ { std::random_device { } () };

    //! epsilon
    static constexpr double desired_imbalance_ = 0.3;

    //! calculate currently desired number of samples
    size_t wanted_sample_size() const {
        size_t s = std::log2(local_items_ * context_.num_workers())
                   * (1 / (desired_imbalance_ * desired_imbalance_));
        return std::max(s, size_t(1));
    }

    //! }

    //! Local data files
    std::deque<data::File> files_;
    //! Total number of local elements after communication
    size_t local_out_size_ = 0;

    void FindAndSendSplitters(
        std::vector<ValueType>& splitters, size_t sample_size,
        data::MixStreamPtr& sample_stream,
        std::vector<data::MixStream::Writer>& sample_writers) {

        // Get samples from other workers
        size_t num_total_workers = context_.num_workers();

        std::vector<ValueType> samples;
        samples.reserve(sample_size * num_total_workers);

        auto reader = sample_stream->OpenMixReader(/* consume */ true);

        while (reader.HasNext()) {
            samples.push_back(reader.template Next<ValueType>());
        }
        if (samples.size() == 0) return;

        // Find splitters
        std::sort(samples.begin(), samples.end(), compare_function_);

        size_t splitting_size = samples.size() / num_total_workers;

        // Send splitters to other workers
        for (size_t i = 1; i < num_total_workers; i++) {
            splitters.push_back(samples[i * splitting_size]);
            for (size_t j = 1; j < num_total_workers; j++) {
                sample_writers[j].Put(samples[i * splitting_size]);
            }
        }

        for (size_t j = 1; j < num_total_workers; j++) {
            sample_writers[j].Close();
        }
    }

    class TreeBuilder
    {
    public:
        ValueType* tree_;
        ValueType* samples_;
        size_t index_ = 0;
        size_t ssplitter_;

        /*!
         * Target: tree. Size of 'number of splitter'
         * Source: sorted splitters. Size of 'number of splitter'
         * Number of splitter
         */
        TreeBuilder(ValueType* splitter_tree,
                    ValueType* samples,
                    size_t ssplitter)
            : tree_(splitter_tree),
              samples_(samples),
              ssplitter_(ssplitter) {
            if (ssplitter != 0)
                recurse(samples, samples + ssplitter, 1);
        }

        void recurse(ValueType* lo, ValueType* hi, unsigned int treeidx) {

            // pick middle element as splitter
            ValueType* mid = lo + (ssize_t)(hi - lo) / 2;
            assert(mid < samples_ + ssplitter_);
            tree_[treeidx] = *mid;

            ValueType* midlo = mid, * midhi = mid + 1;

            if (2 * treeidx < ssplitter_)
            {
                recurse(lo, midlo, 2 * treeidx + 0);
                recurse(midhi, hi, 2 * treeidx + 1);
            }
        }
    };

    bool Equal(const ValueType& ele1, const ValueType& ele2) {
        return !(compare_function_(ele1, ele2) || compare_function_(ele2, ele1));
    }

    //! round n down by k where k is a power of two.
    template <typename Integral>
    static inline size_t RoundDown(Integral n, Integral k) {
        return (n & ~(k - 1));
    }

    void TransmitItems(
        // Tree of splitters, sizeof |splitter|
        const ValueType* const tree,
        // Number of buckets: k = 2^{log_k}
        size_t k,
        size_t log_k,
        // Number of actual workers to send to
        size_t actual_k,
        const ValueType* const sorted_splitters,
        size_t prefix_items,
        size_t total_items,
        data::MixStreamPtr& data_stream) {

        data::File::ConsumeReader unsorted_reader =
            unsorted_file_.GetConsumeReader();

        std::vector<data::MixStream::Writer> data_writers =
            data_stream->OpenWriters();

        // enlarge emitters array to next power of two to have direct access,
        // because we fill the splitter set up with sentinels == last splitter,
        // hence all items land in the last bucket.
        assert(data_writers.size() == actual_k);
        assert(actual_k <= k);

        while (data_writers.size() < k)
            data_writers.emplace_back(nullptr);

        std::swap(data_writers[actual_k - 1], data_writers[k - 1]);

        // classify all items (take two at once) and immediately transmit them.

        const size_t stepsize = 2;

        size_t i = 0;
        for ( ; i < RoundDown(local_items_, stepsize); i += stepsize)
        {
            // take two items
            size_t j0 = 1;
            ValueType el0 = unsorted_reader.Next<ValueType>();

            size_t j1 = 1;
            ValueType el1 = unsorted_reader.Next<ValueType>();

            // run items down the tree
            for (size_t l = 0; l < log_k; l++)
            {
                j0 = 2 * j0 + (compare_function_(el0, tree[j0]) ? 0 : 1);
                j1 = 2 * j1 + (compare_function_(el1, tree[j1]) ? 0 : 1);
            }

            size_t b0 = j0 - k;
            size_t b1 = j1 - k;

            if (b0 && Equal(el0, sorted_splitters[b0 - 1])) {
                while (b0 && Equal(el0, sorted_splitters[b0 - 1]) &&
                       (prefix_items + i) * actual_k < b0 * total_items) {
                    b0--;
                }

                if (b0 + 1 >= actual_k) {
                    b0 = k - 1;
                }
            }

            if (b1 && Equal(el1, sorted_splitters[b1 - 1])) {
                while (b1 && Equal(el1, sorted_splitters[b1 - 1]) &&
                       (prefix_items + i + 1) * actual_k < b1 * total_items) {
                    b1--;
                }

                if (b1 + 1 >= actual_k) {
                    b1 = k - 1;
                }
            }

            assert(data_writers[b0].IsValid());
            assert(data_writers[b1].IsValid());

            data_writers[b0].Put(el0);
            data_writers[b1].Put(el1);
        }

        // last iteration of loop if we have an odd number of items.
        for ( ; i < local_items_; i++)
        {
            size_t j0 = 1;
            ValueType el0 = unsorted_reader.Next<ValueType>();

            // run item down the tree
            for (size_t l = 0; l < log_k; l++)
            {
                j0 = 2 * j0 + (compare_function_(el0, tree[j0]) ? 0 : 1);
            }

            size_t b0 = j0 - k;

            while (b0 && Equal(el0, sorted_splitters[b0 - 1])
                   && (prefix_items + i) * actual_k < b0 * total_items) {
                b0--;
            }

            if (b0 + 1 >= actual_k) {
                b0 = k - 1;
            }

            assert(data_writers[b0].IsValid());
            data_writers[b0].Put(el0);
        }

        // close writers and flush data
        for (size_t j = 0; j < data_writers.size(); j++)
            data_writers[j].Close();
    }

    void SortAndWriteToFile(
        std::vector<ValueType>& vec, std::deque<data::File>& files) {

        LOG1 << "SortAndWriteToFile() " << vec.size()
             << " items into file #" << files.size();

        local_out_size_ += vec.size();

        // advice block pool to write out data if necessary
        context_.block_pool().AdviseFree(vec.size() * sizeof(ValueType));

        common::StatsTimerStart timer;

        std::sort(vec.begin(), vec.end(), compare_function_);

        LOG << "SortAndWriteToFile() sort took " << timer;

        files.emplace_back(context_.GetFile());
        auto writer = files.back().GetWriter();
        for (const ValueType& ele : vec) {
            writer.Put(ele);
        }
        writer.Close();

        LOG << "SortAndWriteToFile() finished writing files";

        vec.clear();

        LOG << "SortAndWriteToFile() vector cleared";
    }

    void MainOp() {
        size_t prefix_items = context_.net.ExPrefixSum(local_items_);
        size_t total_items = context_.net.AllReduce(local_items_);

        size_t num_total_workers = context_.num_workers();

        LOG << "Local sample size on worker " << context_.my_rank() <<
            ": " << samples_.size();
        LOG << "Number of local items: " << local_items_;

        // stream to send samples to process 0 and receive them back
        data::MixStreamPtr sample_stream = context_.GetNewMixStream();

        // Send all samples to worker 0.
        std::vector<data::MixStream::Writer> sample_writers =
            sample_stream->OpenWriters();

        for (const ValueType& sample : samples_) {
            sample_writers[0].Put(sample);
        }
        sample_writers[0].Close();
        std::vector<ValueType>().swap(samples_);

        // Get the ceiling of log(num_total_workers), as SSSS needs 2^n buckets.
        size_t ceil_log = common::IntegerLog2Ceil(num_total_workers);
        size_t workers_algo = 1 << ceil_log;
        size_t splitter_count_algo = workers_algo - 1;

        std::vector<ValueType> splitters;
        splitters.reserve(workers_algo);

        if (context_.my_rank() == 0) {
            FindAndSendSplitters(splitters, samples_.size(),
                                 sample_stream, sample_writers);
        }
        else {
            // Close unused emitters
            for (size_t j = 1; j < num_total_workers; j++) {
                sample_writers[j].Close();
            }
            data::MixStream::MixReader reader =
                sample_stream->OpenMixReader(/* consume */ true);
            while (reader.HasNext()) {
                splitters.push_back(reader.template Next<ValueType>());
            }
        }
        sample_writers.clear();
        sample_stream->Close();

        // code from SS2NPartition, slightly altered

        ValueType* splitter_tree = new ValueType[workers_algo + 1];

        // add sentinel splitters if fewer nodes than splitters.
        for (size_t i = num_total_workers; i < workers_algo; i++) {
            splitters.push_back(splitters.back());
        }

        TreeBuilder(splitter_tree,
                    splitters.data(),
                    splitter_count_algo);

        data::MixStreamPtr data_stream = context_.GetNewMixStream();

        // launch receiver thread.
        context_.thread_pool_.Enqueue(
            [this, &data_stream]() {
                return ReceiveItems(data_stream);
            });

        TransmitItems(
            splitter_tree, // Tree. sizeof |splitter|
            workers_algo,  // Number of buckets
            ceil_log,
            num_total_workers,
            splitters.data(),
            prefix_items,
            total_items,
            data_stream);

        delete[] splitter_tree;

        context_.thread_pool_.LoopUntilEmpty();

        data_stream->Close();

        double balance = 0;
        if (local_out_size_ > 0) {
            balance = static_cast<double>(local_out_size_)
                      * static_cast<double>(num_total_workers)
                      / static_cast<double>(total_items);
        }

        if (balance > 1) {
            balance = 1 / balance;
        }

        Super::logger_
            << "class" << "SortNode"
            << "event" << "done"
            << "workers" << num_total_workers
            << "local_out_size" << local_out_size_
            << "balance" << balance
            << "sample_size" << samples_.size();
    }

    void ReceiveItems(data::MixStreamPtr& data_stream) {

        auto reader = data_stream->OpenMixReader(/* consume */ true);

        LOG << "Writing files";

        // M/2 such that the other half is used to prepare the next bulk
        size_t capacity = DIABase::mem_limit_ / sizeof(ValueType) / 2;
        std::vector<ValueType> temp_data;
        temp_data.reserve(capacity);

        while (reader.HasNext()) {
            if (!mem::memory_exceeded && temp_data.size() < capacity) {
                temp_data.push_back(reader.template Next<ValueType>());
            }
            else {
                SortAndWriteToFile(temp_data, files_);
            }
        }

        if (temp_data.size())
            SortAndWriteToFile(temp_data, files_);
    }
};

template <typename ValueType, typename Stack>
template <typename CompareFunction>
auto DIA<ValueType, Stack>::Sort(const CompareFunction &compare_function) const {
    assert(IsValid());

    using SortNode = api::SortNode<ValueType, DIA, CompareFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<0>
            >::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<1> >::value ||
        std::is_same<CompareFunction, std::less<ValueType> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<CompareFunction>::result_type,
            bool>::value,
        "CompareFunction has the wrong output type (should be bool)");

    auto shared_node = std::make_shared<SortNode>(*this, compare_function);

    return DIA<ValueType>(shared_node);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SORT_HEADER

/******************************************************************************/
