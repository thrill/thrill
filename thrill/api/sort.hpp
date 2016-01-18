/*******************************************************************************
 * thrill/api/sort.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Michael Axtmann <michael.axtmann@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SORT_HEADER
#define THRILL_API_SORT_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/file.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>
#include <cstdlib>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Sort operation. Sort sorts a DIA according to a given
 * compare function
 *
 * \tparam ValueType Type of DIA elements
 * \tparam Stack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam CompareFunction Type of the compare function
 */
template <typename ValueType, typename ParentDIA, typename CompareFunction>
class SortNode final : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    /*!
     * Constructor for a sort node.
     *
     * \param parent DIA.
     * \param parent_stack Stack of lambda functions between parent and this node
     * \param compare_function Function comparing two elements.
     */
    SortNode(const ParentDIA& parent,
             CompareFunction compare_function,
             StatsNode* stats_node)
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, stats_node),
          compare_function_(compare_function),
          stream_id_samples_(parent.ctx().GetNewCatStream()),
          emitters_samples_(stream_id_samples_->OpenWriters()),
          stream_id_data_(parent.ctx().GetNewCatStream()),
          emitters_data_(stream_id_data_->OpenWriters())
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };

        unsorted_data_ = context_.GetFilePtr();
        unsorted_writer_ = unsorted_data_->GetWriter();

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Executes the sum operation.
    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {	
		assert(merge_degree_ > 1);
		size_t numfiles = files_.size();
		if (numfiles > 1) {
			LOG << "start multiwaymerge with " << numfiles << " files";
			using Iterator = thrill::core::FileIteratorWrapper<ValueType>;
			std::vector<std::pair<Iterator, Iterator> > seq;
			for (size_t t = 0; t < numfiles; ++t) {
				std::shared_ptr<data::File::Reader> reader = std::make_shared<data::File::Reader>(
					files_[t].GetReader(consume));
				Iterator s = Iterator(&files_[t], reader, 0, true);
				Iterator e = Iterator(&files_[t], reader, files_[t].num_items(), false);
				seq.push_back(std::make_pair(std::move(s), std::move(e)));
			}
			
			size_t first_file = 0;

			while (numfiles > merge_degree_) {

				size_t sumsizes_ = 0;
				for (size_t i = 0; i < merge_degree_; ++i) {
					sumsizes_ += files_[i].num_items();
				}

				auto puller = core::get_sequential_file_multiway_merge_tree<true, false>(
					seq.begin() + first_file,
					seq.begin() + first_file + merge_degree_,
					sumsizes_,
					compare_function_);

				files_.emplace_back(context_.GetFile());
				auto writer = files_.back().GetWriter();

				while(puller.HasNext()) {
					writer.Put(puller.Next());
				}
				writer.Close();



				first_file += merge_degree_;
				numfiles -= merge_degree_;
				numfiles++;

				for (size_t i = 0; i < merge_degree_; ++i) {
					files_.pop_front();
				}

				std::shared_ptr<data::File::Reader> reader = std::make_shared<data::File::Reader>(
					files_.back().GetReader(consume));
				Iterator s = Iterator(&files_.back(), reader, 0, true);
				Iterator e = Iterator(&files_.back(), reader, files_.back().num_items(), false);

				seq.push_back(std::make_pair(std::move(s), std::move(e)));

			}

			auto puller = core::get_sequential_file_multiway_merge_tree<true, false>(
				seq.begin() + first_file,
				seq.end(),
				totalsize_,
				compare_function_);

            while(puller.HasNext()) {
				this->PushItem(puller.Next());
			}
		} else {
			if (totalsize_) {
				data::File::Reader reader =  files_[0].GetReader(consume);
				while(reader.HasNext()) {
					this->PushItem(reader.Next<ValueType>());
				}
			}
		}
    }

    void Dispose() final {  }

private:
    //! The sum function which is applied to two elements.
    CompareFunction compare_function_;
    //! Sample vector
    std::vector<ValueType> samples_;
	//! All local unsorted elements before communicaton
    data::FilePtr unsorted_data_;
	//! Writer for unsorted_data_
    data::File::Writer unsorted_writer_;
	//! Number of items on this worker
    size_t total_items_ = 0;
	//! Number of items since last sample was drawn
    size_t after_sample_ = 0;
	//! Number of items between this and next sample
    size_t next_sample_ = 0;

	//! Maximum merging degree 
	size_t merge_degree_ = 10;
	//! Maximum number of elements per file while merging
	size_t elements_per_file_ = 10000;
	//! Total number of local elements after communication
	size_t totalsize_ = 0;
	//! Local data files
	std::deque<data::File> files_;

    //! Emitter to send samples to process 0
    data::CatStreamPtr stream_id_samples_;
    std::vector<data::CatStream::Writer> emitters_samples_;

    //! Emitters to send data to other workers specified by splitters.
    data::CatStreamPtr stream_id_data_;
    std::vector<data::CatStream::Writer> emitters_data_;

    // epsilon
    static constexpr double desired_imbalance_ = 0.25;

    void PreOp(ValueType input) {
        unsorted_writer_.Put(input);
        total_items_++;
        after_sample_++;
        //In this stage we do not know how many elements are there in total.
        //Therefore we draw samples based on current number of elements and
        //randomly replace older samples when we have too many.
        if (after_sample_ > next_sample_) {
            if (samples_.size() >
                std::log2(total_items_ * context_.num_workers())
                * (1 / (desired_imbalance_ * desired_imbalance_))) {
                samples_[std::rand() % samples_.size()] = input;
            } else {
                samples_.push_back(input);
            }
            after_sample_ = 0;
            double sample = (double)total_items_ /
                (std::log2(total_items_ * context_.num_workers())
                 * (1 / (desired_imbalance_ * desired_imbalance_)));
            next_sample_ = std::floor(sample);
        }
    }

    void FindAndSendSplitters(
        std::vector<ValueType>& splitters, size_t sample_size) {
        // Get samples from other workers
        size_t num_total_workers = context_.num_workers();

        std::vector<ValueType> samples;
        samples.reserve(sample_size * num_total_workers);
        // TODO(tb): OpenConsumeReader
        auto reader = stream_id_samples_->OpenCatReader(true);

        while (reader.HasNext()) {
            samples.push_back(reader.template Next<ValueType>());
        }

        // Find splitters
        std::sort(samples.begin(), samples.end(), compare_function_);

        size_t splitting_size = samples.size() / num_total_workers;

        // Send splitters to other workers
        for (size_t i = 1; i < num_total_workers; i++) {
            splitters.push_back(samples[i * splitting_size]);
            for (size_t j = 1; j < num_total_workers; j++) {
                emitters_samples_[j].Put(samples[i * splitting_size]);
            }
        }

        for (size_t j = 1; j < num_total_workers; j++) {
            emitters_samples_[j].Close();
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

    void EmitToBuckets(
        // Tree of splitters, sizeof |splitter|
        const ValueType* const tree,
        // Number of buckets: k = 2^{log_k}
        size_t k,
        size_t log_k,
        // Number of actual workers to send to
        size_t actual_k,
        const ValueType* const sorted_splitters,
        size_t prefix_elem,
        size_t total_elem) {

        auto unsorted_reader_ = unsorted_data_->GetReader(/* consume */ true);

        // enlarge emitters array to next power of two to have direct access,
        // because we fill the splitter set up with sentinels == last splitter,
        // hence all items land in the last bucket.
        assert(emitters_data_.size() == actual_k);
        assert(actual_k <= k);

        while (emitters_data_.size() < k)
            emitters_data_.emplace_back(nullptr);

        std::swap(emitters_data_[actual_k - 1], emitters_data_[k - 1]);

        // classify all items (take two at once) and immediately transmit them.

        const size_t stepsize = 2;

        size_t i = 0;
        for ( ; i < RoundDown(total_items_, stepsize); i += stepsize)
        {
            // take two items
            size_t j0 = 1;
            const ValueType& el0 = unsorted_reader_.Next<ValueType>();

            size_t j1 = 1;
            const ValueType& el1 = unsorted_reader_.Next<ValueType>();

            // run items down the tree
            for (size_t l = 0; l < log_k; l++)
            {
                j0 = j0 * 2 + !(compare_function_(el0, tree[j0]));
                j1 = j1 * 2 + !(compare_function_(el1, tree[j1]));
            }

            size_t b0 = j0 - k;
            size_t b1 = j1 - k;

            if (b0 && Equal(el0, sorted_splitters[b0 - 1])) {
                while (b0 && Equal(el0, sorted_splitters[b0 - 1])
                       && (prefix_elem + i) * actual_k < b0 * total_elem) {
                    b0--;
                }

                if (b0 + 1 >= actual_k) {
                    b0 = k - 1;
                }
            }

            assert(emitters_data_[b0].IsValid());
            emitters_data_[b0].Put(el0);

            if (b1 && Equal(el1, sorted_splitters[b1 - 1])) {
                while (b1 && Equal(el1, sorted_splitters[b1 - 1])
                       && (prefix_elem + i + 1) * actual_k < b1 * total_elem) {
                    b1--;
                }

                if (b1 + 1 >= actual_k) {
                    b1 = k - 1;
                }
            }

            assert(emitters_data_[b1].IsValid());
            emitters_data_[b1].Put(el1);
        }

        // last iteration of loop if we have an odd number of items.
        for ( ; i < total_items_; i++)
        {
            
            const ValueType& last_item = unsorted_reader_.Next<ValueType>();
            size_t j = 1;
            for (size_t l = 0; l < log_k; l++)
            {
                j = j * 2 + !(compare_function_(last_item, tree[j]));
            }

            size_t b = j - k;

            while (b && Equal(last_item, sorted_splitters[b - 1])
                   && (prefix_elem + i) * actual_k < b * total_elem) {
                b--;
            }

            if (b + 1 >= actual_k) {
                b = k - 1;
            }

            assert(emitters_data_[b].IsValid());
            emitters_data_[b].Put(last_item);
        }
    }

	void SortAndWriteToFile(std::vector<ValueType>& vec, std::deque<data::File>& files) {
		std::sort(vec.begin(), vec.end(), compare_function_);
		files.emplace_back(context_.GetFile());
		auto writer = files.back().GetWriter();
		for (auto ele : vec) {
			writer.Put(ele);
		}
		writer.Close();
		vec.clear();
	}

    void MainOp() {
        unsorted_writer_.Close();
        net::FlowControlChannel& channel = context_.flow_control_channel();

        size_t prefix_elem = channel.PrefixSum(total_items_, (size_t)0, std::plus<size_t>(), false);
        size_t total_elem = channel.AllReduce(total_items_);

        size_t num_total_workers = context_.num_workers();

        LOG << "Local sample size on worker " << context_.my_rank() <<
            ": " << samples_.size();
        LOG << "Number of elements: " << total_items_;
        
        
        //Send all samples to worker 0.
        for (auto sample : samples_) {
            emitters_samples_[0].Put(sample);
        }
        emitters_samples_[0].Close();        
        std::vector<ValueType>().swap(samples_);
        
        // Get the ceiling of log(num_total_workers), as SSSS needs 2^n buckets.
        size_t ceil_log = common::IntegerLog2Ceil(num_total_workers);
        size_t workers_algo = 1 << ceil_log;
        size_t splitter_count_algo = workers_algo - 1;

        std::vector<ValueType> splitters;
        splitters.reserve(workers_algo);

        if (context_.my_rank() == 0) {
            FindAndSendSplitters(splitters, samples_.size());
        }
        else {
            // Close unused emitters
            for (size_t j = 1; j < num_total_workers; j++) {
                emitters_samples_[j].Close();
            }
            bool consume = false;
            auto reader = stream_id_samples_->OpenCatReader(consume);
            while (reader.HasNext()) {
                splitters.push_back(reader.template Next<ValueType>());
            }
        }
        stream_id_samples_->Close();

        // code from SS2NPartition, slightly altered

        ValueType* splitter_tree = new ValueType[workers_algo + 1];

        // add sentinel splitters if fewer nodes than splitters.
        for (size_t i = num_total_workers; i < workers_algo; i++) {
            splitters.push_back(splitters.back());
        }

        TreeBuilder(splitter_tree,
                    splitters.data(),
                    splitter_count_algo);

        EmitToBuckets(
            splitter_tree, // Tree. sizeof |splitter|
            workers_algo,  // Number of buckets
            ceil_log,
            num_total_workers,
            splitters.data(),
            prefix_elem,
            total_elem);

        delete[] splitter_tree;

        // end of SS2N

        for (size_t i = 0; i < emitters_data_.size(); i++)
            emitters_data_[i].Close();

        bool consume = false;
        auto reader = stream_id_data_->OpenCatReader(consume);

		std::vector<ValueType> temp_data;
		temp_data.reserve(elements_per_file_);
		size_t items_in_file = 0;
		LOG << "Writing files";
        while (reader.HasNext()) {
			if (items_in_file < elements_per_file_) {
				totalsize_++;
				temp_data.push_back(reader.template Next<ValueType>());
				items_in_file++;
			} else {
				SortAndWriteToFile(temp_data, files_);
				items_in_file = 0;
			}
        }
		if (items_in_file) {
			SortAndWriteToFile(temp_data, files_);
		}
		
		
        stream_id_data_->Close();

        double balance = 0;
        if (totalsize_ > 0) {
            balance = static_cast<double>(totalsize_)
                      * static_cast<double>(num_total_workers)
                      / static_cast<double>(total_elem);
        }

        if (balance > 1) {
            balance = 1 / balance;
        }

        STATC << "NodeType" << "Sort"
              << "Workers" << num_total_workers
              << "LocalSize" << totalsize_
              << "Balance Factor" << balance
              << "Sample Size" << samples_.size();

        this->WriteStreamStats(stream_id_data_);
        this->WriteStreamStats(stream_id_samples_);
    }

    void PostOp() { }
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

    StatsNode* stats_node = AddChildStatsNode("Sort", DIANodeType::DOP);
    auto shared_node
        = std::make_shared<SortNode>(*this, compare_function, stats_node);

    return DIA<ValueType>(shared_node, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SORT_HEADER

/******************************************************************************/
