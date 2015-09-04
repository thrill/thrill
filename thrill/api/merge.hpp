/*******************************************************************************
 * thrill/api/merge.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MERGE_HEADER
#define THRILL_API_MERGE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/buffered_block_reader.hpp>
#include <thrill/net/collective_communication.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>
#include <random>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

    //TODO(ej) Move somewhere else.
class MergeNodeHelper {
public:
    template <typename ItemType, typename CompareFunction>
    static ItemType GetAt(size_t k, const std::vector<data::File> &files, CompareFunction comperator) {

        static const bool debug = true;

        //TODO: https://stackoverflow.com/questions/8753345/finding-kth-smallest-number-from-n-sorted-arrays/8799608#8799608
        
        //Non-parallel n-way binary search. log^3(n). Yay.
        //Find element with rank k in n sorted arrays. 
        
        size_t n = files.size();
        std::vector<size_t> left(n);
        std::vector<size_t> width(n);
        std::vector<size_t> remap(n);
        std::vector<size_t> mid(n);

        std::fill(left.begin(), left.end(), 0);
        std::iota(remap.begin(), remap.end(), 0);

        size_t sum = 0, len;

        for(size_t i = 0; i < n; i++) {
            len = files[i].num_items();
            sum += len;
            width[i] = std::min(len, k);
        }

        //Assert check wether k is in bounds of all files. 
        assert(sum > k);

        LOG << "#################################";
        LOG << "Searching for element with rank " << k;

        while(true) {

            //Re-map arrays so that largest one is always first.  
            std::sort(remap.begin(), remap.end(), [&] (size_t a, size_t b) {
                return width[b] < width[a];
            });

            size_t j0 = remap[0];
            size_t j = j0;
            mid[j] = left[j] + width[j] / 2;
            LOG << "Master selecting pivot at " << mid[j];
            ItemType pivot = files[j].GetItemAt<ItemType>(mid[j]);
            size_t leftSum = mid[j] - left[j];

            for(size_t i = 1; i < n; i++) {
                j = remap[i];
                mid[j] = files[j].GetIndexOf(pivot, mid[j0], comperator);
                LOG << i << " selecting pivot at " << mid[j];
                leftSum += mid[j] - left[j];
            }
            LOG << "leftSum: " << leftSum;
            LOG << "K: " << k;
            //Recurse.
            if(k < leftSum) {
                for(size_t i = 0; i < n; i++) {
                    j = remap[i];
                    width[j] = mid[j] - left[j];   
                } 
            } else if(k > leftSum) {
                for(size_t i = 0; i < n; i++) {
                    j = remap[i];
                    width[j] -= mid[j] - left[j];   
                    left[j] = mid[j];   
                }
                k -= leftSum;
            } else {
                k = 0;
                break;
            }

            if(leftSum == 0) 
                break;

        }
        size_t j = remap[k];
        return files[j].GetItemAt<ItemType>(mid[j]);
    }

    template <typename ItemType, typename Comperator>
    static size_t IndexOf(ItemType element, size_t tie, const std::vector<data::File> &files, Comperator comperator) {
        static const bool debug = true;

        //Get the index of a given element, or the first
        //Greater one - 1 
        size_t idx = 0;

        for(size_t i = 0; i < files.size(); i++) {
            LOG << "Found " << element << " at " << idx; 
            idx += files[i].GetIndexOf(element, tie, comperator);
            if(tie > files[i].num_items())
                tie -= files[i].num_items(); //Shift tie. 
            else
                tie = 0;
        }

        return idx;
    }
};
//! todo(ej) todo(tb) Can probably subclass a lot here.

template <typename ValueType,
          typename ParentDIARef0, typename ParentDIARef1,
          typename Comperator>
class TwoMergeNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TwoMergeNode(const ParentDIARef0& parent0,
                 const ParentDIARef1& parent1,
                 Comperator comperator,
                 StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(),
                             { parent0.node(), parent1.node() }, stats_node),
          comperator_(comperator)
    {
        // Hook PreOp(s)
        auto pre_op0_fn = [=](const ValueType& input) {
                              writers_[0](input);
                          };
        auto pre_op1_fn = [=](const ValueType& input) {
                              writers_[1](input);
                          };

        // close the function stacks with our pre ops and register it at parent
        // nodes for output
        auto lop_chain0 = parent0.stack().push(pre_op0_fn).emit();
        auto lop_chain1 = parent1.stack().push(pre_op1_fn).emit();

        parent0.node()->RegisterChild(lop_chain0, this->type());
        parent1.node()->RegisterChild(lop_chain1, this->type());
    }

    ~TwoMergeNode() { }

    /*!
     * Actually executes the merge operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() final {
        MainOp();
    }

    void PushData() override {

        size_t result_count = 0;
        static const bool debug = false;

        LOG << "Entering Main OP";

        typedef data::BufferedBlockReader<ValueType, data::ConcatBlockSource<data::CachingBlockQueueSource>> Reader; 

        // get buffered inbound readers from all Channels
        std::vector<Reader> readers;
        for(size_t i = 0; i < channels_.size(); i++) {
            readers.emplace_back(std::move(channels_[i]->OpenCachingReaderSource()));
        }

        // TODO(ej) - call WriteChannelStats() for each channel when these
        // when they are closed ( = you read all data + called Close() on the
        // channels).
        while(true) {

            int biggest = -1;

            for (size_t i = 0; i < readers.size(); i++) {
                if(readers[i].HasValue()) {
                    if(biggest == -1 || comperator_(readers[i].Value(), readers[biggest].Value())) {
                       biggest = (int)i; 
                    }
                }
            }

            if(biggest == -1) {
                LOG << "Finished Merge.";
                //We finished.
                break;
            }

            auto &reader = readers[biggest];

            for (auto func : DIANode<ValueType>::callbacks_) {
                func(reader.Value());
            }

            reader.Next();

            result_count++;
        }

       // sLOG << "Merge: result_count" << result_count;
    }

    void Dispose() final { }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<ValueType>();
    }

private:
    //! Merge comperator
    Comperator comperator_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

    //! TODO
    size_t result_size_;

    //! Files for intermediate storage
    std::vector<data::File> files_ {
        { context_.GetFile(), context_.GetFile() }
    };

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_  {
        { files_[0].GetWriter(), files_[1].GetWriter() }
    };

    //! Array of inbound Channels
    std::array<data::ChannelPtr, num_inputs_> channels_;

    struct Pivot {
        ValueType first;
        size_t second;
        bool valid;
/*
        bool operator <(const Pivot& y) const {
            return std::tie(valid, first, second) < std::tie(valid, y.first, y.second);
        }*/
    };

    Pivot CreatePivot(ValueType v, size_t i) {
        Pivot p;
        p.first = v;
        p.second = i;
        p.valid = true;
        return p;
    }
    

    size_t dataSize; //Count of items on this worker.
    size_t prefixSize; //Count of items on all prev workers.

    template <typename T>
    std::string VToStr(std::vector<T> data) {
        std::stringstream ss;
        
        for(T elem : data)
            ss << elem << " ";

        return ss.str();
    }
    
    std::string VToStr(std::vector<Pivot> data) {
        std::stringstream ss;
        
        for(Pivot elem : data)
            ss << "(" << elem.valid << ", " << elem.first << ", " << elem.second << ") ";

        return ss.str();
    }

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }
        net::FlowControlChannel& flowControl = context_.flow_control_channel();

        //Partitioning happens here.
        
        //Environment
        size_t me = context_.my_rank(); //Local rank. 
        size_t p = context_.num_workers(); //Count of all workers (and count of target partitions)

        LOG << "Splitting to " << p << " workers";

        //Partitions in rank over all local collections.
        std::vector<size_t> partitions(p - 1);
        std::mt19937 rand(0); //Give const seed. May choose this one over the net.

        //Overall count of local items.
        dataSize = 0;

        for(size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i].num_items();
        };

        //Global size off aaaalll data.
        size_t globalSize = flowControl.AllReduce(dataSize, std::plus<ValueType>());

        LOG << "Global size: " << globalSize;

        //Rank we search for
        std::vector<size_t> srank(p - 1);

        for(size_t r = 0; r < p - 1; r++) {
            srank[r] = (globalSize / p) * (r + 1);
        }
        for(size_t r = 0; r < globalSize % p; r++) {
            srank[r] += 1;
        }

        if(debug) {
            for(size_t r = 0; r < p - 1; r++) { 
                LOG << "Search Rank " << r << ": " << srank[r];

                size_t res = flowControl.Broadcast(srank[r]);
                assert(res == srank[r]);
            }
        }

        //Partition borders. Let there by binary search. 
        std::vector<size_t> left(p - 1);
        std::vector<size_t> width(p - 1);

        std::fill(left.begin(), left.end(), 0);
        std::fill(width.begin(), width.end(), dataSize);
       
        prefixSize = flowControl.PrefixSum(dataSize, std::plus<ValueType>(), false); 

        LOG << "Data count left of me: " << prefixSize;

        //Auxillary Arrays
        std::vector<size_t> widthscan(p - 1);
        std::vector<size_t> widthsum(p - 1);
        std::vector<size_t> pivotrank(p - 1);
        std::vector<size_t> split(p - 1);
        std::vector<Pivot> pivots(p - 1);
        std::vector<size_t> splitsum(p - 1);

        Pivot zero = CreatePivot(MergeNodeHelper::GetAt<ValueType>(0, files_, comperator_), 0);
        zero.valid = false;

        //Partition loop 
        
        while(1) {
            flowControl.ArrayPrefixSum(width, widthscan, std::plus<ValueType>(), false);
            flowControl.ArrayAllReduce(width, widthsum, std::plus<ValueType>());
             
            size_t done = 0;

            LOG << "left: " << VToStr(left);
            LOG << "width: " << VToStr(width);
            LOG << "srank: " << VToStr(srank);
            LOG << "widthsum: " << VToStr(widthsum);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 2) { //Yes, this is cheating, but I have no idea why it gets unstable for very low widthsum[r] 
                    partitions[r] = left[r];
                    done++;
                }
            }

            if(done == p - 1) {
                LOG << "Finished";
                break;
            } else {
                LOG << "Continue";
            }
            
            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1) {
                    pivotrank[r] = rand() % widthsum[r];
                } else {
                    pivotrank[r] = 0;
                }
            }
            
            LOG << "Pivotranks: " << VToStr(pivotrank);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1 &&
                   widthscan[r] <= pivotrank[r] &&
                   pivotrank[r] < widthscan[r] + width[r]) {
                   
                   size_t localRank = left[r] + pivotrank[r] - widthscan[r]; 
                   LOG << "Selecting local rank " << localRank << " zero pivot " << r;
                   ValueType pivotElement = MergeNodeHelper::GetAt<ValueType>(localRank, files_, comperator_);

                   if(debug) {
                        assert(MergeNodeHelper::IndexOf(pivotElement, localRank, files_, comperator_) == localRank);
                   }
                    
                   Pivot pivot = CreatePivot(pivotElement, localRank + prefixSize);

                   pivots[r] = pivot;
                } else {
                   LOG << "Selecting zero pivot " << r;
                   pivots[r] = zero;
                }
            }

            LOG << "Pivots: " << VToStr(pivots);

            flowControl.ArrayAllReduce(pivots, pivots, 
            [this] (const Pivot a, const Pivot b) { 
                    if(!a.valid) {
                        return b; 
                    } else if(!b.valid) {
                        return a;
                    } else if(comperator_(b.first, a.first)) {
                        return a; 
                    } else if(comperator_(a.first, b.first)) {
                        return b;
                    } else if(a.second > b.second) {
                        return a;
                    } else {
                        return b;
                    }
            }); //Return maximal pivot (Is this  OK?);

            LOG << "Final Pivots: " << VToStr(pivots);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 1) {
                    split[r] = 0;
                } else {
                    split[r] = MergeNodeHelper::IndexOf(pivots[r].first, pivots[r].second - prefixSize, files_, comperator_) - left[r];
                }
            }

            flowControl.ArrayAllReduce(split, splitsum, std::plus<ValueType>());

            LOG << "Splitters: " << VToStr(split);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] < 1) continue;
                
                if(debug) {
                    assert(width[r] - split[r] <= width[r]);
                }

                if(splitsum[r] < srank[r])  {
                    left[r] += split[r];
                    width[r] -= split[r];
                    srank[r] -= splitsum[r];
                } else {
                    if(debug) {
                        assert(width[r] >= split[r]);
                    }
                    width[r] = split[r];
                }
            } 
        }

        //Cool, parts contains the global splitters now. But we need
        //To convert them to file-local parts. 

        std::vector<std::vector<size_t>> offsets(num_inputs_);

        LOG << "Creating channels";
        
        //Init channels and offsets.
        for(size_t j = 0; j < num_inputs_; j++) {
            channels_[j] = context_.GetNewChannel();
            offsets[j] = std::vector<size_t>(p);
            offsets[j][p - 1] = files_[j].num_items();
        }

        LOG << "Calculating offsets.";

        //TODO(ej) - this is superfluiouuiuius. We can probably
        //Extract the ranks per file from the above loop. 
        for(size_t i = 0; i < p - 1; i++) {
            size_t globalSplitter = partitions[i];
            static const bool debug = false;
            LOG << "Global Splitter " << i << ": " << globalSplitter;
            if(globalSplitter < dataSize) {
                //We have to find file-local splitters.
                ValueType pivot = MergeNodeHelper::GetAt<ValueType, Comperator>(partitions[i], files_, comperator_);

                size_t prefixSum = 0;

                for(size_t j = 0; j < num_inputs_; j++) {
                    offsets[j][i] = files_[j].GetIndexOf(pivot, partitions[i] - prefixSum, comperator_);
                    prefixSum += files_[j].num_items();
                }
            } else {
                //Ok, splitter beyond all sizes. 
                for(size_t j = 0; j < num_inputs_; j++) {
                    offsets[j][i] = files_[j].num_items();
                }
            }
        }
        LOG << "Scattering.";
        
        for(size_t j = 0; j < num_inputs_; j++) {
            static const bool debug = false;
            for(size_t i = 0; i < p; i++) {
                LOG << "Offset " << i << " for file " << j << ": " << offsets[j][i];
            }

            channels_[j]->template Scatter<ValueType>(files_[j], offsets[j]);
        }
   }
};

template <typename ValueType, typename Stack>
template <typename Comperator, typename SecondDIA>
auto DIARef<ValueType, Stack>::Merge(
    SecondDIA second_dia, const Comperator &comperator) const {
    assert(IsValid());
    assert(second_dia.IsValid());

    using CompareResult
              = typename FunctionTraits<Comperator>::result_type;

    using MergeResultNode
              = TwoMergeNode<ValueType, DIARef, SecondDIA, Comperator>;
    
    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            ValueType
            >::value,
        "DIA 1 and DIA 0 have different types");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<Comperator>::template arg<0>
            >::value,
        "Comperator has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            typename FunctionTraits<Comperator>::template arg<1>
            >::value,
        "Comperator has the wrong input type in DIA 1");

    static_assert(
        std::is_convertible<
            bool,
            CompareResult
            >::value,
        "Comperator must return bool");

    StatsNode* stats_node = AddChildStatsNode("Merge", DIANodeType::DOP);
    second_dia.AppendChildStatsNode(stats_node);
    auto merge_node
        = std::make_shared<MergeResultNode>(*this,
                                            second_dia,
                                            comperator,
                                            stats_node);

    auto merge_stack = merge_node->ProduceStack();

    return DIARef<ValueType, decltype(merge_stack)>(
        merge_node,
        merge_stack,
        { stats_node });
}


//! \}

} // namespace api
} // namespace thrill

//! \}
#endif // !THRILL_API_MERGE_HEADER

/******************************************************************************/
