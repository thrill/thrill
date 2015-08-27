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

//! Todo(ej) Merge has to identical types. 
//! todo(ej) todo(tb) Can probably subclass a lot here.

template <typename ValueType,
          typename ParentDIARef0, typename ParentDIARef1,
          typename Comperator>
class TwoMergeNode : public DOpNode<ValueType>
{
    static const bool debug = true;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TwoMergeNode(const ParentDIARef0& parent0,
               const ParentDIARef1& parent1,
               Comperator comperator,
               StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(), { parent0.node(), parent1.node() }, "MergeNode", stats_node),
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

        typedef data::BufferedBlockReader<ValueType, data::ConcatBlockSource<data::CachingBlockQueueSource>> Reader; 

        // get buffered inbound readers from all Channels
        std::vector<Reader> readers;
        for(size_t i = 0; i < channels_.size(); i++) {
            readers.emplace_back(std::move(channels_[i]->OpenCachingReaderSource()));
        }

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

        sLOG << "Merge: result_count" << result_count;
    }

    void Dispose() final { }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<ValueType>();
    }

    /*!
     * Returns "[MergeNode]" as a string.
     * \return "[MergeNode]"
     */
    std::string ToString() final {
        return "[MergeNode]";
    }

    template <typename ItemType, typename CompareFunction>
    static ItemType GetAt(size_t k, const std::vector<data::File> &files, const CompareFunction comperator) {
        //Select am element based on its rank from 
        //all collections.
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

        for(size_t i = 0; i < n; i++) {
            width[i] = std::max(files[n].NumItems(), k);
        }

        while(true) {

            //Re-map arrays so that largest one is always first.  
            std::sort(remap.begin(), remap.end(), [&] (size_t a, size_t b) {
                return width[b] < width[a];
            });

            bool done = true;

            for(size_t i = 0; i < n; i++) {
                if(width[i] > 1) {
                    done = false; 
                    break;
                }
            }

            if(done)
                break;
           

            size_t j0 = remap[0];
            size_t j = j0;
            mid[j] = left[j] + width[j] / 2;
            ItemType pivot = files[j].GetItemAt<ItemType>(mid[j]);
            size_t leftSum = mid[j] - left[j];

            for(size_t i = 1; i < n; i++) {
                j = remap[i];
                mid[j] = files[j].GetIndexOf(pivot, mid[j0], comperator);
                leftSum += mid[j] - left[j];
            }

            //Recurse.
            if(k < leftSum) {
                for(size_t i = 0; i < n; i++) {
                    j = remap[i];
                    width[j] = mid[j] - left[j];   
                } 
            } else {
                for(size_t i = 0; i < n; i++) {
                    left[j] = mid[j] + left[j];   
                    width[j] -= mid[j] - left[j];   
                    k -= leftSum;
                }
            }
        }

        return files[remap[k]].GetItemAt<ItemType>(mid[remap[k]]);
    }

    template <typename ItemType>
    size_t IndexOf(ItemType element, size_t tie, const std::vector<data::File> &files) { 
        //Get the index of a given element, or the first
        //Greater one. 
        size_t idx = 0;

        for(size_t i = 0; i < files.size(); i++) {
            idx += files[i].GetIndexOf(element, tie);
            tie -= files[i].NumItems(); //Shift tie. 
        }

        return idx;
    }

private:
    //! Merge comperator
    Comperator comperator_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

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

        bool operator <(const Pivot& y) const {
            return std::tie(first, second) < std::tie(y.first, y.second);
        }
    };

    Pivot CreatePivot(ValueType v, size_t i) {
        Pivot p;
        p.first = v;
        p.second = i;
        return p;
    }
    

    size_t dataSize; //Count of items on this worker.
    size_t prefixSize; //Count of items on all prev workers. 

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

        //Partitions in rank over all local collections.
        std::vector<size_t> partitions(p - 1);
        std::mt19937 rand(0); //Give const seed. May choose this one over the net.

        //Overall count of local items.
        dataSize = 0;

        for(size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i].NumItems();
        };

        //Care! This does only work if dataSize is the same
        //on each worker. 
        size_t targetSize = dataSize;

        //Partition borders. Let there by binary search. 
        std::vector<size_t> left(p - 1);
        std::vector<size_t> width(p - 1);

        std::fill(left.begin(), left.end(), 0);
        std::fill(width.begin(), width.end(), dataSize);
       
        prefixSize = flowControl.PrefixSum(dataSize, std::plus<ValueType>(), false); 

        //Rank we search for
        std::vector<size_t> srank(p - 1);

        for(size_t r = 0; r  < p - 1; r++) {
            srank[r] = (r + 1) * targetSize;
        }

        //Auxillary Arrays
        std::vector<size_t> widthscan(p - 1);
        std::vector<size_t> widthsum(p - 1);
        std::vector<size_t> pivotrank(p - 1);
        std::vector<size_t> split(p - 1);
        std::vector<Pivot> pivots;
        pivots.reserve(p - 1);
        std::vector<size_t> splitsum(p - 1);

        Pivot zero = CreatePivot(GetAt<ValueType>(0, files_, comperator_), 0);

        //Partition loop 
        
        while(1) {
            flowControl.ArrayPrefixSum(width, widthscan, std::plus<ValueType>(), false);
            flowControl.ArrayAllReduce(width, widthsum, std::plus<ValueType>());
             
            size_t done = 0;

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 1) { //Not sure about this condition. It's been modified. 
                    partitions[r] = left[r];
                    done++;
                }
            }

            if(done == p - 1) break;
            
            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1) {
                    pivotrank[r] = rand() % widthsum[r];
                } else {
                    pivotrank[r] = 0;
                }
            }

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1 &&
                   widthscan[r] <= pivotrank[r] &&
                   pivotrank[r] < widthscan[r] + width[r]) {
                   
                   size_t localRank = left[r] + pivotrank[r] - widthscan[r]; 
                   ValueType pivotElement = GetAt<ValueType>(localRank, files_, comperator_);
                    
                   Pivot pivot = CreatePivot(pivotElement, localRank + prefixSize);

                   pivots[r] = pivot;
                } else {
                   pivots[r] = zero;
                }
            }

            flowControl.ArrayAllReduce(pivots, pivots, [] (const Pivot a, const Pivot b) { return b < a ? a : b; }); //Return maximal pivot (Is this  OK?);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 1) {
                    split[r] = 0;
                } else {
                    split[r] = IndexOf(pivots[r].first, pivots[r].second - prefixSize, files_) - left[r];
                }
            }

            flowControl.ArrayAllReduce(split, splitsum, std::plus<ValueType>());

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] < 1) continue;
                
                if(widthsum[r] < srank[r])  {
                    left[r] += split[r];
                    width[r] -= split[r];
                    srank[r] -= splitsum[r];
                } else {
                    width[r] = split[r];
                }
            } 
        }

        //Cool, parts contains the global splittage now

       for(size_t i = 0; i < partitions.size(); i++) {
            LOG << "Part " << i << " " << partitions[i];
       } 
        
        //For now, do "trivial" scattering. 
        channels_[0] = context_.GetNewChannel();
        channels_[1] = context_.GetNewChannel();

        std::vector<size_t> offset1(context_.num_workers(), 0);  
        std::vector<size_t> offset2(context_.num_workers(), 0);  
        
        size_t sizes[] = { files_[0].NumItems(), files_[1].NumItems() };
        for (size_t i = me; i != offset1.size(); ++i) {
            offset1[i] = sizes[0];
            offset2[i] = sizes[1];
        }
    
        channels_[0]->template Scatter<ValueType>(files_[0], offset1);
        channels_[1]->template Scatter<ValueType>(files_[1], offset2);
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
            int,
            CompareResult
            >::value,
        "Comperator must return int");

    StatsNode* stats_node = AddChildStatsNode("Merge", NodeType::DOP);
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
