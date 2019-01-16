/*******************************************************************************
 * thrill/api/window.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WINDOW_HEADER
#define THRILL_API_WINDOW_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/ring_buffer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename Input,
          typename WindowFunction, typename PartialWindowFunction>
class BaseWindowNode : public DOpNode<ValueType>
{
protected:
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    //! RingBuffer used and passed to user-defined function.
    using RingBuffer = common::RingBuffer<Input>;

public:
    template <typename ParentDIA>
    BaseWindowNode(const ParentDIA& parent,
                   const char* label, size_t window_size,
                   const WindowFunction& window_function,
                   const PartialWindowFunction& partial_window_function)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }),
          parent_stack_empty_(ParentDIA::stack_empty),
          window_size_(window_size),
          window_function_(window_function),
          partial_window_function_(partial_window_function) {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const Input& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        return window_size_ * sizeof(Input);
    }

    void StartPreOp(size_t /* parent_index */) final {
        window_.allocate(window_size_);
    }

    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOGC(common::g_debug_push_file)
                << "Window rejected File from parent "
                << "due to non-empty function stack.";
            return false;
        }
        // accept file
        assert(file_.num_items() == 0);
        file_ = file.Copy();
        if (file_.num_items() != 0) {
            // read last k - 1 items from File
            size_t pos = file_.num_items() > window_size_ - 1 ?
                         file_.num_items() - window_size_ + 1 : 0;
            auto reader = file_.GetReaderAt<Input>(pos);
            while (reader.HasNext())
                window_.push_back(reader.template Next<Input>());
        }
        return true;
    }

    //! PreOp: keep last k - 1 items (local window) and store items.
    void PreOp(const Input& input) {
        if (window_.size() >= window_size_ - 1)
            window_.pop_front();
        window_.push_back(input);

        writer_.Put(input);
    }

    void StopPreOp(size_t /* parent_index */) final {
        writer_.Close();
    }

    DIAMemUse PushDataMemUse() final {
        // window_ is copied in PushData()
        return 2 * window_size_ * sizeof(Input);
    }

    void Dispose() final {
        window_.deallocate();
        file_.Clear();
    }

protected:
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;
    //! Size k of the window
    size_t window_size_;
    //! The window function which is applied to k elements.
    WindowFunction window_function_;
    //! The window function which is applied to the last < k elements.
    PartialWindowFunction partial_window_function_;

    //! cache the last k - 1 items for transmission
    RingBuffer window_;

    //! Local data file
    data::File file_ { context_.GetFile(this) };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ { file_.GetWriter() };

    //! rank of our first element in file_
    size_t first_rank_;
};

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename Input,
          typename WindowFunction, typename PartialWindowFunction>
class OverlapWindowNode final
    : public BaseWindowNode<
          ValueType, Input, WindowFunction, PartialWindowFunction>
{
    using Super = BaseWindowNode<
        ValueType, Input, WindowFunction, PartialWindowFunction>;
    using Super::debug;
    using Super::context_;

    using typename Super::RingBuffer;

public:
    template <typename ParentDIA>
    OverlapWindowNode(const ParentDIA& parent,
                      const char* label, size_t window_size,
                      const WindowFunction& window_function,
                      const PartialWindowFunction& partial_window_function)
        : Super(parent, label, window_size,
                window_function, partial_window_function) { }

    //! Executes the window operation by receiving k - 1 items from our
    //! preceding worker.
    void Execute() final {
        // get rank of our first element
        first_rank_ = context_.net.ExPrefixSum(file_.num_items());

        // copy our last elements into a vector
        std::vector<Input> my_last;
        my_last.reserve(window_size_ - 1);

        assert(window_.size() < window_size_);
        window_.move_to(&my_last);

        // collective operation: get k - 1 predecessors
        std::vector<Input> pre =
            context_.net.Predecessor(window_size_ - 1, my_last);

        sLOG << "Window::MainOp()"
             << "first_rank_" << first_rank_
             << "window_size_" << window_size_
             << "pre.size()" << pre.size();

        assert(pre.size() == std::min(window_size_ - 1, first_rank_));

        // put k - 1 predecessors back into window_
        for (size_t i = 0; i < pre.size(); ++i)
            window_.push_back(pre[i]);
    }

    void PushData(bool consume) final {
        data::File::Reader reader = file_.GetReader(consume);

        // copy window ring buffer containing first items
        RingBuffer window = window_;
        // this may wrap around, but that is okay. -tb
        size_t rank = first_rank_ - (window_size_ - 1);

        size_t num_items = file_.num_items();

        sLOG << "WindowNode::PushData()"
             << "window.size()" << window.size()
             << "first_rank_" << first_rank_
             << "rank" << rank
             << "num_items" << num_items;

        for (size_t i = 0; i < num_items; ++i, ++rank) {
            // append an item.
            window.emplace_back(reader.Next<Input>());

            // only issue full window frames
            if (window.size() != window_size_) continue;

            // call window user-defined function
            window_function_(
                rank, window, [this](const ValueType& output) {
                    this->PushItem(output);
                });

            // return to window size - 1
            if (window.size() >= window_size_ - 1)
                window.pop_front();
        }

        if (context_.my_rank() == context_.num_workers() - 1) {
            if (window.size() < window_size_ - 1)
                rank = 0;
            while (window.size()) {
                partial_window_function_(
                    rank, window, [this](const ValueType& output) {
                        this->PushItem(output);
                    });
                ++rank;
                window.pop_front();
            }
        }
    }

private:
    using Super::file_;
    using Super::first_rank_;
    using Super::window_;
    using Super::window_size_;
    using Super::window_function_;
    using Super::partial_window_function_;
};

template <typename ValueType, typename Stack>
template <typename ValueOut,
          typename WindowFunction, typename PartialWindowFunction>
auto DIA<ValueType, Stack>::FlatWindow(
    size_t window_size, const WindowFunction& window_function,
    const PartialWindowFunction& partial_window_function) const {
    assert(IsValid());

    using WindowNode = api::OverlapWindowNode<
        ValueOut, ValueType, WindowFunction, PartialWindowFunction>;

    // cannot check WindowFunction's arguments, since it is a template methods
    // due to the auto emitter.

    auto node = tlx::make_counting<WindowNode>(
        *this, "FlatWindow", window_size,
        window_function, partial_window_function);

    return DIA<ValueOut>(node);
}

template <typename ValueType, typename Stack>
template <typename ValueOut, typename WindowFunction>
auto DIA<ValueType, Stack>::FlatWindow(
    size_t window_size, const WindowFunction& window_function) const {
    assert(IsValid());

    auto no_operation_function =
        [](size_t /* index */,
           const common::RingBuffer<ValueType>& /* window */,
           auto /* emit */) { };

    return FlatWindow<ValueOut>(
        window_size, window_function, no_operation_function);
}

template <typename ValueType, typename Stack>
template <typename WindowFunction>
auto DIA<ValueType, Stack>::Window(
    size_t window_size, const WindowFunction& window_function) const {
    assert(IsValid());

    using Result
        = typename FunctionTraits<WindowFunction>::result_type;

    static_assert(
        std::is_convertible<
            size_t,
            typename FunctionTraits<WindowFunction>::template arg<0>
            >::value,
        "WindowFunction's first argument must be size_t (index)");

    static_assert(
        std::is_convertible<
            common::RingBuffer<ValueType>,
            typename FunctionTraits<WindowFunction>::template arg<1>
            >::value,
        "WindowFunction's second argument must be common::RingBuffer<T>");

    // transform Map-like function into FlatMap-like function
    auto flatwindow_function =
        [window_function](size_t index,
                          const common::RingBuffer<ValueType>& window,
                          auto emit) {
            emit(window_function(index, window));
        };

    auto no_operation_function =
        [](size_t /* index */,
           const common::RingBuffer<ValueType>& /* window */,
           auto /* emit */) { };

    using WindowNode = api::OverlapWindowNode<
        Result, ValueType,
        decltype(flatwindow_function), decltype(no_operation_function)>;

    auto node = tlx::make_counting<WindowNode>(
        *this, "Window", window_size,
        flatwindow_function, no_operation_function);

    return DIA<Result>(node);
}

template <typename ValueType, typename Stack>
template <typename WindowFunction, typename PartialWindowFunction>
auto DIA<ValueType, Stack>::Window(
    size_t window_size, const WindowFunction& window_function,
    const PartialWindowFunction& partial_window_function) const {
    assert(IsValid());

    using Result
        = typename FunctionTraits<WindowFunction>::result_type;

    static_assert(
        std::is_convertible<
            size_t,
            typename FunctionTraits<WindowFunction>::template arg<0>
            >::value,
        "WindowFunction's first argument must be size_t (index)");

    static_assert(
        std::is_convertible<
            common::RingBuffer<ValueType>,
            typename FunctionTraits<WindowFunction>::template arg<1>
            >::value,
        "WindowFunction's second argument must be common::RingBuffer<T>");

    // transform Map-like function into FlatMap-like function
    auto flatwindow_function =
        [window_function](size_t index,
                          const common::RingBuffer<ValueType>& window,
                          auto emit) {
            emit(window_function(index, window));
        };

    // transform Map-like function into FlatMap-like function
    auto flatwindow_partial_function =
        [partial_window_function](size_t index,
                                  const common::RingBuffer<ValueType>& window,
                                  auto emit) {
            emit(partial_window_function(index, window));
        };

    using WindowNode = api::OverlapWindowNode<
        Result, ValueType,
        decltype(flatwindow_function), decltype(flatwindow_partial_function)>;

    auto node = tlx::make_counting<WindowNode>(
        *this, "Window", window_size,
        flatwindow_function, flatwindow_partial_function);

    return DIA<Result>(node);
}

/******************************************************************************/

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename Input,
          typename WindowFunction, typename PartialWindowFunction>
class DisjointWindowNode final
    : public BaseWindowNode<
          ValueType, Input, WindowFunction, PartialWindowFunction>
{
    using Super = BaseWindowNode<
        ValueType, Input, WindowFunction, PartialWindowFunction>;
    using Super::debug;
    using Super::context_;

    using typename Super::RingBuffer;

public:
    template <typename ParentDIA>
    DisjointWindowNode(const ParentDIA& parent,
                       const char* label, size_t window_size,
                       const WindowFunction& window_function,
                       const PartialWindowFunction& partial_window_function)
        : Super(parent, label, window_size,
                window_function, partial_window_function) { }

    //! Executes the window operation by receiving k - 1 items from our
    //! preceding worker.
    void Execute() final {
        // get rank of our first element
        first_rank_ = context_.net.ExPrefixSum(file_.num_items());

        // copy our last elements into a vector
        std::vector<Input> my_last;
        my_last.reserve(window_size_ - 1);

        assert(window_.size() < window_size_);
        window_.move_to(&my_last);

        // collective operation: get k - 1 predecessors
        std::vector<Input> pre =
            context_.net.Predecessor(window_size_ - 1, my_last);

        assert(pre.size() == std::min(window_size_ - 1, first_rank_));

        // calculate how many (up to  k - 1) predecessors to put into window_

        size_t fill_size = first_rank_ % window_size_;

        sLOG << "Window::MainOp()"
             << "first_rank_" << first_rank_
             << "file_.size()" << file_.num_items()
             << "window_size_" << window_size_
             << "pre.size()" << pre.size()
             << "fill_size" << fill_size;

        assert(first_rank_ < window_size_ ||
               (first_rank_ - fill_size) % window_size_ == 0);

        // put those predecessors into window_ for PushData() to start with.
        for (size_t i = pre.size() - fill_size; i < pre.size(); ++i)
            window_.push_back(pre[i]);
    }

    void PushData(bool consume) final {
        data::File::Reader reader = file_.GetReader(consume);

        // copy window into vector containing first items
        std::vector<Input> window;
        window.reserve(window_size_);
        window_.copy_to(&window);
        assert(window.size() < window_size_);

        size_t rank = first_rank_ - (window_size_ - 1);
        size_t num_items = file_.num_items();

        sLOG << "WindowNode::PushData()"
             << "window.size()" << window.size()
             << "rank" << rank
             << "rank+window+1" << (rank + window.size() + 1)
             << "num_items" << num_items;

        for (size_t i = 0; i < num_items; ++i, ++rank) {
            // append an item.
            window.emplace_back(reader.Next<Input>());

            sLOG << "rank" << rank << "window.size()" << window.size();

            // only issue full window frames
            if (window.size() != window_size_) continue;

            // call window user-defined function
            window_function_(
                rank, window, [this](const ValueType& output) {
                    this->PushItem(output);
                });

            // clear window
            window.clear();
        }

        // call user-defined function for last incomplete window
        if (context_.my_rank() == context_.num_workers() - 1 &&
            window.size() != 0)
        {
            rank += window_size_ - window.size() - 1;
            partial_window_function_(
                rank, window, [this](const ValueType& output) {
                    this->PushItem(output);
                });
        }
    }

private:
    using Super::file_;
    using Super::first_rank_;
    using Super::window_;
    using Super::window_size_;
    using Super::window_function_;
    using Super::partial_window_function_;
};

template <typename ValueType, typename Stack>
template <typename ValueOut, typename WindowFunction>
auto DIA<ValueType, Stack>::FlatWindow(
    struct DisjointTag const&, size_t window_size,
    const WindowFunction& window_function) const {
    assert(IsValid());

    using WindowNode = api::DisjointWindowNode<
        ValueOut, ValueType, WindowFunction, WindowFunction>;

    // cannot check WindowFunction's arguments, since it is a template methods
    // due to the auto emitter.

    auto node = tlx::make_counting<WindowNode>(
        *this, "FlatWindow", window_size, window_function, window_function);

    return DIA<ValueOut>(node);
}

template <typename ValueType, typename Stack>
template <typename WindowFunction>
auto DIA<ValueType, Stack>::Window(
    struct DisjointTag const&, size_t window_size,
    const WindowFunction& window_function) const {
    assert(IsValid());

    using Result
        = typename FunctionTraits<WindowFunction>::result_type;

    static_assert(
        std::is_convertible<
            size_t,
            typename FunctionTraits<WindowFunction>::template arg<0>
            >::value,
        "WindowFunction's first argument must be size_t (index)");

    static_assert(
        std::is_convertible<
            std::vector<ValueType>,
            typename FunctionTraits<WindowFunction>::template arg<1>
            >::value,
        "WindowFunction's second argument must be std::vector<T>");

    // transform Map-like function into FlatMap-like function
    auto flatwindow_function =
        [window_function](size_t index,
                          const std::vector<ValueType>& window,
                          auto emit) {
            emit(window_function(index, window));
        };

    using WindowNode = api::DisjointWindowNode<
        Result, ValueType,
        decltype(flatwindow_function), decltype(flatwindow_function)>;

    auto node = tlx::make_counting<WindowNode>(
        *this, "Window", window_size, flatwindow_function, flatwindow_function);

    return DIA<Result>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WINDOW_HEADER

/******************************************************************************/
