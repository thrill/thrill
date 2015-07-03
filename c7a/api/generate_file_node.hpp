/*******************************************************************************
 * c7a/api/generate_file_node.hpp
 *
 * DIANode for a generate operation. Performs the actual generate operation
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_GENERATE_FILE_NODE_HEADER
#define C7A_API_GENERATE_FILE_NODE_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/api/dop_node.hpp>
#include <c7a/api/function_stack.hpp>

#include <string>
#include <fstream>
#include <random>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a GenerateFromFile operation. Generate uses a file
 * from the file system to generate random inputs. Therefore Generate reads the
 * complete file and applies the generator function on each element. Afterwards
 * each worker generates a DIA with a certain number of random (possibly
 * duplicate) elements from the generator file.
 *
 * \tparam Output Output type of the Generate operation.
 * \tparam ReadFunction Type of the generate function.
 */
template <typename Output, typename GeneratorFunction>
class GenerateFileNode : public DOpNode<Output>
{
public:
    using Super = DOpNode<Output>;
    using Super::context_;
    /*!
    * Constructor for a GeneratorNode. Sets the Context, parents, generator
    * function and file path.
    *
    * \param ctx Reference to Context, which holds references to data and network.
    * \param generator_function Generator function, which defines how each line
    * of the file is read and used for generation of a DIA.
    * \param path_in Path of the input file
    * \param size Number of elements in the generated DIA
    */
    GenerateFileNode(Context& ctx,
                     GeneratorFunction generator_function,
                     std::string path_in,
                     size_t size)
        : DOpNode<Output>(ctx, { }),
          generator_function_(generator_function),
          path_in_(path_in),
          size_(size)
    { }

    virtual ~GenerateFileNode() { }

    //! Executes the generate operation. Reads a file line by line and creates a
    //! element vector, out of which elements are randomly chosen (possibly
    //! duplicated).
    void execute() {

        LOG << "GENERATING data with id " << this->data_id_;

        std::ifstream file(path_in_);
        assert(file.good());

        std::string line;
        while (std::getline(file, line))
        {
            if (*line.rbegin() == '\r') {
                line.erase(line.length() - 1);
            }
            elements_.push_back(generator_function_(line));
        }

        size_t local_elements;
        if (context_.number_worker() == context_.rank() + 1) {
            //last worker gets leftovers
            local_elements = size_ -
                             ((context_.number_worker() - 1) *
                              (size_ / context_.number_worker()));
        }
        else {
            local_elements = (size_ / context_.number_worker());
        }

        std::random_device random_device;
        std::default_random_engine generator(random_device());
        std::uniform_int_distribution<int> distribution(0, elements_.size() - 1);

        for (size_t i = 0; i < local_elements; i++) {
            size_t rand_element = distribution(generator);
            for (auto func : DIANode<Output>::callbacks_) {
                func(elements_[rand_element]);
            }
        }
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](Output t, auto emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns information about the GeneratorNode as a string.
     * \return Stringified node.
     */
    std::string ToString() override {
        return "[GeneratorNode] Id: " + this->data_id_.ToString();
    }

private:
    //! The read function which is applied on every line read.
    GeneratorFunction generator_function_;
    //! Path of the input file.
    std::string path_in_;
    //! Element vector used for generation
    std::vector<Output> elements_;
    //! Size of the output DIA.
    size_t size_;

    static const bool debug = false;
};

//! \}

template <typename GeneratorFunction>
auto GenerateFromFile(Context & ctx, std::string filepath,
                      const GeneratorFunction &generator_function,
                      size_t size) {
    using GeneratorResult =
              typename FunctionTraits<GeneratorFunction>::result_type;
    using GenerateResultNode =
              GenerateFileNode<GeneratorResult, GeneratorFunction>;

    auto shared_node =
        std::make_shared<GenerateResultNode>(ctx,
                                             generator_function,
                                             filepath,
                                             size);

    auto generator_stack = shared_node->ProduceStack();

    return DIARef<GeneratorResult, decltype(generator_stack)>
               (std::move(shared_node), generator_stack);
}
}

} // namespace api

#endif // !C7A_API_GENERATE_FILE_NODE_HEADER

/******************************************************************************/
