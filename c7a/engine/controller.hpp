/*******************************************************************************
 * c7a/engine/controller.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_ENGINE_CONTROLLER_HEADER
#define C7A_ENGINE_CONTROLLER_HEADER

#include "stage_builder.hpp"
#include <c7a/common/logger.hpp>
#include <functional>
#include <vector>

namespace c7a {

namespace engine {

//todo make it virtual!
template <typename T, typename K, typename V>
class Controller
{
public:
    Controller(int id, const std::vector<int>& otherWorkers) :
        _id(id), _otherWorkers(otherWorkers) { }

    Controller() { }

    //    template <typename K, V>
    // This is the final reduce
    T reduce(std::function<T(T, T)> ReduceFn)
    {
        T res;
        // Iterate over
        for (auto it = _data.getIt(); it.hasNext(); it = it.next())
            res = ReduceFn(*it, res);
        // Alternativ<F3>
        for (auto x : _data)
            res = ReduceFn(x, res);
    }

    void ExecuteScope(DIABase* action)
    {
        std::vector<Stage> result;
        FindStages(action, result);
        for (auto s : result)
        {
            s.Run();
        }
    }

    void populateData(std::vector<std::pair<K, V> >) { }

private:
    int _id;
    // The worker needs to know the ids of all other workers
    std::vector<int> _otherWorkers;
    std::vector<std::pair<K, V> > _data;
    // First reduce with reduce() function
    //
    // Shuffle
    //
    // Second reduce with reduce() function

    void shuffle()
    {
        //
    }
};

/*
class MasterController : public Controller
{
public:
    void foo();
};

class WorkerController : public Controller
{
public:
    void foo();
};
*/

} // namespace engine

} // namespace c7a

#endif // !C7A_ENGINE_CONTROLLER_HEADER

/******************************************************************************/
