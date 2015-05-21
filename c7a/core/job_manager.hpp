/*******************************************************************************
 * c7a/core/job_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_CORE_JOB_MANAGER_HEADER
#define C7A_CORE_JOB_MANAGER_HEADER

#include "c7a/data/data_manager.hpp"

//includes for thread and condition variables magic
#include <thread>
#include <mutex>
#include <condition_variable>

namespace c7a {
namespace core {
class JobManager
{
public:
    JobManager() : net_dispatcher_(), cmp_(net_dispatcher_), data_manager_(cmp_)
    {
        //OVERALL TODO(cn): Run Dispatcher in own thread

        //TODO(cn): find out how to run the dispatcher. Is it like this?
        //net_dispatcher_.Dispatch();

        //TODO(cn): now run it in an owen threeeead. like this?
        //std::thread(net_dispatcher_.Dispatch());
    }

    data::DataManager & get_data_manager()
    {
        return data_manager_;
    }

    //When a DIA calls HasNext() on the data manager but HasNext() returns false and IsClosed() returns false too, then the DIA needs to wait. Therefore, this function is needed
    void WaitOnData()
    {
        while (!new_data_arrived_) {
            //TODO(cn): Call on wait and as soon as data manager has data, it needs to signal the DIA to continue
            //Use Condition Variables to do this.
            std::unique_lock<std::mutex> locker(waiting_on_data_);
            idontknowhowtonameit_.wait(locker);

            //Internet is saying something about putting wait() into a while loop. find out why and whether that is needed
        }
    }

private:
    net::NetDispatcher net_dispatcher_;
    net::ChannelMultiplexer cmp_;
    data::DataManager data_manager_;
    std::mutex waiting_on_data_;
    std::condition_variable idontknowhowtonameit_;
    bool new_data_arrived_;
};
}  // namespace core
}  // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/
