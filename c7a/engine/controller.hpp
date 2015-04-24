/*******************************************************************************
 * c7a/engine/controller.hpp
 *
 ******************************************************************************/

#ifndef C7A_ENGINE_CONTROLLER_HEADER
#define C7A_ENGINE_CONTROLLER_HEADER

namespace c7a {
namespace engine {

//todo make it virtual!
class Controller
{
public:
    void foo();
    //todo take exec graph and execute it
};

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

}
}

#endif // !C7A_ENGINE_CONTROLLER_HEADER

/******************************************************************************/
