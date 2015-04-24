/*******************************************************************************
 * c7a/api/dia.hpp
 *
 * Functions to build stages
 ******************************************************************************/


#ifndef C7A_ENGINE_STAGE_BUILD
#define C7A_ENGINE_STAGE_BUILD

#include "../common/logger.hpp"

namespace c7a { namespace engine {

std::vector<Stage> BuildStages(DIABase *scope_root);

class Stage
{
public:
    void run()
    {
        SpacingLogger() << "I'm executing the stage.";
    };
private:
    void bar();
}

}}
// !C7A_ENGINE_STAGE_BUILD

/******************************************************************************/
