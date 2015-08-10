%module(directors="1") c7a
%{

#undef Zero
#undef seed
#include "c7a_perl5.hpp"

#include <c7a/core/job_manager.hpp>

using namespace c7a;

%}

%feature("director") GeneratorFunction;

/* %define ARRAYHELPER(FunctionType,function_var) %{ */
/*    if not isinstance(function_var, FunctionType) and callable(function_var): */
/*       class CallableWrapper(FunctionType): */
/*          def __init__(self, f): */
/*             super(CallableWrapper, self).__init__() */
/*             self.f_ = f */
/*          def __call__(self, index): */
/*             return self.f_(index) */
/*       function_var = CallableWrapper(function_var) */
/* %} */
/* %enddef */

/* %feature("pythonprepend") c7a::Generate(Context&, GeneratorFunction&, size_t) */
/*    ARRAYHELPER(GeneratorFunction, generator_function) */

/* %feature("pythonprepend") c7a::PythonDIA::Map(MapFunction&) const */
/*    ARRAYHELPER(MapFunction, map_function) */

/* %feature("pythonprepend") c7a::PythonDIA::ReduceBy(KeyExtractorFunction&, ReduceFunction&) const %{ */
/*    if not isinstance(key_extractor, KeyExtractorFunction) and callable(key_extractor): */
/*       class CallableWrapper(KeyExtractorFunction): */
/*          def __init__(self, f): */
/*             super(CallableWrapper, self).__init__() */
/*             self.f_ = f */
/*          def __call__(self, obj): */
/*             return self.f_(obj) */
/*       key_extractor = CallableWrapper(key_extractor) */

/*    if not isinstance(reduce_function, ReduceFunction) and callable(reduce_function): */
/*       class CallableWrapper(ReduceFunction): */
/*          def __init__(self, f): */
/*             super(CallableWrapper, self).__init__() */
/*             self.f_ = f */
/*          def __call__(self, obj): */
/*             return self.f_(obj) */
/*       reduce_function = CallableWrapper(reduce_function) */
/* %} */

%include <std_string.i>
%include <std_vector.i>
/* %include <std_shared_ptr.i> */
/* %shared_ptr(c7a::core::JobManager) */

/* %template(VectorJobManagerPtr) std::vector<std::shared_ptr<c7a::core::JobManager>>; */

/* %include <c7a/core/job_manager.hpp> */
/* %include <c7a/api/context.hpp> */

/* %ignore c7a::PyObjectRef; */
/* %ignore c7a::data::Serialization; */

%include "c7a_perl5.hpp"
