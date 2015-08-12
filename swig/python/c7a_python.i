%module(directors="1") c7a
%{

#include "c7a_python.hpp"

using namespace c7a;

%}

%feature("director") GeneratorFunction;
%feature("director") MapFunction;
%feature("director") KeyExtractorFunction;
%feature("director") ReduceFunction;

%define ARRAYHELPER(FunctionType,function_var) %{
   if not isinstance(function_var, FunctionType) and callable(function_var):
      class CallableWrapper(FunctionType):
         def __init__(self, f):
            super(CallableWrapper, self).__init__()
            self.f_ = f
         def __call__(self, index):
            return self.f_(index)
      function_var = CallableWrapper(function_var)
%}
%enddef

%feature("pythonprepend") c7a::Generate(Context&, GeneratorFunction&, size_t)
   ARRAYHELPER(GeneratorFunction, generator_function)

%feature("pythonprepend") c7a::PyDIA::Map(MapFunction&) const
   ARRAYHELPER(MapFunction, map_function)

%feature("pythonprepend") c7a::PyDIA::ReduceBy(KeyExtractorFunction&, ReduceFunction&) const %{
   if not isinstance(key_extractor, KeyExtractorFunction) and callable(key_extractor):
      class CallableWrapper(KeyExtractorFunction):
         def __init__(self, f):
            super(CallableWrapper, self).__init__()
            self.f_ = f
         def __call__(self, obj):
            return self.f_(obj)
      key_extractor = CallableWrapper(key_extractor)

   if not isinstance(reduce_function, ReduceFunction) and callable(reduce_function):
      class CallableWrapper(ReduceFunction):
         def __init__(self, f):
            super(CallableWrapper, self).__init__()
            self.f_ = f
         def __call__(self, obj):
            return self.f_(obj)
      reduce_function = CallableWrapper(reduce_function)
%}

%include <std_string.i>
%include <std_vector.i>
%include <std_shared_ptr.i>

%shared_ptr(c7a::PyContext)
%shared_ptr(c7a::api::Context)
%template(VectorPyContext) std::vector<std::shared_ptr<c7a::PyContext>>;

%ignore c7a::PyObjectRef;
%ignore c7a::data::Serialization;

%ignore c7a::api::HostContext::ConstructLocalMock;

%include <c7a/api/context.hpp>
%include "c7a_python.hpp"
