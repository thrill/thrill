%module(directors="1") thrill
%{

#include "thrill_python.hpp"

using namespace thrill;

%}

%feature("director") GeneratorFunction;

%feature("director") MapFunction;
%feature("director") FilterFunction;

%feature("director") KeyExtractorFunction;
%feature("director") ReduceFunction;

%feature("director:except") {
    if ($error != NULL) {
        // print backtrace
        PyErr_PrintEx(0);
        throw Swig::DirectorMethodException();
    }
}

%define CallbackHelper(FunctionType,function_var) %{
   if not isinstance(function_var, FunctionType) and callable(function_var):
      class CallableWrapper(FunctionType):
         def __init__(self, f):
            super(CallableWrapper, self).__init__()
            self.f_ = f
         def __call__(self, *args):
            return self.f_(*args)
      function_var = CallableWrapper(function_var)
%}
%enddef

%define CallbackHelper2(FunctionType1,function_var1,FunctionType2,function_var2) %{
   if not isinstance(function_var1, FunctionType1) and callable(function_var1):
      class CallableWrapper(FunctionType1):
         def __init__(self, f):
            super(CallableWrapper, self).__init__()
            self.f_ = f
         def __call__(self, *args):
            return self.f_(*args)
      function_var1 = CallableWrapper(function_var1)
   if not isinstance(function_var2, FunctionType2) and callable(function_var2):
      class CallableWrapper(FunctionType2):
         def __init__(self, f):
            super(CallableWrapper, self).__init__()
            self.f_ = f
         def __call__(self, *args):
            return self.f_(*args)
      function_var2 = CallableWrapper(function_var2)
%}
%enddef

%feature("pythonprepend") thrill::PyContext::Generate(GeneratorFunction&, size_t)
   CallbackHelper(GeneratorFunction, generator_function)

%feature("pythonprepend") thrill::PyDIA::Map(MapFunction&) const
   CallbackHelper(MapFunction, map_function)

%feature("pythonprepend") thrill::PyDIA::Filter(FilterFunction&) const
   CallbackHelper(FilterFunction, filter_function)

%feature("pythonprepend") thrill::PyDIA::ReduceBy(KeyExtractorFunction&, ReduceFunction&) const
CallbackHelper2(KeyExtractorFunction, key_extractor, ReduceFunction, reduce_function)

%include <std_string.i>
%include <std_vector.i>
%include <std_shared_ptr.i>

%shared_ptr(thrill::PyContext)
%shared_ptr(thrill::api::Context)

%template(VectorPyObject) std::vector<PyObject*>;
%template(VectorString) std::vector<std::string>;

%template(VectorPyContext) std::vector<std::shared_ptr<thrill::PyContext>>;

%ignore thrill::api::HostContext::ConstructLocalMock;

// ignore all Context methods: forward them via PyContext if they should be
// available.
%ignore thrill::api::Context;

%feature("pythonappend") thrill::PyContext::PyContext(HostContext&, size_t) %{
    # acquire a reference to the HostContext
    self._host_context = host_context
    %}

%include <thrill/api/context.hpp>
%include "thrill_python.hpp"
