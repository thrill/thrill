%module(directors="1") thrill
%{

#include "thrill_java.hpp"

#include <thrill/core/job_manager.hpp>

using namespace thrill;

%}

%feature("director") GeneratorFunction;

%pragma(java) jniclasscode=%{
     static {
         try {
             System.loadLibrary("thrill");
         }
         catch (UnsatisfiedLinkError e) {
             System.err.println("Native code library failed to load.\n" + e);
             System.exit(1);
         }
     }
%}

// from http://stackoverflow.com/questions/8168517/generating-java-interface-with-swig

%rename(GeneratorFunctionNative) GeneratorFunction;

%typemap(jstype) const thrill::GeneratorFunction& "GeneratorFunction";
%typemap(javainterfaces) thrill::GeneratorFunction "GeneratorFunction"
%typemap(javain, pgcppname="n",
         pre="    GeneratorFunctionNative n = makeNative($javainput);")
     const thrill::GeneratorFunction&  "GeneratorFunctionNative.getCPtr(n)"

%pragma(java) modulecode=%{
  // (2.4)
  private static class GeneratorFunctionNativeProxy extends GeneratorFunctionNative {
    private GeneratorFunction delegate;
    public GeneratorFunctionNativeProxy(GeneratorFunction gf) {
      delegate = gf;
    }

    public String call(long i) {
      return delegate.call(i);
    }
  }

  // (2.5)
  private static GeneratorFunctionNative makeNative(GeneratorFunction i) {
    if (i instanceof GeneratorFunctionNative) {
      // If it already *is* a NativeInterface don't bother wrapping it again
      return (GeneratorFunctionNative)i;
    }
    return new GeneratorFunctionNativeProxy(i);
  }
%}

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

/* %feature("pythonprepend") thrill::Generate(Context&, GeneratorFunction&, size_t) */
/*    ARRAYHELPER(GeneratorFunction, generator_function) */

/* %feature("pythonprepend") thrill::PythonDIA::Map(MapFunction&) const */
/*    ARRAYHELPER(MapFunction, map_function) */

/* %feature("pythonprepend") thrill::PythonDIA::ReduceBy(KeyExtractorFunction&, ReduceFunction&) const %{ */
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
%include <std_shared_ptr.i>
%shared_ptr(thrill::core::JobManager)

%template(VectorJobManagerPtr) std::vector<std::shared_ptr<thrill::core::JobManager>>;

%include <thrill/core/job_manager.hpp>
%include <thrill/api/context.hpp>

/* %ignore thrill::PyObjectRef; */
/* %ignore thrill::data::Serialization; */

%include "thrill_java.hpp"
