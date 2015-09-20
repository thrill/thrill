/*******************************************************************************
 * swig/python/thrill_python.i
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

%module(directors="1") thrill
%{

#include "thrill_python.hpp"

using namespace thrill;

%}

// this makes python functions use *args instead of explicit arguments in swig3.
%feature("compactdefaultargs");

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

/*[[[cog
import cog

def callback_wrapper(func, arglist):
  cog.outl('%%feature("pythonprepend") %s %%{' % func)
  cog.outl('  wa = []')
  for i, arg in enumerate(arglist):
    if arg != '':
      cog.outl('  if not isinstance(args[%d], %s) and callable(args[0]):' % (i, arg))
      cog.outl('    class CallableWrapper(%s):' % arg)
      cog.outl('      def __init__(self, f):')
      cog.outl('        super(CallableWrapper, self).__init__()')
      cog.outl('        self.f_ = f')
      cog.outl('      def __call__(self, *args):')
      cog.outl('        return self.f_(*args)')
      cog.outl('    wa.append(CallableWrapper(args[%d]))' % i)
      cog.outl('  else:')
      cog.outl('    wa.append(args[%d])' % i)
    else:
      cog.outl('  wa.append(args[%d])' % i)
  cog.outl('  args = tuple(wa)')
  cog.outl('%}')

callback_wrapper('thrill::PyContext::Generate(GeneratorFunction&, size_t)',
                 ['GeneratorFunction', ''])

callback_wrapper('thrill::PyDIA::Map(MapFunction&) const',
                 ['MapFunction'])

callback_wrapper('thrill::PyDIA::Filter(FilterFunction&) const',
                 ['FilterFunction'])

  ]]]*/
%feature("pythonprepend") thrill::PyContext::Generate(GeneratorFunction&, size_t) %{
  wa = []
  if not isinstance(args[0], GeneratorFunction) and callable(args[0]):
    class CallableWrapper(GeneratorFunction):
      def __init__(self, f):
        super(CallableWrapper, self).__init__()
        self.f_ = f
      def __call__(self, *args):
        return self.f_(*args)
    wa.append(CallableWrapper(args[0]))
  else:
    wa.append(args[0])
  wa.append(args[1])
  args = tuple(wa)
%}
%feature("pythonprepend") thrill::PyDIA::Map(MapFunction&) const %{
  wa = []
  if not isinstance(args[0], MapFunction) and callable(args[0]):
    class CallableWrapper(MapFunction):
      def __init__(self, f):
        super(CallableWrapper, self).__init__()
        self.f_ = f
      def __call__(self, *args):
        return self.f_(*args)
    wa.append(CallableWrapper(args[0]))
  else:
    wa.append(args[0])
  args = tuple(wa)
%}
%feature("pythonprepend") thrill::PyDIA::Filter(FilterFunction&) const %{
  wa = []
  if not isinstance(args[0], FilterFunction) and callable(args[0]):
    class CallableWrapper(FilterFunction):
      def __init__(self, f):
        super(CallableWrapper, self).__init__()
        self.f_ = f
      def __call__(self, *args):
        return self.f_(*args)
    wa.append(CallableWrapper(args[0]))
  else:
    wa.append(args[0])
  args = tuple(wa)
%}
// [[[end]]]


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

%feature("pythonprepend") thrill::PyDIA::ReduceBy(KeyExtractorFunction&, ReduceFunction&) const
CallbackHelper2(KeyExtractorFunction, key_extractor, ReduceFunction, reduce_function)

%include <std_string.i>
%include <std_vector.i>
%include <std_shared_ptr.i>

%shared_ptr(thrill::PyContext)
%shared_ptr(thrill::api::Context)

%template(VectorPyObject) std::vector<PyObject*>;
%template(VectorString) std::vector<std::string>;

%template(VectorPyContext) std::vector<std::shared_ptr<thrill::PyContext> >;

// ignore all Context methods: forward them via PyContext if they should be
// available.
%ignore thrill::api::Context;

// ignore constructor
%ignore thrill::api::HostContext::HostContext;

%feature("pythonappend") thrill::PyContext::PyContext(HostContext&, size_t) %{
    # acquire a reference to the HostContext
    self._host_context = host_context
    %}

%include <thrill/api/context.hpp>
%include "thrill_python.hpp"

// Local Variables:
// mode: c++
// mode: mmm
// End:

/******************************************************************************/
