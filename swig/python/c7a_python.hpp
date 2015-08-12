/*******************************************************************************
 * swig/python/c7a_python.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_SWIG_PYTHON_C7A_PYTHON_HEADER
#define C7A_SWIG_PYTHON_C7A_PYTHON_HEADER

#include <c7a/api/allgather.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/distribute.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/collapse.hpp>
#include <c7a/api/cache.hpp>
#include <c7a/api/reduce.hpp>
#include <c7a/api/size.hpp>
#include <c7a/common/string.hpp>

#include <Python.h>
#include <bytesobject.h>
#include <marshal.h>

#include <string>
#include <vector>

namespace c7a {

#ifndef SWIG

static const bool debug = true;

/*!
 * This class holds a PyObject* and a reference count on the enclosed
 * object. PyDIAs contain exclusively items of this class.
 */
class PyObjectRef
{
public:
    PyObjectRef()
        : obj_(0)
    { }

    PyObjectRef(const PyObjectRef& pyref)
        : obj_(pyref.obj_) {
        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        Py_XINCREF(obj_);
        SWIG_PYTHON_THREAD_END_BLOCK;
    }

    explicit PyObjectRef(PyObject* obj, bool initial_ref = true)
        : obj_(obj) {
        if (initial_ref) {
            SWIG_PYTHON_THREAD_BEGIN_BLOCK;
            Py_XINCREF(obj_);
            SWIG_PYTHON_THREAD_END_BLOCK;
        }
    }

    PyObjectRef& operator = (const PyObjectRef& pyref) {
        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        Py_XINCREF(pyref.obj_);
        Py_XDECREF(obj_);
        obj_ = pyref.obj_;
        SWIG_PYTHON_THREAD_END_BLOCK;
        return *this;
    }

    ~PyObjectRef() {
        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        Py_XDECREF(obj_);
        SWIG_PYTHON_THREAD_END_BLOCK;
    }

    PyObject * get() const {
        return obj_;
    }

    void incref() const {
        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        Py_XINCREF(obj_);
        SWIG_PYTHON_THREAD_END_BLOCK;
    }

    PyObject * get_incref() const {
        incref();
        return get();
    }

    PyObject* operator -> () const {
        return obj_;
    }

    bool operator == (const PyObjectRef& other) const {
        if (get() == nullptr || other.get() == nullptr)
            return get() == other.get();
        return PyObject_RichCompareBool(get(), other.get(), Py_EQ);
    }

    bool operator < (const PyObjectRef& other) const {
        return PyObject_RichCompareBool(get(), other.get(), Py_LT);
    }

protected:
    PyObject* obj_;
};

#endif

typedef swig::SwigVar_PyObject PyObjectVarRef;

#ifndef SWIG

namespace data {

/*!
 * c7a serialization interface for PyObjects: call the PyMarshal C API.
 */
template <typename Archive>
struct Serialization<Archive, PyObjectRef>
{
    static void Serialize(const PyObjectRef& obj, Archive& ar) {
        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        PyObject* mar =
            PyMarshal_WriteObjectToString(obj.get(), Py_MARSHAL_VERSION);

        char* data;
        Py_ssize_t len;

        PyBytes_AsStringAndSize(mar, &data, &len);
        if (debug)
            sLOG0 << "Serialized:" << common::hexdump(data, len);

        ar.PutVarint(len).Append(data, len);
        Py_DECREF(mar);
        SWIG_PYTHON_THREAD_END_BLOCK;
    }
    static PyObjectRef Deserialize(Archive& ar) {
        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        std::string data = ar.Read(ar.GetVarint());
        PyObjectRef obj(
            PyMarshal_ReadObjectFromString(
                const_cast<char*>(data.data()), data.size()));
        SWIG_PYTHON_THREAD_END_BLOCK;
        return obj;
    }
    static const bool   is_fixed_size = false;
    static const size_t fixed_size = 0;
};

} // namespace data

#endif

class GeneratorFunction
{
public:
    virtual ~GeneratorFunction() { }
    virtual PyObjectVarRef operator () (size_t index) = 0;
};

class MapFunction
{
public:
    virtual ~MapFunction() { }
    virtual PyObjectVarRef operator () (PyObject* obj) = 0;
};

class FilterFunction
{
public:
    virtual ~FilterFunction() { }
    virtual bool operator () (PyObject* obj) = 0;
};

class KeyExtractorFunction
{
public:
    virtual ~KeyExtractorFunction() { }
    virtual PyObjectVarRef operator () (PyObject* obj) = 0;
};

class ReduceFunction
{
public:
    virtual ~ReduceFunction() { }
    virtual PyObjectVarRef operator () (PyObject* obj1, PyObject* obj2) = 0;
};

} // namespace c7a

// import Swig Director classes.
#include "c7a_pythonPYTHON_wrap.h"

#ifndef SWIG

// TODO: this should not be used, parameterize our code to use a HashFunction.
namespace std {
template <>
struct hash<c7a::PyObjectRef>
    : public std::unary_function<c7a::PyObjectRef, size_t>{
    std::size_t operator () (const c7a::PyObjectRef& ob) const {
        auto h = PyObject_Hash(ob.get());
        if (h == -1) {
            throw std::exception();
        }
        return h;
    }
};

} // namespace std

#endif

namespace c7a {

//! all DIAs used in the python code contain PyObjectRefs, which are reference
//! counted PyObjects.
typedef api::DIARef<PyObjectRef> PyDIARef;

/*!
 * This is a wrapper around the C++ DIARef class, which returns plain PyDIAs
 * again. The C++ function stack is always collapsed.
 */
class PyDIA
{
    static const bool debug = false;

public:
    //! underlying C++ DIARef class, which can be freely copied by the object.
    PyDIARef dia_;

    explicit PyDIA(const PyDIARef& dia)
        : dia_(dia) {
        sLOG << "create PyDIA" << this;
    }

    ~PyDIA() {
        sLOG << "delete PyDIA" << this;
    }

    PyDIA Map(MapFunction& map_function) const {
        assert(dia_.IsValid());

        // the object MapFunction is actually an instance of the Director
        SwigDirector_MapFunction& director =
            *dynamic_cast<SwigDirector_MapFunction*>(&map_function);

        return PyDIA(
            dia_.Map(
                [&map_function,
                 // this holds a reference count to the callback object for the
                 // lifetime of the capture object.
                 ref = PyObjectRef(director.swig_get_self())
                ](const PyObjectRef& obj) {
                 // increase reference count, since calling the map_function
                 // implicitly passed ownership of the reference to the
                 // caller.
                    return PyObjectRef(map_function(obj.get_incref()), true);
                })
            .Collapse());
    }

    PyDIA Filter(FilterFunction& filter_function) const {
        assert(dia_.IsValid());

        // the object FilterFunction is actually an instance of the Director
        SwigDirector_FilterFunction& director =
            *dynamic_cast<SwigDirector_FilterFunction*>(&filter_function);

        return PyDIA(
            dia_.Filter(
                [&filter_function,
                 // this holds a reference count to the callback object for the
                 // lifetime of the capture object.
                 ref = PyObjectRef(director.swig_get_self())
                ](const PyObjectRef& obj) {
                 // increase reference count, since calling the filter_function
                 // implicitly passed ownership of the reference to the
                 // caller.
                    return filter_function(obj.get_incref());
                })
            .Collapse());
    }

    PyDIA ReduceBy(KeyExtractorFunction& key_extractor,
                       ReduceFunction& reduce_function) const {
        assert(dia_.IsValid());

        SwigDirector_KeyExtractorFunction& director1 =
            *dynamic_cast<SwigDirector_KeyExtractorFunction*>(&key_extractor);
        SwigDirector_ReduceFunction& director2 =
            *dynamic_cast<SwigDirector_ReduceFunction*>(&reduce_function);

        return PyDIA(
            dia_.ReduceBy(
                [&key_extractor,
                 // this holds a reference count to the callback object for the
                 // lifetime of the capture object.
                 ref1 = PyObjectRef(director1.swig_get_self())
                ](const PyObjectRef& obj) -> PyObjectRef {
                 // increase reference count, since calling the map_function
                 // implicitly passed ownership of the reference to the
                 // caller.
                    return PyObjectRef(key_extractor(obj.get_incref()), true);
                },
                [&reduce_function,
                 // this holds a reference count to the callback object for the
                 // lifetime of the capture object.
                 ref2 = PyObjectRef(director2.swig_get_self())
                ](const PyObjectRef& obj1, const PyObjectRef& obj2) -> PyObjectRef {
                 // increase reference count, since calling the map_function
                 // implicitly passed ownership of the reference to the
                 // caller.
                    return PyObjectRef(
                        reduce_function(obj1.get_incref(), obj2.get_incref()),
                        true);
                })
            // TODO: remove the Cache one ReduceNode can be executed again.
            .Cache());
    }

    PyDIA Cache() const {
        assert(dia_.IsValid());
        return PyDIA(dia_.Cache());
    }

    size_t Size() const {
        assert(dia_.IsValid());
        return dia_.Size();
    }

    PyObject * AllGather() const {
        assert(dia_.IsValid());
        std::vector<PyObjectRef> vec = dia_.AllGather();

        SWIG_PYTHON_THREAD_BEGIN_BLOCK;
        PyObject* pylist = PyList_New(0);
        for (size_t i = 0; i < vec.size(); ++i) {
            PyList_Append(pylist, vec[i].get());
        }
        SWIG_PYTHON_THREAD_END_BLOCK;
        return pylist;
    }
};

class PyContext : public api::Context
{
public:
    PyContext(std::unique_ptr<HostContext>&& host_context,
              size_t local_worker_id)
        : Context(*host_context, local_worker_id),
          host_context_(std::move(host_context))
    { }

    ~PyContext() {
        std::cout << "Destroy PyContext" << std::endl;
    }

    static std::vector<std::shared_ptr<PyContext> >
    ConstructLocalMock(size_t host_count, size_t workers_per_host)
    {
        std::vector<std::unique_ptr<HostContext> > host_contexts
            = HostContext::ConstructLocalMock(host_count, workers_per_host);

        std::vector<std::shared_ptr<PyContext> > contexts;

        for (size_t h = 0; h < host_count; ++h) {
            for (size_t w = 0; w < workers_per_host; ++w) {
                contexts.emplace_back(
                    std::make_shared<PyContext>(std::move(host_contexts[h]), w));
            }
        }

        return contexts;
    }

    PyDIA Generate(GeneratorFunction& generator_function, size_t size) {

        // the object GeneratorFunction is actually an instance of the Director
        SwigDirector_GeneratorFunction& director =
            *dynamic_cast<SwigDirector_GeneratorFunction*>(&generator_function);

        PyDIARef dia = api::Generate(
            *this, [&generator_function,
                  // this holds a reference count to the callback object for the
                  // lifetime of the capture object.
                  ref = PyObjectRef(director.swig_get_self())
                ](size_t index) {
                return PyObjectRef(generator_function(index), true);
            }, size);

        return PyDIA(dia);
    }

    PyDIA Distribute(const std::vector<PyObject*>& list) {

        // this acquires a reference count on the objects
        std::vector<PyObjectRef> list_refed(list.begin(), list.end());

        PyDIARef dia = api::Distribute(*this, std::move(list_refed));

        return PyDIA(dia);
    }

protected:
    std::unique_ptr<HostContext> host_context_;
};

} // namespace c7a

#endif // !C7A_SWIG_PYTHON_C7A_PYTHON_HEADER

/******************************************************************************/
