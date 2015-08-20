/*******************************************************************************
 * swig/perl5/thrill_perl5.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_SWIG_PERL5_THRILL_PERL5_HEADER
#define THRILL_SWIG_PERL5_THRILL_PERL5_HEADER

#include <thrill/api/allgather.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/lop_node.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/string.hpp>

// #include <Python.h>
// #include <bytesobject.h>
// #include <marshal.h>

#include <string>
#include <vector>

namespace thrill {

static const bool debug = true;

void testCall(size_t i) {
    std::cout << "testCall in C++" << i << std::endl;
}

#if 0

/*!
 * This class holds a PyObject* and a reference count on the enclosed
 * object. PythonDIAs contain exclusively items of this class.
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

typedef swig::SwigVar_PyObject PyObjectVarRef;

namespace data {

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

} // namespace thrill

// import Swig Director classes.
#include "thrill_pythonPYTHON_wrap.h"

namespace std {
template <>
struct hash<thrill::PyObjectRef>
    : public std::unary_function<thrill::PyObjectRef, size_t>{
    std::size_t operator () (const thrill::PyObjectRef& ob) const {
        auto h = PyObject_Hash(ob.get());
        if (h == -1) {
            throw std::exception();
        }
        return h;
    }
};

} // namespace std

namespace thrill {

typedef api::DIARef<PyObjectRef> PythonDIARef;

/*!
 * This is a wrapper around the C++ DIARef class, which returns plain PythonDIAs
 * again. The C++ function stack is always collapsed.
 */
class PythonDIA
{
    static const bool debug = false;

public:
    PythonDIARef dia_;

    explicit PythonDIA(const PythonDIARef& dia)
        : dia_(dia) {
        sLOG << "create PythonDIA" << this;
    }

    ~PythonDIA() {
        sLOG << "delete PythonDIA" << this;
    }

    PythonDIA Map(MapFunction& map_function) const {
        assert(dia_.IsValid());

        // the object MapFunction is actually an instance of the Director
        SwigDirector_MapFunction& director =
            *dynamic_cast<SwigDirector_MapFunction*>(&map_function);

        return PythonDIA(
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

    PythonDIA ReduceBy(KeyExtractorFunction& key_extractor,
                       ReduceFunction& reduce_function) const {
        assert(dia_.IsValid());

        SwigDirector_KeyExtractorFunction& director1 =
            *dynamic_cast<SwigDirector_KeyExtractorFunction*>(&key_extractor);
        SwigDirector_ReduceFunction& director2 =
            *dynamic_cast<SwigDirector_ReduceFunction*>(&reduce_function);

        return PythonDIA(
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
                }));
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

static inline
PythonDIA Generate(
    Context& ctx, GeneratorFunction& generator_function, size_t size) {

    // the object GeneratorFunction is actually an instance of the Director
    SwigDirector_GeneratorFunction& director =
        *dynamic_cast<SwigDirector_GeneratorFunction*>(&generator_function);

    PythonDIARef dia = api::Generate(
        ctx, [&generator_function,
              // this holds a reference count to the callback object for the
              // lifetime of the capture object.
              ref = PyObjectRef(director.swig_get_self())
        ](size_t index) {
            return PyObjectRef(generator_function(index), true);
        }, size);

    return PythonDIA(dia);
}

#endif

} // namespace thrill

#endif // !THRILL_SWIG_PERL5_THRILL_PERL5_HEADER

/******************************************************************************/
