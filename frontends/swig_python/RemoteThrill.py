#!/usr/bin/env python
##########################################################################
# frontends/swig_python/RemoteThrill.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import marshal
import rpyc


class RemoteDIA():

    def __init__(self, dias):
        self._dias = dias

    def AllGather(self):
        # make async objects
        anetrefs = [rpyc.async(dia.AllGather) for dia in self._dias]
        # issue async requests
        asyncs = [ref() for ref in anetrefs]
        for a in asyncs:
            a.wait()
        # return values of workers as list
        return [a.value for a in asyncs]

    def Size(self):
        # make async objects
        anetrefs = [rpyc.async(dia.Size) for dia in self._dias]
        # issue async requests
        asyncs = [ref() for ref in anetrefs]
        for a in asyncs:
            a.wait()
        # return values of workers as list
        return [a.value for a in asyncs]

    def Map(self, map_function):
        # make async objects
        anetrefs = [rpyc.async(dia.Map) for dia in self._dias]
        # issue async requests
        _map_function = marshal.dumps(map_function.__code__)
        asyncs = [ref(_map_function) for ref in anetrefs]
        for a in asyncs:
            a.wait()
        # return RemoteDIA
        return RemoteDIA([a.value for a in asyncs])

    def ReduceBy(self, key_extractor, reduce_function):
        # make async objects
        anetrefs = [rpyc.async(dia.ReduceBy) for dia in self._dias]
        # issue async requests
        _key_extractor = marshal.dumps(key_extractor.__code__)
        _reduce_function = marshal.dumps(reduce_function.__code__)
        asyncs = [ref(_key_extractor, _reduce_function) for ref in anetrefs]
        for a in asyncs:
            a.wait()
        # return RemoteDIA
        return RemoteDIA([a.value for a in asyncs])


class RemoteThrill():

    def __init__(self, rpyc_hosts, thrill_hosts):
        # connect to rpyc servers
        self._conn = [rpyc.connect(*hp) for hp in rpyc_hosts]
        # set up background serving threads
        self._bgthr = [rpyc.BgServingThread(conn) for conn in self._conn]
        # make async objects to create Thrill contexts
        anetrefs = [rpyc.async(conn.root.Create) for conn in self._conn]
        # issue async requests
        asyncs = [ref(rank, thrill_hosts) for rank, ref in enumerate(anetrefs)]
        for a in asyncs:
            a.wait()
        # get created Thrill contexts
        self._ctx = [a.value for a in asyncs]

    def Distribute(self, array):
        # make async objects
        anetrefs = [rpyc.async(ctx.Distribute) for ctx in self._ctx]
        # issue async requests
        asyncs = [ref(array) for ref in anetrefs]
        for a in asyncs:
            a.wait()
        # return RemoteDIA
        return RemoteDIA([a.value for a in asyncs])

    def Generate(self, generator_function, size):
        # make async objects
        anetrefs = [rpyc.async(ctx.Generate) for ctx in self._ctx]
        # issue async requests
        _generator_function = marshal.dumps(generator_function.__code__)
        asyncs = [ref(_generator_function, size) for ref in anetrefs]
        for a in asyncs:
            a.wait()
        # return RemoteDIA
        return RemoteDIA([a.value for a in asyncs])

##########################################################################
