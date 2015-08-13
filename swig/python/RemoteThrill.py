#!/usr/bin/env python

import marshal, rpyc

class RemoteDIA():
    def __init__(self, dias):
        self._dias = dias

    def AllGather(self):
        # make async objects
        anetrefs = [rpyc.async(dia.AllGather) for dia in self._dias]
        # issue async requests
        asyncs = [ref() for ref in anetrefs]
        for a in asyncs: a.wait()
        # return RemoteDIA
        return [a.value for a in asyncs]

    def Size(self):
        # make async objects
        anetrefs = [rpyc.async(dia.Size) for dia in self._dias]
        # issue async requests
        asyncs = [ref() for ref in anetrefs]
        for a in asyncs: a.wait()
        # return RemoteDIA
        return [a.value for a in asyncs]

class RemoteThrill():
    def __init__(self, rpyc_hosts, thrill_hosts):
        # connect to rpyc servers
        self._conn = [rpyc.connect(*hp) for hp in rpyc_hosts]
        # set up background serving threads
        self._bgthr = [rpyc.BgServingThread(conn) for conn in self._conn]
        # make async objects to create c7a contexts
        anetrefs = [rpyc.async(conn.root.Create) for conn in self._conn]
        # issue async requests
        asyncs = [ref(rank, thrill_hosts) for rank, ref in enumerate(anetrefs)]
        for a in asyncs: a.wait()
        # get created c7a contexts
        self._ctx = [a.value for a in asyncs]

    def Distribute(self, array):
        # make async objects
        anetrefs = [rpyc.async(ctx.Distribute) for ctx in self._ctx]
        # issue async requests
        asyncs = [ref(array) for ref in anetrefs]
        for a in asyncs: a.wait()
        # return RemoteDIA
        return RemoteDIA([a.value for a in asyncs])

    def Generate(self, generator_function, size):
        # make async objects
        anetrefs = [rpyc.async(ctx.Generate) for ctx in self._ctx]
        # issue async requests
        mcode = marshal.dumps(generator_function.__code__)
        asyncs = [ref(mcode, size) for ref in anetrefs]
        for a in asyncs: a.wait()
        # return RemoteDIA
        return RemoteDIA([a.value for a in asyncs])
