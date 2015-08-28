#!/usr/bin/env python
##########################################################################
# swig/python/python_rpyc_server.py
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
##########################################################################

import sys
import marshal
import types
import rpyc
import thrill


class RpcDIA():

    def __init__(self, dia):
        self._dia = dia

    def AllGather(self):
        return self._dia.AllGather()

    def Size(self):
        return self._dia.Size()

    def Map(self, map_function):
        code1 = marshal.loads(map_function)
        func1 = types.FunctionType(code1, globals())
        return RpcDIA(self._dia.Map(func1))

    def ReduceBy(self, key_extractor, reduce_function):
        code1 = marshal.loads(key_extractor)
        func1 = types.FunctionType(code1, globals())
        code2 = marshal.loads(reduce_function)
        func2 = types.FunctionType(code2, globals())
        return RpcDIA(self._dia.ReduceBy(func1, func2))


class RpcContext():

    def __init__(self, host_ctx, my_host_rank):
        self._ctx = thrill.PyContext(host_ctx, my_host_rank)

    def Generate(self, generator_function, size):
        code1 = marshal.loads(generator_function)
        function1 = types.FunctionType(code1, globals())
        return RpcDIA(self._ctx.Generate(function1, size))

    def Distribute(self, array):
        return RpcDIA(self._ctx.Distribute(array))


class MyService(rpyc.Service):

    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        print("hello client")
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        print("client disconnected")
        pass

    def exposed_Create(self, my_host_rank, endpoints):
        print("Creating thrill context for rank",
              my_host_rank, "endpoints", endpoints)
        host_ctx = thrill.HostContext(my_host_rank, endpoints, 1)
        return RpcContext(host_ctx, 0)

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port=int(sys.argv[1]),
                       protocol_config={"allow_public_attrs": True})
    t.start()

##########################################################################
