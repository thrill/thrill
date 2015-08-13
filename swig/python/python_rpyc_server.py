#!/usr/bin/env python

import sys, marshal, types
import rpyc
import c7a

class RemoteContext(c7a.PyContext):
    def __init__(self, host_ctx, my_host_rank):
        super(RemoteContext, self).__init__(host_ctx, my_host_rank)

    def Generate(self, generator_function, size):
        new_code = marshal.loads(generator_function)
        func = types.FunctionType(new_code, globals())
        return super(RemoteContext, self).Generate(func, size)

class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        print("hello")
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_Create(self, my_host_rank, endpoints):
        print("Creating c7a context for rank", my_host_rank, "endpoints", endpoints)
        host_ctx = c7a.HostContext(my_host_rank, endpoints, 1)
        return RemoteContext(host_ctx, 0)

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = int(sys.argv[1]),
                       protocol_config = {"allow_public_attrs" : True})
    t.start()
