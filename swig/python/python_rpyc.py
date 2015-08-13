#!/usr/bin/env python

from RemoteThrill import *

# rt = RemoteThrill([["localhost", 18861]], ["localhost:1234"])

rt = RemoteThrill([["localhost", 18861], ["localhost", 18862]],
                  ["localhost:1234", "localhost:1235"])

def genfunc(x):
    print("gen",x)
    return int(x + 10)

dia1 = rt.Generate(genfunc, 16)
print(dia1.AllGather())

dia2 = rt.Distribute(range(1, 100))
print("dia2.AllGather", dia2.AllGather())
print("dia2.Size", dia2.Size())

dia2pairs = dia2.Map(lambda x : [x,x])

dia3 = dia2pairs.ReduceBy(lambda x : (x[0] % 10),
                          lambda x,y : (x[0], x[1] + y[1]))

print("dia3.AllGather", dia3.AllGather())
