#!/usr/bin/env python

from RemoteThrill import *

rt = RemoteThrill([["localhost", 18861], ["localhost", 18862]],
                  ["localhost:1234", "localhost:1235"])

def genfunc(x):
    print("gen",x)
    return int(x + 10)

dia1 = rt.Generate(genfunc, 16)
print(dia1.AllGather())

dia2 = rt.Distribute([1,2,3,4,5,6])
print(dia2.AllGather())
print(dia2.Size())
