#!/usr/bin/env python
##########################################################################
# swig/python/python_rpyc.py
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

from RemoteThrill import *

# rt = RemoteThrill([["localhost", 18861]], ["localhost:1234"])

rt = RemoteThrill([["localhost", 18861], ["localhost", 18862]],
                  ["localhost:1234", "localhost:1235"])


def genfunc(x):
    print("gen", x)
    return int(x + 10)

dia1 = rt.Generate(genfunc, 16)
print(dia1.AllGather())

dia2 = rt.Distribute(range(1, 100))
print("dia2.AllGather", dia2.AllGather())
print("dia2.Size", dia2.Size())

dia2pairs = dia2.Map(lambda x: [x, x])

dia3 = dia2pairs.ReduceBy(lambda x: (x[0] % 10),
                          lambda x, y: (x[0], x[1] + y[1]))

print("dia3.AllGather", dia3.AllGather())

##########################################################################
