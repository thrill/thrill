#!/usr/bin/env python
##########################################################################
# run/ec2/benchmark.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import os
import sys

os.system("python benchmark_tpch.py > tpch")
os.wait()

os.system("python benchmark_pagerankgen.py > pagerank")
os.wait()

os.system("python benchmark_duplicates.py > duplicates")
os.wait()

os.system("python benchmark_triangles.py > triangles")
os.wait()

##########################################################################
