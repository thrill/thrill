1#!/usr/bin/env python

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
