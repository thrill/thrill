#!/usr/bin/env python3
##########################################################################
# scripts/json2graphviz.py
#
# Python script to parse the JSON output log of a Thrill worker and emit a
# graphviz (dot) graph describing the DIA graph executed by the program.
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import sys
import json

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " <thrill-worker-log.json>");
    sys.exit(0)

nodes = {}

with open(sys.argv[1], 'r') as json_file:
    for line in json_file:
        data = json.loads(line)

        if data["class"] == "DIABase" and data["event"] == "create":
            id = data["node_id"]
            nodes[id] = { "label": data["node_label"],
                          "parents": data["parents"] }
        if data["class"] == "DIA" and data["event"] == "create":
            id = data["node_id"]
            nodes[id] = { "label": data["node_label"],
                          "parents": data["parents"] }

print("digraph {")

# print nodes with nice labels
for ni, n in nodes.items():
    label = n["label"]
    sys.stdout.write(" " + str(ni) + "[label=\"" + label + "." + str(ni) + "\"")

    if label == "ReadLines" or \
       label == "ReadBinary" or \
       label == "Generate" or \
       label == "GenerateFile" or \
       label == "Distribute" or \
       label == "DistributeFile":
        sys.stdout.write("colorscheme=accent5, style=filled, color=1, shape=invhouse")

    if label == "PrefixSum" or \
       label == "ReduceBy" or \
       label == "ReducePair" or \
       label == "ReduceToIndex" or \
       label == "GroupBy" or \
       label == "GroupByIndex" or \
       label == "Merge" or \
       label == "Sort" or \
       label == "Window" or \
       label == "Zip":
        sys.stdout.write("colorscheme=accent5, style=filled, color=2, shape=box")

    if label == "AllGather" or \
       label == "Gather" or \
       label == "Size" or \
       label == "Sum" or \
       label == "Min" or \
       label == "Max" or \
       label == "WriteBinary" or \
       label == "WriteLines" or \
       label == "WriteLinesMany":
        sys.stdout.write("colorscheme=accent5, style=filled, color=3, shape=house")

    if label == "Cache":
        sys.stdout.write("colorscheme=accent5, style=filled, color=4, shape=oval")

    if label == "Collapse":
        sys.stdout.write("colorscheme=accent5, style=filled, color=5, shape=oval")

    sys.stdout.write("];\n")

print()

# print edges
for ni, n in nodes.items():
    for e in n["parents"]:
        print(" ", e, "->", ni, ";")

print("}")

##########################################################################
