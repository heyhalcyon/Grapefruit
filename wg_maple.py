#!/usr/local/bin/python3

#input: <FromNode, ToNode>
#output: <ToNode, FromNode>
#argv[1] is a string containing multiple lines, each line is formatted as <FromNode> <ToNode>

import sys

for line in sys.argv[1].split():
   line = line.strip()
   if len(line) > 0:
       elems = line.split(",")
       node = int(elems[1])
       if 1 <= node <= 3:
           print(elems[1] + "," + elems[0])
