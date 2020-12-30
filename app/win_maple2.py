#!/usr/local/bin/python3

#input: <A,B>
#output: <1, A#B>
#argv[1] is a string containing multiple lines, each line is formatted as <A,B>

import sys

# test = "A,B\nB,C\nC,A\nB,A"


for line in sys.argv[1].split():
   line = line.strip()
   if len(line) > 0:
       elems = line.split(",")
       if len(elems) == 2:
           print("1,"+elems[0]+"#"+elems[1])
