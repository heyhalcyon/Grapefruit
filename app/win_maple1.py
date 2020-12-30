#!/usr/local/bin/python3

#input: <A,B,C>
#output: <V1#V2, 0 or 1>
#argv[1] is a string containing multiple lines, each line is formatted as <A,B,C>

import sys
# test = "A,B,C\nB,A,C\nC,B,A\nB,C,A"

for line in sys.argv[1].split():
   line = line.strip()
   if len(line) > 0:
       elems = line.split(",")
       if len(elems) == 3:
           for i in range(3):
               for j in range(i+1, 3):
                   winner = elems[i]
                   loser = elems[j]
                   if winner < loser:
                       print(winner+"#"+loser+",1")
                   else:
                       print(loser+"#"+winner+",0")
