#!/usr/local/bin/python3

# Input: <V1#V2, List of 0's and 1's>.
# Output: <V1,V2> or <V2, V1>
# argv[1] a key i.e. V1#V2
# argv[2] a file containing a list of 0's and 1's

import sys

key = sys.argv[1]
filename = sys.argv[2]
count1 = 0
count0 = 0

candidates = key.split("#")
A = candidates[0]
B = candidates[1]

# Count the number of 0s and number of 1s
with open(filename, "r") as file:
    for line in file:
        line = line.strip()
        if len(line) > 0:
            if line == "1":
                count1 += 1
            elif line == "0":
                count0 += 1

if count1 > count0:  # if more 1s than 0s, output (A,B) // A dominates B
    print(A + "," + B)
else:
    print(B + "," + A) # B dominates A
