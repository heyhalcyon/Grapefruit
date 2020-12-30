#!/usr/local/bin/python3

# Input: List of nodes that points to a center node (key).
# Output: if there are at least 50 nodes that point to the center node, output <center node, Follower Count>
# argv[1] a key i.e. a node
# argv[2] a file containing a list of web nodes

import sys

key = sys.argv[1]
filename = sys.argv[2]
count = 0

with open(filename, "r") as file:
    for line in file:
        if len(line.strip()) > 0:
            count += 1

print(key + "," + str(count))
