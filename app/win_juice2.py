#!/usr/local/bin/python3

# Input: <1, List of V1#V2>.
# Output: todo
# argv[1] a key i.e. V1#V2
# argv[2] a file containing a list of 0's and 1's

import sys

key = sys.argv[1]
filename = sys.argv[2]

candidates = []
countDict = {}

# Count the number of 0s and number of 1s
with open(filename, "r") as file:
    for line in file:
        elems = line.strip().split("#")
        if len(elems) == 2:
            c1 = elems[0]
            c2 = elems[1]
            if(c1 not in candidates):
                candidates.append(c1)
                countDict[c1] = 1
            else:
                countDict[c1] += 1
            if c2 not in candidates:
                candidates.append(c2)
                countDict[c2] = 0

maxCount = 0
coWinners =[]
winnerFound = False
for x in countDict:
    if countDict[x] == 2:
        winnerFound = True
        break
    if countDict[x] > maxCount:
        maxCount = countDict[x]


if winnerFound:
    print(x+",CondorcetWinner")



if not winnerFound:
    for x in countDict:
        if countDict[x] == maxCount:
            coWinners.append(x)
    lstStr = ""
    for x in coWinners:
        lstStr += x + "#"

    print(lstStr[:-1]+",No Condorcet Winner..Highest Condorcet Counts")