#!/bin/bash

usr_name=$1
idx=$2
p="${usr_name}@fa20-cs425-g31-${idx}.cs.illinois.edu:~/"
echo $p
mvn clean install
scp target/distributed-group-membership-1.0-SNAPSHOT.jar $p
