#!/bin/bash
a=$1
b=$2
sh ./clean.sh
mvn clean install
java -cp /home/yitanze2/distributed-group-membership/target/distributed-group-membership-1.0-SNAPSHOT.jar edu.cs425.mp3.MapleJuice $a $b
