#!/bin/bash +vx
LIB_PATH=$"/home/vchaska1/protobuf/protobuf-java-3.5.1.jar"
#Server Name, Port, Log file path, Replica file path, Consistency level
java -classpath bin:$LIB_PATH Server $1 $2 $3 $4 $5
