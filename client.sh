#!/bin/bash +vx
LIB_PATH=$"/home/vchaska1/protobuf/protobuf-java-3.5.1.jar"
#Replica file path
java -classpath bin:$LIB_PATH Client $1
