# Cassandra-Key-Value-store-using-Read-Repair-and-Hinted-Handoff

-----------------------------------------------------------------------
-----------------------------------------------------------------------


Following are the commands and the instructions to run Make on your project.
#### Note: Makefile is present in the same folder.
#### Tools: Make
#### Programming Language: Java

-----------------------------------------------------------------------
## Instruction to clean:

####Command: make clean

Description: It cleans up all the .class files that were generated when you compiled your code.

-----------------------------------------------------------------------
## Instruction to compile:

####Command:<br/>
bash<br/>
export PATH=/home/vchaska1/protobuf/bin:$PATH<br/>
protoc --java_out=. KeyValue.proto<br/>
make

Description: make command is used to create the executables.
Both the commands can be executed from the "cs457-cs557-pa4-kpoudwa1-pvishal1" directory.

-----------------------------------------------------------------------
## Instruction to run:

####Command:<br/>
To start replica: ./replica.sh <REPLICA_NAME> <PORT_NO> <LOG_FILE_PATH><REPLICAS_FILE_PATH><READ_REPAIR/HINTED_HANDOFF><br/>
To start client: ./client.sh <REPLICAS_FILE_PATH>

