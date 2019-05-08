LIB_PATH=/home/vchaska1/protobuf/protobuf-java-3.5.1.jar
all: clean
	mkdir bin
	javac -classpath $(LIB_PATH) -d bin/ KeyValue.java Server.java FileProcessor.java Client.java DataStoreEntry.java Replica.java

clean: 
	rm -rf bin/
