import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;


public class Server
{
	private String serverName;
	private HashMap<Integer, DataStoreEntry> dataStore;
	private HashMap<String, HashMap<Integer, DataStoreEntry>> hintedHandoff;
	private HashMap<String, Replica> replicaMap;
	private String consistencyProcedure;
	final String QUORUM = "QUORUM";
	final String ONE = "ONE";
	final String READ_REPAIR = "READ_REPAIR";
	final String HINTED_HANDOFF = "HINTED_HANDOFF";
	
	public Server(String serverName, String consistencyProcedure) 
	{
		this.serverName = serverName;
		this.consistencyProcedure = consistencyProcedure;
		
		dataStore = new HashMap<Integer, DataStoreEntry>();
		hintedHandoff = new HashMap<String, HashMap<Integer, DataStoreEntry>>();
		replicaMap = new HashMap<String, Replica>();
		
		//Initializing the keys of the data store for each replica
		if(serverName.contains("0") || serverName.contains("1") || serverName.contains("2"))
		{
			for(int i = 0; i < 64; i++)
			{
				dataStore.put(i, null);
			}
		}
		
		if(serverName.contains("1") || serverName.contains("2") || serverName.contains("3"))
		{
			for(int i = 64; i < 128; i++)
			{
				dataStore.put(i, null);
			}
		}
		
		if(serverName.contains("2") || serverName.contains("3") || serverName.contains("0"))
		{
			for(int i = 128; i < 192; i++)
			{
				dataStore.put(i, null);
			}
		}
		
		if( serverName.contains("3") || serverName.contains("0") || serverName.contains("1"))
		{
			for(int i = 192; i < 256; i++)
			{
				dataStore.put(i, null);
			}
		}
	}
	
	/**
	 * Function for creating a hash map of the replicas
	 * @param fp
	 * @throws IOException
	 */
	void setReplicaMapping(FileProcessor fp) throws IOException
	{
		String line = null;
		
		//File format "ReplicaName IP Port"
		while((line = fp.readLine()) != null)
		{
			String arr[] = line.split("\\s");
			Replica tempReplica = new Replica();
			tempReplica.setIp(arr[1]);
			tempReplica.setPort(Integer.parseInt(arr[2]));
			
			//Setting the excluding ranges
			if(arr[0].contains("0"))
			{
				tempReplica.setStart(64);
				tempReplica.setEnd(127);
			}
			else if(arr[0].contains("1"))
			{
				tempReplica.setStart(128);
				tempReplica.setEnd(191);
			}
			else if(arr[0].contains("2"))
			{
				tempReplica.setStart(192);
				tempReplica.setEnd(255);
			}
			else if(arr[0].contains("3"))
			{
				tempReplica.setStart(0);
				tempReplica.setEnd(63);
			}
			
			if(!serverName.equalsIgnoreCase(arr[0]))
			{
				replicaMap.put(arr[0], tempReplica);
			}
		}
		
		
	}
	
	/**
	 * Function for performing the read repair consistency
	 * @param key Key for which read repair is to be performed
	 * @param responseList The key-value pairs received from different replicas
	 * @param responseServerList List of servers for which the keys is to be repaired
	 * @param logFilePath Path to the write-ahead log
	 * @throws IOException When unable to connect to host
	 */
	public void doReadRepair(int key, ArrayList<DataStoreEntry> responseList, ArrayList<String> responseServerList, String logFilePath) throws IOException 
	{
		Long maxTimeStamp = (long)0;
		String updatedValue = "";
		
		//Getting the timestamp and value of the latest entry
		for (DataStoreEntry dataStoreEntry : responseList) 
		{
			if(dataStoreEntry.getTimestamp() > maxTimeStamp) 
			{
				maxTimeStamp = dataStoreEntry.getTimestamp();
				updatedValue = dataStoreEntry.getValue();
			}
		}
		
		//Identifying the servers for which read repair is to be performed
		for(int i = 0; i<responseList.size(); i++) 
		{
			if(responseList.get(i).getTimestamp() < maxTimeStamp)
			{
				//Checking if the current server contains an old entry
				if(!responseServerList.get(i).equalsIgnoreCase(serverName))
				{
					Socket socket = null;
					try
					{
						socket = new Socket(replicaMap.get(responseServerList.get(i)).getIp(), replicaMap.get(responseServerList.get(i)).getPort());
						
						//Creating request messages
						KeyValue.KeyValueMessage.Builder keyValueMessage = KeyValue.KeyValueMessage.newBuilder();
						KeyValue.ServerRequest.Builder requestMessage = KeyValue.ServerRequest.newBuilder();

						//Setting message data
						requestMessage.setKey(key);
						requestMessage.setValue(updatedValue);
						requestMessage.setTimestamp(maxTimeStamp);
						requestMessage.setType("PUT");
						requestMessage.setReplicaname(serverName);
						keyValueMessage.setServerRequest(requestMessage);
						
						//Sending the message
						keyValueMessage.build().writeDelimitedTo(socket.getOutputStream());
						
						//Reading the response messages
						System.out.println("READ_REPAIR reply: " + KeyValue.KeyValueMessage.parseDelimitedFrom(socket.getInputStream()));
					}
					catch(IOException e) 
					{
						e.printStackTrace();
					}
					finally
					{
						if(socket!=null)
							socket.close();
					}
				}
				else//Updating the old entry of the replica
				{
					//Writing to write-ahead log
					FileProcessor.writeLog(key, updatedValue, maxTimeStamp, logFilePath);
					
					//Updating the data store
					DataStoreEntry newEntry = new DataStoreEntry();
					newEntry.setValue(updatedValue);
					newEntry.setTimestamp(maxTimeStamp);
					dataStore.put(key, newEntry);
				}
			}
		}
	}
	
	/**
	 * Function for performing Hinted Handoff
	 * @param replica The replica name
	 * @throws IOException When the host is not found
	 */
	private void doHintedHandoff(String replica) throws IOException
	{
		//Getting the hints for the replica
		HashMap<Integer, DataStoreEntry> mapHints = hintedHandoff.get(replica);
		
		//Iterating over the hints
		Iterator<Integer> it = mapHints.keySet().iterator();
		while(it.hasNext())
		{
			int key = it.next();
			DataStoreEntry dataEntry = mapHints.get(key);
			
			Socket socket = null;
			try
			{
				socket = new Socket(replicaMap.get(replica).getIp(), replicaMap.get(replica).getPort());
				
				//Creating request messages
				KeyValue.KeyValueMessage.Builder keyValueMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.ServerRequest.Builder requestMessage = KeyValue.ServerRequest.newBuilder();

				//Setting the required values
				requestMessage.setKey(key);
				requestMessage.setValue(dataEntry.getValue());
				requestMessage.setTimestamp(dataEntry.getTimestamp());
				requestMessage.setType("PUT");
				requestMessage.setReplicaname(serverName);
				keyValueMessage.setServerRequest(requestMessage);
				
				//Sending the message
				keyValueMessage.build().writeDelimitedTo(socket.getOutputStream());

				//Reading the response
				KeyValue.KeyValueMessage keyvalueMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(socket.getInputStream());
				
				//Removing the hint from the list of hints
				if(keyvalueMessage.hasServerResponse())
				{
					mapHints.remove(key);
					it = mapHints.keySet().iterator();
				}
			}
			catch(IOException e) 
			{
				e.printStackTrace();
			}
			finally
			{
				if(socket!=null)
					socket.close();
			}
		}
		//Removing the hints
		if(mapHints.isEmpty())
		{
			hintedHandoff.remove(replica);
		}
	}
	
	/**
	 * Function for handling the requests received by the replica
	 * @param serverSocket Socket object
	 * @param logFilePath File path for the write-ahead log
	 * @throws IOException Throws an exception when unable
	 *  to connect to a host
	 */
	public void handleRequest(Socket serverSocket, String logFilePath) throws IOException
	{
		//Parsing the request message
		KeyValue.KeyValueMessage requestMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(serverSocket.getInputStream());
		System.out.println("Message Received: " + requestMessage);
		
		//List of available servers
		ArrayList<String> availableServer = new ArrayList<>();
		
		if(requestMessage != null)
		{
			//Creating a response message
			KeyValue.KeyValueMessage.Builder responseMessage = KeyValue.KeyValueMessage.newBuilder();

			//Handling client request
			if(requestMessage.hasClientRequest())
			{
				//Getting the client request
				KeyValue.ClientRequest clientRequest = requestMessage.getClientRequest();
				
				//Creating the client response object
				KeyValue.ClientResponse.Builder clientResponseMessage = KeyValue.ClientResponse.newBuilder();
				clientResponseMessage.setKey(clientRequest.getKey());

				/*Getting the consistency level requested by
				 the client*/
				String consistency = clientRequest.getConsistency();
				
				//Handling client get request
				if(clientRequest.getType().equals("GET"))
				{
					ArrayList<DataStoreEntry> responseList = new ArrayList<>();
					ArrayList<String> responseServerList = new ArrayList<>();
					
					boolean isOwner = false;
					
					Set<String> replicaKeys = replicaMap.keySet();
					Iterator<String> replicaIterator = replicaKeys.iterator();
					for(int i = 0; i<4; i++)
					{
						DataStoreEntry newEntry = new DataStoreEntry();
						/*Handling if consistency is 1 and coordinator
						   is itself the replica*/
						if(dataStore.containsKey(clientRequest.getKey()) && isOwner==false)
						{
							if(dataStore.get(requestMessage.getClientRequest().getKey()) == null)
							{
								newEntry.setValue("");
								newEntry.setTimestamp((long)0);
							}
							else
							{
								newEntry.setValue(dataStore.get(requestMessage.getClientRequest().getKey()).getValue());
								newEntry.setTimestamp(dataStore.get(requestMessage.getClientRequest().getKey()).getTimestamp());
							}
							responseList.add(newEntry);
							responseServerList.add(serverName);
							isOwner = true;
						}
						else
						{
							if(replicaIterator.hasNext())
							{
								String replicaKey = replicaIterator.next();
								
								//Check if the replica contains key or not
								if(replicaMap.get(replicaKey).isInRange(clientRequest.getKey()))
								{
									Socket socket = null;
									try
									{
										System.out.println("Sending request to " + replicaKey);
										socket = new Socket(replicaMap.get(replicaKey).getIp(), replicaMap.get(replicaKey).getPort());
										
										KeyValue.KeyValueMessage.Builder requestMessage1 = KeyValue.KeyValueMessage.newBuilder();
										KeyValue.ServerRequest.Builder serverRequest = KeyValue.ServerRequest.newBuilder();
										
										serverRequest.setKey(clientRequest.getKey());
										serverRequest.setType("GET");
										serverRequest.setReplicaname(serverName);
										
										//Send request to server
										requestMessage1.setServerRequest(serverRequest);
										requestMessage1.build().writeDelimitedTo(socket.getOutputStream());
										
										//Handle server Response
										KeyValue.KeyValueMessage keyvalueMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(socket.getInputStream());
										if(keyvalueMessage.hasServerResponse())
										{
											newEntry.setValue(keyvalueMessage.getServerResponse().getValue());
											newEntry.setTimestamp(keyvalueMessage.getServerResponse().getTimestamp());
											responseList.add(newEntry);
											responseServerList.add(replicaKey);

											//Add the servers connected at this point of time for hinted handoff
											if(!availableServer.contains(replicaKey))
											{
												availableServer.add(replicaKey);
											}
										}
									}
									catch(IOException e)
									{
										System.out.println("Server " + replicaKey + " is not available");
										System.out.println(replicaMap.get(replicaKey).getIp() +"::"+ replicaMap.get(replicaKey).getPort());
									}
									finally
									{
										if(socket != null)
											socket.close();
									}
								}
							}
						}
					}
					//Checking the consistency levels
					if(!responseList.isEmpty())
					{
						//When consistency is ONE
						if(consistency.equalsIgnoreCase(ONE))
						{
							clientResponseMessage.setValue(responseList.get(0).getValue());
							clientResponseMessage.setStatus("SUCCESS");
						}//When consistency is QUORUM
						else if(responseList.size() >= 2)
						{
							if(responseList.get(0).getTimestamp() > responseList.get(1).getTimestamp())
							{
								clientResponseMessage.setValue(responseList.get(0).getValue());
								clientResponseMessage.setStatus("SUCCESS");
							}
							else
							{
								clientResponseMessage.setValue(responseList.get(1).getValue());
								clientResponseMessage.setStatus("SUCCESS");
							}
						}
						else
						{
							clientResponseMessage.setStatus("FAIL");
						}
					}
					else
					{
						clientResponseMessage.setStatus("FAIL");
					}
					
					//Performing read repair
					if(consistencyProcedure.equalsIgnoreCase(READ_REPAIR))
					{
						doReadRepair(clientRequest.getKey(), responseList, responseServerList, logFilePath);
					}
					
				}//Handling client put request
				else if(clientRequest.getType().equals("PUT"))
				{
					ArrayList<String> responseList = new ArrayList<>();
					boolean isOwner = false;
					
					Set<String> replicaKeys = replicaMap.keySet();
					Iterator<String> replicaIterator = replicaKeys.iterator();
					Long timestampOfPutRequest = System.currentTimeMillis();
					
					for(int i = 0; i < 4; i++)
					{
						//If the coordinator is the owner for the key
						if(dataStore.containsKey(clientRequest.getKey()) && isOwner == false)
						{
							//Write to write-ahead log file
							FileProcessor.writeLog(clientRequest.getKey(), clientRequest.getValue(), timestampOfPutRequest, logFilePath);
							
							//Update the data store
							DataStoreEntry newEntry = new DataStoreEntry();
							newEntry.setValue(clientRequest.getValue());
							newEntry.setTimestamp(timestampOfPutRequest);
							dataStore.put(clientRequest.getKey(), newEntry);
							
							responseList.add("SUCCESS");
							isOwner = true;
						}
						else
						{
							//If the coordinator is not the owner of the key
							if(replicaIterator.hasNext())
							{
								String replicaKey = replicaIterator.next();
								
								//Check if the replica extracted contains key or not
								if(replicaMap.get(replicaKey).isInRange(clientRequest.getKey()))
								{
									Socket socket = null;
									try
									{
										System.out.println("Sending PUT request to " + replicaKey);
										socket = new Socket(replicaMap.get(replicaKey).getIp(), replicaMap.get(replicaKey).getPort());
										
										//Creating a request message
										KeyValue.KeyValueMessage.Builder requestMessage1 = KeyValue.KeyValueMessage.newBuilder();
										KeyValue.ServerRequest.Builder serverRequest = KeyValue.ServerRequest.newBuilder();
										
										//Setting the appropriate parameters
										serverRequest.setKey(clientRequest.getKey());
										serverRequest.setValue(clientRequest.getValue());
										serverRequest.setType(clientRequest.getType());
										serverRequest.setTimestamp(timestampOfPutRequest);
										serverRequest.setReplicaname(serverName);
										
										//Send request to server
										requestMessage1.setServerRequest(serverRequest);
										requestMessage1.build().writeDelimitedTo(socket.getOutputStream());
										
										//Processing the response
										KeyValue.KeyValueMessage keyvalueMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(socket.getInputStream());
										if(keyvalueMessage.hasServerResponse())
										{
											responseList.add("SUCCESS");

											//Add the servers connected at this point of time for hinted handoff
											if(!availableServer.contains(replicaKey))
											{
												availableServer.add(replicaKey);
											}
										}
									}
									catch(IOException e)
									{
										System.out.println("Server " + replicaKey + " is not available");
										System.out.println(replicaMap.get(replicaKey).getIp() +"::"+ replicaMap.get(replicaKey).getPort());
										
										//Checking for consistency levels
										if(consistencyProcedure.equalsIgnoreCase(HINTED_HANDOFF))
										{
											HashMap<Integer, DataStoreEntry> hints;
											DataStoreEntry hintDataStoreEntry = new DataStoreEntry();
											if (hintedHandoff.containsKey(replicaKey))
											{
												hints = hintedHandoff.get(replicaKey);
											}
											else
											{
												hints = new HashMap<>();
											}
											//Handling multiple value for same key 
											if(hints.get(clientRequest.getKey()) == null || hints.get(clientRequest.getKey()).getTimestamp() < timestampOfPutRequest)
											{
												hintDataStoreEntry.setValue(clientRequest.getValue());
												hintDataStoreEntry.setTimestamp(timestampOfPutRequest);
												hints.put(clientRequest.getKey(), hintDataStoreEntry);
												
												hintedHandoff.put(replicaKey, hints);
											}
										}
									}
									finally
									{
										if(socket != null)
											socket.close();
									}
								}
							}
						}
					}
					if(!responseList.isEmpty())
					{
						if(consistency.equalsIgnoreCase(ONE))
						{
							if(responseList.size() >= 1)
							{
								clientResponseMessage.setStatus("SUCCESS");
							}
							else
							{
								clientResponseMessage.setStatus("FAIL");
							}
						}
						else if(responseList.size() >= 2)
						{
							clientResponseMessage.setStatus("SUCCESS");
						}
						else
						{
							clientResponseMessage.setStatus("FAIL");
						}
					}
					else
					{
						clientResponseMessage.setStatus("FAIL");
					}
				}
				
				//Setting and sending the response
				responseMessage.setClientResponse(clientResponseMessage);
				responseMessage.build().writeDelimitedTo(serverSocket.getOutputStream());

			}//Handling server request
			else if(requestMessage.hasServerRequest())
			{
				//Creating the request message
				KeyValue.ServerRequest serverRequest = requestMessage.getServerRequest();
				KeyValue.ServerResponse.Builder serverResponseMessage = KeyValue.ServerResponse.newBuilder();

				System.out.println("Server request received for key : " + serverRequest.getKey() + " from " + serverRequest.getReplicaname());
				if(serverRequest.getType().equalsIgnoreCase("GET"))
				{
					serverResponseMessage.setKey(serverRequest.getKey());
					//Setting the response fields
					if(dataStore.get(serverRequest.getKey()) == null)
					{
						serverResponseMessage.setValue("");
						serverResponseMessage.setTimestamp((long)0);
					}
					else
					{
						serverResponseMessage.setValue(dataStore.get(serverRequest.getKey()).getValue());
						serverResponseMessage.setTimestamp(dataStore.get(serverRequest.getKey()).getTimestamp());
					}
					serverResponseMessage.setStatus("SUCCESS");
					System.out.println("Sending the response for the required key");
				}
				else if(serverRequest.getType().equalsIgnoreCase("PUT")) 
				{
					//Updating the data store
					if(dataStore.get(serverRequest.getKey()) == null ||dataStore.get(serverRequest.getKey()).getTimestamp() < serverRequest.getTimestamp())
					{
						//Writing to the write-ahead log file
						FileProcessor.writeLog(serverRequest.getKey(), serverRequest.getValue(), serverRequest.getTimestamp(), logFilePath);

						//Updating the data store
						DataStoreEntry newEntry = new DataStoreEntry();
						newEntry.setValue(serverRequest.getValue());
						newEntry.setTimestamp(serverRequest.getTimestamp());
						dataStore.put(serverRequest.getKey(), newEntry);
					}
					serverResponseMessage.setStatus("SUCCESS");
				}
				
				responseMessage.setServerResponse(serverResponseMessage);
				responseMessage.build().writeDelimitedTo(serverSocket.getOutputStream());
				
				//Add the servers connected at this point of time for hinted handoff
				if(!availableServer.contains(serverRequest.getReplicaname()))
				{
					availableServer.add(serverRequest.getReplicaname());
				}
			}

			System.out.println(dataStore);
			System.out.println("Hints before sending hints: "+hintedHandoff);
			System.out.println("Replicas connected to: "+availableServer);
			
			//For hinted handoff
			for (String hintReplicaName : availableServer) {
				if(hintedHandoff.containsKey(hintReplicaName) && consistencyProcedure.equalsIgnoreCase(HINTED_HANDOFF))
				{
					System.out.println("Doing Hinted Handoff for: "+hintReplicaName);
					doHintedHandoff(hintReplicaName);
				}
			}
			
			//Clearing the list
			availableServer.clear();
		}
	}

	public static void main(String[] args)
	{
		ServerSocket server = null;
		if (args.length != 5)
		{
			System.err.println("Error: Incorrect number of arguments. The program accepts 5 argument(s).");
			System.exit(0);
			//Server Name, port, log file path, replica file path, Hinted handoff or read repair 
		}
		
		try
		{
			//Init Server and set the server name
			Server s = new Server(args[0], args[4]);
			
			//Check if write-ahead file exists or not
			if(FileProcessor.checkLogFile(args[2]))
			{
				//For populating in-memory key-value store from the file
				FileProcessor fp = new FileProcessor(args[2]);
				fp.updateDatastore(s.dataStore);
			}
			
			//Setting the replica mappings
			FileProcessor fp1 = new FileProcessor(args[3]);
			s.setReplicaMapping(fp1);
			
			//Starting the server
			server = new ServerSocket(Integer.parseInt(args[1]));
			System.out.println("Replica Name: " + s.serverName);
			System.out.println("Host Name: " + server.getInetAddress().getLocalHost().getHostName());
			System.out.println("Port Number: " + server.getLocalPort());
			System.out.println("Server started....");
			System.out.println("=============================================================================");
			
			while(true)
			{
				//Accepting the client
				Socket serverSocket = server.accept();
				
				//Handling the request
				s.handleRequest(serverSocket, args[2]);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(server != null)
					server.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}