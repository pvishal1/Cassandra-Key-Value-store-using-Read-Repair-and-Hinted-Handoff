import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

public class Client
{
	private static HashMap<Integer, Replica> replicas = new HashMap<Integer, Replica>();
	private static HashMap<Integer, String> methods = new HashMap<Integer, String>();
	private static HashMap<Integer, String> consistency = new HashMap<Integer, String>();
	
	private static void initReplicas(FileProcessor fp) throws IOException
	{
		String line = null;
		while((line = fp.readLine()) != null)
		{
			String []tempArr = line.split("\\s");
			int key = Integer.parseInt(tempArr[0].substring(tempArr[0].length() - 1));

			Replica tempReplica = new Replica();
			tempReplica.setReplicaName(tempArr[0]);
			tempReplica.setIp(tempArr[1]);
			tempReplica.setPort(Integer.parseInt(tempArr[2]));
			
			replicas.put(key, tempReplica);
		}
		
		//Init the methods
		methods.put(0, "GET");
		methods.put(1, "PUT");
		
		//Init the consistency
		consistency.put(0, "ONE");
		consistency.put(1, "QUORUM");
	}
	
	public static void main(String[] args) 
	{
		if (args.length != 1)
		{
			System.err.println("Error: Incorrect number of arguments. The program accepts 1 argument.");
			System.exit(0);
			//Replica file path 
		}
		
		DataInputStream dis = null;
		Socket socket = null;
		
		try
		{
			FileProcessor fp = new FileProcessor(args[0]);
			initReplicas(fp);
			
			
			while(true)
			{
				System.out.println("===================================================================================================");
				//DataInputStream for reading user input
				dis = new DataInputStream(System.in);
				int replicaChoice;
				int method;
				int key;
				int consistencyLevel;
				String value = null;
				
				System.out.println("List of replicas:");
				for(int i = 0; i < replicas.size(); i++)
				{
					System.out.println((i + 1) + ". " + replicas.get(i).getIp() + " : " + replicas.get(i).getPort());
				}
				System.out.println("Enter your choice as 1, 2, 3 or 4:");
				replicaChoice = Integer.parseInt(dis.readLine()) - 1;
				
				System.out.println("Enter the consistency level");
				for(int i = 0; i < consistency.size(); i++)
				{
					System.out.println((i + 1) + ". " + consistency.get(i));
				}
				consistencyLevel = Integer.parseInt(dis.readLine()) - 1;
				
				System.out.println("Enter your method");
				for(int i = 0; i < methods.size(); i++)
				{
					System.out.println((i + 1) + ". " + methods.get(i));
				}
				method = Integer.parseInt(dis.readLine()) - 1;
				
				System.out.println("Enter the key: ");
				key = Integer.parseInt(dis.readLine());
				
				if(method == 1)
				{
					System.out.println("Enter the value: ");
					value = dis.readLine();
				}
				
				//Connecting to coordinator
				socket = new Socket(replicas.get(replicaChoice).getIp(), replicas.get(replicaChoice).getPort());
				System.out.println("Connected to server");
				
				//Create message
				KeyValue.KeyValueMessage.Builder requestMessage = KeyValue.KeyValueMessage.newBuilder();
				
				//Create client message
				KeyValue.ClientRequest.Builder clientRequestMessage = KeyValue.ClientRequest.newBuilder();
				
				// Code for getting value for respective key from server
				clientRequestMessage.setKey(key);
				clientRequestMessage.setType(methods.get(method));
				clientRequestMessage.setConsistency(consistency.get(consistencyLevel));
				
				//Setting the value for put
				if(method == 1)
				{
					clientRequestMessage.setValue(value);
				}
				
				//Send the message
				requestMessage.setClientRequest(clientRequestMessage);
				requestMessage.build().writeDelimitedTo(socket.getOutputStream());
				
				//Processing the response
				KeyValue.KeyValueMessage responseMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(socket.getInputStream());
				KeyValue.ClientResponse clientResponseMessage = responseMessage.getClientResponse();
				
				System.out.println("Response received from Coordinator");
				System.out.println("Key: " + clientResponseMessage.getKey());
				System.out.println("Value: " + clientResponseMessage.getValue());
				System.out.println("Status: " + clientResponseMessage.getStatus());
				
				socket.close();
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
				if(dis != null)
					dis.close();
				if(socket != null)
					socket.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}