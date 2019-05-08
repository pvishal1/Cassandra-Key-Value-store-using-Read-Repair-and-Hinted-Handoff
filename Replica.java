
/**
 * Class representing the different replicas 
 */
public class Replica
{
	private String replicaName;
	private String ip;
	private int port;
	private int start;
	private int end;
	
	public String getReplicaName() 
	{
		return replicaName;
	}

	public void setReplicaName(String replicaName) 
	{
		this.replicaName = replicaName;
	}

	public String getIp()
	{
		return ip;
	}
	
	public void setIp(String ip) 
	{
		this.ip = ip;
	}
	
	public int getPort() 
	{
		return port;
	}
	
	public void setPort(int port) 
	{
		this.port = port;
	}
	
	public int getStart() 
	{
		return start;
	}
	
	public void setStart(int start) 
	{
		this.start = start;
	}
	
	public int getEnd() 
	{
		return end;
	}
	
	public void setEnd(int end) 
	{
		this.end = end;
	}
	
	public boolean isInRange(int key)
	{
		if(key < start || key > end)
			return true;
		else
			return false;
	}

	@Override
	public String toString()
	{
		return "Replica [replicaName=" + replicaName + ", ip=" + ip + ", port="
				+ port + ", start=" + start + ", end=" + end + "]";
	}
}