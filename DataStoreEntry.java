
/**
 * Class representing a single entry in the data store 
 *
 */
public class DataStoreEntry
{
	private String value;
	private long timestamp;
	
	public String getValue()
	{
		return value;
	}
	
	public void setValue(String value) 
	{
		this.value = value;
	}
	
	public long getTimestamp() 
	{
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) 
	{
		this.timestamp = timestamp;
	}

	@Override
	public String toString() 
	{
		return "DataStoreEntry [value=" + value + ", timestamp=" + timestamp
				+ "]";
	}
}