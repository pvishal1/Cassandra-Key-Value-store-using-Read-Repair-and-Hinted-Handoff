
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

/**
 * A class for performing file related operations.
 */
public class FileProcessor
{
	private BufferedReader bfrReader = null;
	
	/**
	 * Creates a FileProcessor instance. It initializes it
	 *  with the input file path.
	 * @param inputFilePath A String path for the input file
	 * @throws FileNotFoundException If the input file is not found
	 */
	public FileProcessor(String inputFilePath) throws FileNotFoundException
	{
		try
		{
				bfrReader = new BufferedReader(new FileReader(inputFilePath));
		}
		catch(FileNotFoundException e)
		{
			throw e;
		}
	}
	
	/**
	 * This function reads the file one line
	 *  at a time and returns the line back to the
	 *  caller function for processing
	 * @return A single line read from the file
	 * @throws If an I/O error occurs
	 */
	public String readLine() throws IOException
	{
		String line = null;
		return ((line = bfrReader.readLine()) != null) ?  line.trim() : line;
	}
	
	/**
	 * Static function for writing to the write-ahead log file
	 * @param key The key for the entry
	 * @param value The value for the entry
	 * @param timestamp The timestamp for the entry
	 * @param logFilePath The path to the log file
	 */
	public static void writeLog(int key, String value, long timestamp, String logFilePath)
	{
		try
		{
			PrintWriter pwr = new PrintWriter(new FileWriter(new File(logFilePath), true));
			pwr.println(key + ":" + value + ":" + timestamp);
			pwr.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Function for updating the data store from
	 *  the write-ahead log file
	 * @param dataStore The data store
	 * @throws IOException When the file is not found
	 */
	public void updateDatastore(HashMap<Integer, DataStoreEntry> dataStore) throws IOException
	{
		String line;
		while((line = readLine()) != null)
		{
			String []tempArr = line.split(":");
			
			if(dataStore.containsKey(Integer.parseInt(tempArr[0])) && (dataStore.get(Integer.parseInt(tempArr[0])) == null || dataStore.get(Integer.parseInt(tempArr[0])).getTimestamp() < Long.parseLong(tempArr[2])))
			{
				DataStoreEntry tempEntry = new DataStoreEntry();
				tempEntry.setValue(tempArr[1]);
				tempEntry.setTimestamp(Long.parseLong(tempArr[2]));
				
				dataStore.put(Integer.parseInt(tempArr[0]), tempEntry);
			}
		}
	}
	
	/**
	 * Static function for checking if the write-ahead log file
	 *  exists or not. If the output file does not exists,
	 *  it returns false else returns true.
	 * @param logFilePath A String path for the log file
	 * @return Returns true if the file exists, else returns false
	 *  if the file does not exists
	 */
	public static boolean checkLogFile(String logFilePath)
	{
		//Check if the write-ahead log file already exists
		File logFile = new File(logFilePath);
		
		if(logFile.isFile())
			return true;
		else
			return false;
	}
	
	/**
	 * This function is for closing the file
	 * @throws IOException
	 */
	public void closeFile() throws IOException
	{
		try
		{
			//Closing the file
			if(bfrReader != null)
				bfrReader.close();
		}
		catch(IOException e)
		{
			throw e;
		}
	}

	@Override
	public String toString()
	{
		return "FileProcessor [bfrReader=" + bfrReader + "]";
	}
}