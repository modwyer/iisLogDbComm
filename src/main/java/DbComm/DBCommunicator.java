package DbComm;


public class DBCommunicator 
{
	//
	//PRIVATE
	//
	private DBWriterThread writerThread = DBWriterThread.getWriter();
	//
	//PUBLIC
	//
	public DBCommunicator() 
	{
		System.out.println("IIS Log DB Communicator started...");		
	}
	
	public void process_file(String filename) 
	{		
		this.writerThread.write_file(filename);
	}
}
