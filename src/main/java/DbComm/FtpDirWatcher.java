package DbComm;


import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;


public class FtpDirWatcher {

	/**
	 * @param args
	 */
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws IOException,
	InterruptedException {
		
		Path ftpstor = Paths.get("/home/bcftp/ftpstor/");
		WatchService watchService = FileSystems.getDefault().newWatchService();
		ftpstor.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);		
		
		System.out.println("*******************************************");
		System.out.println("IIS Log DB Communicator");
		System.out.println("*******************************************");
		System.out.println("DirWatcher watching: " + ftpstor + "...");
		System.out.println("Starting db communicator...");
		
		DBCommunicator dbComm = new DBCommunicator();		
		
		boolean valid = true;
		do {
			WatchKey watchKey = watchService.take();

			for (WatchEvent event : watchKey.pollEvents()) {
				WatchEvent.Kind kind = event.kind();
				if (StandardWatchEventKinds.ENTRY_CREATE.equals(kind)) {
					String fileName = event.context().toString();
					System.out.println("(New File Found:" + fileName + ")");
					
//					String response = dbComm.process_file(ftpstor + "/" + fileName);
					dbComm.process_file(ftpstor + "/" + fileName);
										
//					System.out.println(response);
				}
			}			
			valid = watchKey.reset();

		} while (valid);
	}
}
