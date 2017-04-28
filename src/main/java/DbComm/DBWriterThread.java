package DbComm;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.time.StopWatch;

import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;
import org.neo4j.rest.graphdb.RestResultException;

public class DBWriterThread extends Thread {
	//
	// PRIVATE
	//
	static Logger log = Logger.getLogger(DBWriterThread.class);
	
	private static final String SHUTDOWN_REQ = "SHUTDOWN";
	private static final String DB_PATH = "http://localhost:7474/db/data";
	
	private volatile boolean shuttingDown, writerTerminated;
	
	private BlockingQueue<String> filesQueue = 
			new LinkedBlockingQueue<String>();
	
	private static StopWatch stopwatch = new StopWatch();
	
	private static final DBWriterThread instance = new DBWriterThread();
		
	private static RestAPI restapi;
	private static RestCypherQueryEngine rcqer;
	
	
	private DBWriterThread() {
		print("Starting DBWriter Thread instance...");
		print("Starting database...");
		
		BasicConfigurator.configure();	// Logging

		restapi = new RestAPIFacade( DB_PATH );
		rcqer = new RestCypherQueryEngine(restapi);
		registerShutdownHook( restapi );
				
		start();
	}
	
	private static void registerShutdownHook( final RestAPI db )
	{
		// Registers a shutdown hook for the Neo4j and index service instances
		// so that it shuts down nicely when the VM exits (even if you
		// "Ctrl-C" the running example before it's completed)
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				db.close();
			}
		} );
	}
	
	/**
	 * Process the csv file and attempt to load the rows of the
	 * csv file into the database.
	 * @param filename
	 * @return
	 */
	private String process_file(String filename) {
		String retval = "Error!";		
		String prefix = get_filename_prefix(filename);
		String query = get_query(prefix, filename);
		
		try {
			stopwatch.start();

			// Perform DB operations
			QueryResult<Map<String,Object>> queryResult = 
					rcqer.query(query, org.neo4j.helpers.collection.MapUtil.map("null",null));
					
		} catch (RestResultException ex) {
			System.out.println("ex: " + ex.getLocalizedMessage());
		} finally {
			retval = "Done!";			
		}
		
		stopwatch.stop();
		print("stopwatch:" + stopwatch);
		stopwatch.reset();
		
		return retval;
	}
	
	/**
	 * Helper method so I don't have to write System.out.println
	 * every time I want to print to console.
	 * @param output
	 */
	private static void print(String output) {
		System.out.println(output);
	}
	
	/**
	 * Helper method to hold and give out the big queries and make
	 * the calling code cleaner.
	 * @param prefix : The first three letters of the UriStem of the log.
	 * @param filename : The name of the csv file.
	 * @return A query to use to load the csv file to database.
	 */
	private static String get_query(String prefix, String filename) {
		String retval = "";
		if (prefix.equals("cmu")) {
			retval = "USING PERIODIC COMMIT " +
					"LOAD CSV WITH HEADERS FROM " +
					"'file:///" + filename + "' AS csvLine " +
					"WITH csvLine, " +
					"SPLIT(csvLine.date, '-') AS date, " +
					"SPLIT(csvLine.e, '/') AS expDate, " +
					"SPLIT(csvLine.time, ':') AS time " +
					"MERGE (s:SerialNumber { name: TOINT(csvLine.s) }) " +
					"MERGE (x:BCExpDate { name: csvLine.e }) " +
					"MERGE (ip:IPAddress { name: csvLine.clientIp }) " +
					"MERGE (v:BCVersion { name: TOINT(csvLine.v) }) " +
					"MERGE (r:BCRelease { name: TOINT(csvLine.r) }) " +
					"MERGE (crid:CrpUid { name: csvLine.crid }) " +
					"MERGE (cmu:CMUpdate { name: csvLine.key }) " +
					"MERGE (crwho:CRWho { name: csvLine.crwho }) " +
					"MERGE (crtype:CRType { name: csvLine.crtyp }) " +
					"MERGE (ua:UserAgent { name: csvLine.userAgent }) " +
					"MERGE (ip)-[:FROM]->(s) " +
					"MERGE (s)-[:HAS]->(x) " +
					"MERGE (ip)-[ipr:USED]->(r) " +
					"MERGE (ip)-[ipv:USED]->(v) " +
					"MERGE (crid)-[:USED]->(ip) " +
					"MERGE (ip)-[ipcmu:ORDERED]->(cmu) " +
					"MERGE (crid)-[cridcmu:ORDERED]->(cmu) " +
					"MERGE (cmu)-[:ORDERED_FROM]->(crwho) " +
					"MERGE (cmu)-[:HAS]->(crtype) " +
					"MERGE (cmu)-[:USED]->(ua) " +
					"SET s.alias = COALESCE(csvLine.n, \"none\") " +
					"SET s.phone = COALESCE(csvLine.p, \"none\") " +
					"SET x.year = COALESCE(expDate[2], \"none\") " +
					"SET x.month = COALESCE(expDate[1], \"none\") " +
					"SET x.day = COALESCE(expDate[0], \"none\") " +
					"SET ip.client_count = COALESCE(csvLine.c, \"none\") " +
					"SET ipr.year = COALESCE(date[0], \"none\") " +
					"SET ipr.month = COALESCE(date[1], \"none\") " +
					"SET ipr.day = COALESCE(date[2], \"none\") " +
					"SET ipv.year = COALESCE(date[0], \"none\") " +
					"SET ipv.month = COALESCE(date[1], \"none\") " +
					"SET ipv.day = COALESCE(date [2], \"none\") " +
					"SET crid.uid = COALESCE(csvLine.crsi, \"none\") " +
					"SET ipcmu.year = COALESCE(date[0], \"none\") " +
					"SET ipcmu.month = COALESCE(date[1], \"none\") " +
					"SET ipcmu.day = COALESCE(date[2], \"none\") " +
					"SET ipcmu.hour = COALESCE(time[0], \"none\") " +
					"SET ipcmu.minute = COALESCE(time[1], \"none\") " +
					"SET ipcmu.second = COALESCE(time[2], \"none\") " +
					"SET ipcmu.wuid = COALESCE(csvLine.uid, \"none\") " +
					"SET cmu.pdclr = COALESCE(csvLine.pdclr, \"none\") " +
					"SET cridcmu.year = COALESCE(date[0], \"none\") " +
					"SET cridcmu.month = COALESCE(date[1], \"none\") " +
					"SET cridcmu.day = COALESCE(date[2], \"none\") ";
		}

		return retval;
	}
	
	/**
	 * Get the first part of the filename where each part
	 * of the filename is separated by an underscore.
	 * @param filename : Filename to parse.
	 * @return The three letter prefix of this file.
	 */
	private static String get_filename_prefix(String filename) {
		if (!filename.contains("_")) { return ""; }
		
		String[] dirs = filename.split("/");		// Parse out filepath.
		String[] name_parts = dirs[4].split("_");	// Parse out filename.
		return name_parts[0];
	}
	
	//
	// PUBLIC
	//	
	public static DBWriterThread getWriter() {
		return instance;
	}
	

	public void run() {
		try {			
			String file;
			while ((file = filesQueue.take()) != SHUTDOWN_REQ) {
				System.out.println("Next file: " + file);
				System.out.println("Processing...");
				
				String response = this.process_file(file);
				
				System.out.println(response);
			}
		} catch (Exception ex) {
			System.out.println("run.ex:" + ex.getLocalizedMessage());
		} finally {
			System.out.println("Leaving run of DBWriterThread...");
		}
	}
	
	/**
	 * Stores the 'filename' into the queue so it can be processed.
	 * @param filename
	 */
	public void write_file(String filename) {
		if (shuttingDown || writerTerminated) return;
		try {
			filesQueue.put(filename);
		} catch (InterruptedException iex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Unexpected interruption");
		}
	}
}