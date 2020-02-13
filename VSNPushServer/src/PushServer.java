import java.io.*;
import java.net.*;
import java.lang.reflect.Array;
import java.util.*;
import java.security.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PushServer extends Thread
{
	public static final int DEFAULT_PORT = 8081;
	public static final int DEFAULT_UDP_PORT = 8091;
	
	private ServerSocket server = null;
	private int thisPort = DEFAULT_PORT;
	private int ptTimeout = PullServerProxyThread.DEFAULT_TIMEOUT;
	private int debugLevel = 0;
	private PrintStream debugOut = System.out;
	private String database_driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private Connection conn;
	private float alpha = 1;
	
	private int UDPport = DEFAULT_UDP_PORT;
	UDPServerThread udpthread;
	
	/* here's a main method, in case you want to run this by itself */
	public static void main (String args[])
	{
		int port = 5556;
		int udp_port = 5601;
		
		// create and start the jProxy thread, using a 20 second timeout
		// value to keep the threads from piling up too much
		System.err.println("  **  Starting Server on port " + port + ". Press CTRL-C to end.  **\n");
		PushServer jp = new PushServer(port,udp_port, 5);
		jp.setDebug(1, System.out);		// or set the debug level to 2 for tons of output
		jp.start();
		
		// run forever; if you were calling this class from another
		// program and you wanted to stop the jProxy thread at some
		// point, you could write a loop that waits for a certain
		// condition and then calls jProxy.closeSocket() to kill
		// the running jProxy thread
		while (true)
		{
			try { Thread.sleep(3000); } catch (Exception e) {}
		}
		
		// if we ever had a condition that stopped the loop above,
		// we'd want to do this to kill the running thread
		//jp.closeSocket();
		//return;
	}
	
	
	/* the proxy server just listens for connections and creates
	 * a new thread for each connection attempt (the ProxyThread
	 * class really does all the work)
	 */

	public PushServer (int port, int udp_port)
	{
		thisPort = port;
		UDPport = udp_port;
	}
	
	public PushServer (int port, int udp_port, int timeout)
	{
		thisPort = port;
		UDPport = udp_port;
		ptTimeout = timeout;
	}
	
	
	/* allow the user to decide whether or not to send debug
	 * output to the console or some other PrintStream
	 */
	public void setDebug (int level, PrintStream out)
	{
		debugLevel = level;
		debugOut = out;
	}
	
	
	/* get the port that we're supposed to be listening on
	 */
	public int getPort ()
	{
		return thisPort;
	}
	
	public int getUDPPort()
	{
		return UDPport;
	}
	
	/* return whether or not the socket is currently open
	 */
	public boolean isRunning ()
	{
		if (server == null)
			return false;
		else
			return true;
	}
	 
	
	/* closeSocket will close the open ServerSocket; use this
	 * to halt a running jProxy thread
	 */
	public void closeSocket ()
	{
		try {
			// close the open server socket
			server.close();
		}  catch(Exception e)  { 
			if (debugLevel > 0)
				debugOut.println(e);
		}
		
		server = null;
	}
	
	
	public void run()
	{
		try {
			try {
				Class.forName(database_driver);
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
				
				return;
			}

			try {
				conn = DriverManager.getConnection("jdbc:derby:serverdatabase");
				System.out.println("Database exists");
				DatabaseMetaData dbmd = conn.getMetaData();
				String[] names = { "TABLE" };
				ResultSet rs = dbmd.getTables(null, null, null, names);
				boolean tableExists = false;
				while ((rs.next()) && (tableExists == false)) {
					if (rs.getString("TABLE_NAME").toLowerCase()
							.compareTo("serverhashlist") == 0) {
						System.out.println(rs.getString("TABLE_NAME"));
						tableExists = true;
						break;
					}
				}
				rs.close();
				if (!tableExists) {
					System.out.println("Table does not exist");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ServerHashlist (UrlHash VARCHAR(36) NOT NULL PRIMARY KEY, ObjectHash VARCHAR(36) NOT NULL, HashTime TIMESTAMP NOT NULL, Occurrence INT DEFAULT 0)");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX hashIndex on ServerHashlist(UrlHash ASC)");
				}
			} catch (SQLException e) {
				if (e.getSQLState().equals("XJ004")) { // DB not found
					System.out.println("Database does not exist, creating database and table");
					conn = DriverManager.getConnection("jdbc:derby:serverdatabase;create=true");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ServerHashlist (UrlHash VARCHAR(40) NOT NULL PRIMARY KEY, ObjectHash VARCHAR(40) NOT NULL, HashTime TIMESTAMP NOT NULL, Occurrence INT DEFAULT 0)");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX HashIndex on ServerHashlist(UrlHash ASC)");
				} else
					e.printStackTrace();
			}
			 
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				String[] names = { "TABLE" };
				ResultSet rs = dbmd.getTables(null, null, null, names);
				boolean tableExists = false;
				while ((rs.next()) && (tableExists == false)) {
					if (rs.getString("TABLE_NAME").toLowerCase()
							.compareTo("clientdatabase") == 0) {
						System.out.println(rs.getString("TABLE_NAME"));
						tableExists = true;
						break;
					}
				}
				rs.close();
				if (!tableExists) {
					System.out.println("Client database Table does not exist");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ClientDatabase (IP VARCHAR(16) NOT NULL PRIMARY KEY, Port INT DEFAULT 9000, Added TIMESTAMP NOT NULL)");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX IPIndex on ClientDatabase(IP ASC)");
				}
			} catch (SQLException e) {
				if (e.getSQLState().equals("XJ004")) { // DB not found
					System.out.println("Database does not exist, creating database and table");
					conn = DriverManager.getConnection("jdbc:derby:serverdatabase;create=true");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ClientDatabase (IP VARCHAR(16) NOT NULL PRIMARY KEY, Port INT DEFAULT 9000, Added TIMESTAMP NOT NULL)");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX IPIndex on ClientDatabase(IP ASC)");
				} else
					e.printStackTrace();
			}
			// create a server socket, and loop forever listening for
			// client connections
			server = new ServerSocket(thisPort);
			if (debugLevel > 0)
				debugOut.println("Started server on port " + thisPort);
			
			try{
				System.out.println("Starting UDP server");
				udpthread = new UDPServerThread(UDPport,conn);
				udpthread.start();
			}
			catch(SocketException e){
				System.out.println("Could not create UDP server socket");
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
			while (true)
			{
				Socket client = server.accept();
				String clientip = client.getInetAddress().toString();
				try { 		
					Statement stmt2 = conn.createStatement();
					ResultSet rs = stmt2.executeQuery("select * from ClientDatabase where IP='"+clientip+"'");
					if (rs.next()) {
						System.out.println("Client entry exists -  IP: " + rs.getString(1));
					} else {
						PreparedStatement psInsert = conn.prepareStatement("insert into ClientDatabase values (?,?,?)");

						psInsert.setString(1, clientip);
						psInsert.setInt(2, DEFAULT_UDP_PORT);
						java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
						psInsert.setTimestamp(3, currentTimestamp);

						psInsert.executeUpdate();
						
						udpthread.sendoldhashes(clientip, DEFAULT_UDP_PORT);
					}
					rs.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				PullServerProxyThread t = new PullServerProxyThread(client, conn, udpthread);
				t.setDebug(debugLevel, debugOut);
				t.setTimeout(ptTimeout);
				t.start();
			}
		}  catch (Exception e)  {
			if (debugLevel > 0)
				debugOut.println("VSNServer Proxy Thread error: " + e);
		}
		
		closeSocket();
	}
	
}

class UDPServerThread extends Thread
{
	public static final int DEFAULT_UDP_PORT = 9000;
	private int UDPport = DEFAULT_UDP_PORT;
	private DatagramSocket UDPserver = null;
	
	private Connection database_conn;
	
	public UDPServerThread(int port, Connection conn) throws SocketException{
		UDPport = port;
		database_conn = conn;
		
		UDPserver = new DatagramSocket(UDPport);
	}
	
	public int getUDPPort()
	{
		return UDPport;
	}
	
	public void closeserver ()
	{
		try {
			UDPserver.close();
		}  catch(Exception e)  { 
			e.printStackTrace();
		}	
		UDPserver = null;
	}
	
	public void run(){
		byte[] receiveData; 
	    
		while(true){
			receiveData = new byte[1024];
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			try{
				UDPserver.receive(receivePacket);
				String IPAddress = new String(receivePacket.getAddress().getAddress()); 
				int port = receivePacket.getPort(); 
				
				try { 		
					Statement stmt2 = database_conn.createStatement();
					ResultSet rs = stmt2.executeQuery("select * from ClientDatabase where IP='"+IPAddress+"'");
					if (! rs.next()) {
						PreparedStatement psInsert = database_conn.prepareStatement("insert into ClientDatabase values (?,?,?)");

						psInsert.setString(1, IPAddress);
						psInsert.setInt(2, port);
						java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
						psInsert.setTimestamp(3, currentTimestamp);

						psInsert.executeUpdate();
						
						sendoldhashes(IPAddress, port);
					}
					rs.close();
				} catch (SQLException e) {
					System.out.println("Unable to add client details for the UDP packet with IP:"+IPAddress+" port:"+port);
					e.printStackTrace();
				}
			}
			catch(IOException e){
				System.out.println("Unable to receive UDP packet");
				e.printStackTrace();
			}
		}
	}
	
	public void sendoldhashes(String ip, int port){
		try { 		
			Statement stmt2 = database_conn.createStatement();
			ResultSet rs = stmt2.executeQuery("select * from ServerHashlist");
			int num = 0;
			int MAX_per_packet = 30; //considering 42 bytes (2 sha1 digests+tab+newline) per hash entry, and max 1400 MTU for the UDP packet
			String sendhashdata = "";
			while (rs.next()) {
				System.out.println("ENTRY EXISTS -  UrlHash: " + rs.getString(1)
						+ " ObjectHash:" + rs.getString(2) );
				sendhashdata += rs.getString(1)+"\t"+rs.getString(2)+"\n";
				num++;
				if(num==MAX_per_packet){
					byte [] sendData = sendhashdata.getBytes();
					DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,InetAddress.getByAddress(ip.getBytes()),port);
					UDPserver.send(sendPacket);
					sendhashdata = "";
					num = 0;
				}
			}
			if(num>0){
				byte [] sendData = sendhashdata.getBytes();
				DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,InetAddress.getByAddress(ip.getBytes()),port);
				UDPserver.send(sendPacket);
				sendhashdata = "";
				num = 0;
			}
			
			rs.close();
		} catch (Exception e) {
			System.out.println("Exception in sending old hashes to IP:"+ip);
			e.printStackTrace();
		}
	}
	
	public void sendhashtoall(String newhash){
		String clientip;
		int port;
		
		try { 		
			Statement stmt = database_conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from ClientDatabase");
			while(rs.next()) {
				clientip = rs.getString(1);
				port = rs.getInt(2);
				byte [] sendData = newhash.getBytes();
				try{
					DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,InetAddress.getByAddress(clientip.getBytes()),port);
					UDPserver.send(sendPacket);
				}
				catch(Exception e){
					System.out.println("Something wrong in pushing new hash entry to a user");
					e.printStackTrace();
				}
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("Unable to read data from clientdatabase while sending new hash entry to all users");
			e.printStackTrace();
		}
	}
}
/* 
 * The ProxyThread will take an HTTP request from the client
 * socket and send it to either the server that the client is
 * trying to contact, or another proxy server
 */
class PullServerProxyThread extends Thread
{
	private Socket pSocket;
	private int debugLevel = 0;
	private PrintStream debugOut = System.out;
	private Connection conn;
	private UDPServerThread udpthread;
	
	// the socketTimeout is used to time out the connection to
	// the remote server after a certain period of inactivity;
	// the value is in milliseconds -- use zero if you don't want 
	// a timeout
	public static final int DEFAULT_TIMEOUT = 20 * 1000;
	private int socketTimeout = DEFAULT_TIMEOUT;
	
	
//	public PullServerProxyThread(Socket s)
//	{
//		pSocket = s;
//	}

	public PullServerProxyThread(Socket s, Connection con2, UDPServerThread udp)
	{
		pSocket = s;
		conn = con2;
		udpthread = udp;
	}
	
	
	public void setTimeout (int timeout)
	{
		// assume that the user will pass the timeout value
		// in seconds (because that's just more intuitive)
		socketTimeout = timeout * 1000;
	}


	public void setDebug (int level, PrintStream out)
	{
		debugLevel = level;
		debugOut = out;
	}


	public void run()
	{
		try
		{
			long startTime = System.currentTimeMillis();
			
			// client streams (make sure you're using streams that use
			// byte arrays, so things like GIF and JPEG files and file
			// downloads will transfer properly)
			BufferedInputStream clientIn = new BufferedInputStream(pSocket.getInputStream());
			BufferedOutputStream clientOut = new BufferedOutputStream(pSocket.getOutputStream());
			
			// the socket to the remote server
			Socket server = null;
			
			// other variables
			byte[] request = null;
			byte[] response = null;
			int requestLength = 0;
			int responseLength = 0;
			int pos = -1;
			StringBuffer host = new StringBuffer("");
			String hostName = "";
			String url_string = "";
			int hostPort = 80;
			StringBuffer url = new StringBuffer("");
			StringBuffer contactedorigin = new StringBuffer("");
			
			// get the header info (the web browser won't disconnect after
			// it's sent a request, so make sure the waitForDisconnect
			// parameter is false)
			request = getHTTPData(clientIn, host, url, contactedorigin, false);
			requestLength = Array.getLength(request);
			
			// separate the host name from the host port, if necessary
			// (like if it's "servername:8000")
			hostName = host.toString();
			url_string = url.toString();
			pos = hostName.indexOf(":");
			if (pos > 0)
			{
				try { hostPort = Integer.parseInt(hostName.substring(pos + 1)); 
					}  catch (Exception e)  { }
				hostName = hostName.substring(0, pos);
			}
			System.out.println("gotrequest url:"+url+"\nhost:"+host+"\nhostport:"+hostPort);			
	    	
			try
			{
				server = new Socket(hostName, hostPort);
			}  catch (Exception e)  {
				// tell the client there was an error
				System.out.println("Error connecting to the server");
				String errMsg_body = "<html><head><title>503 Service Temporarily Unavailable</title></head>"+
				        "<body><h1>VSN ERROR MESSAGE: Service Temporarily Unavailable</h1>"+
				        "<p>The server("+hostName+") is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.</p>"+
				        "</body></html>\r\n";
				String errMsg_header = "HTTP/1.1 503 Service Unavailable\r\n"+
		                "Content-Type: text/html\r\n"+
						"Content-Length: "+errMsg_body.length()+"\r\n"+
				        "Connection: close\r\n\r\n";
				String errMsg = errMsg_header+errMsg_body;        
				clientOut.write(errMsg.getBytes(), 0, errMsg.length());
				clientOut.flush();
			}
			
			if (server != null)
			{
				server.setSoTimeout(socketTimeout);
				BufferedInputStream serverIn = new BufferedInputStream(server.getInputStream());
				BufferedOutputStream serverOut = new BufferedOutputStream(server.getOutputStream());
				try{
					serverOut.write(request, 0, requestLength);
					serverOut.flush();
				
					responseLength = streamHTTPData2(serverIn, clientOut,host,url, true);
				}
				catch (SocketTimeoutException ste)
				{
					System.out.println ("Socket timeout occurred - killing connection");
					String errMsg_body = "<html><head><title>504 Gateway Time-out</title></head>"+
					        "<body><h1>VSN ERROR MESSAGE: Gateway Time-out</h1>"+
					        "<p>Connecting to the server("+hostName+") timedout as the server was not responsive. Please try again later.</p>"+
					        "</body></html>\r\n";
					String errMsg_header = "HTTP/1.0 504 Gateway Time-out\r\n"+
			                "Content-Type: text/html\r\n"+
							"Content-Length: "+errMsg_body.length()+"\r\n"+
					        "Connection: close\r\n\r\n";
					String errMsg = errMsg_header+errMsg_body;
//					String errMsg = "HTTP/1.0 504 Gateway Time-out\r\nContent-Type: text/html\r\n\r\n" + 
//							"<html><body>Error connecting to the server:\n" + ste + "\n</body></html>\r\n";
					clientOut.write(errMsg.getBytes(), 0, errMsg.length());
				}
				
				serverIn.close();
				serverOut.close();
			}
			
			
			// if the user wants debug info, send them debug info; however,
			// keep in mind that because we're using threads, the output won't
			// necessarily be synchronous
			if (debugLevel > 0)
			{
				long endTime = System.currentTimeMillis();
				debugOut.println("Request from " + pSocket.getInetAddress().getHostAddress() + 
									" on Port " + pSocket.getLocalPort() + 
									" to host " + hostName + ":" + hostPort + 
									"\n  "+url_string+
									"\n"+"(" + requestLength + " bytes sent, " + 
									responseLength + " bytes returned, " + 
									Long.toString(endTime - startTime) + " ms elapsed)");
				debugOut.flush();
			}
			
			// close all the client streams so we can listen again
			clientOut.close();
			clientIn.close();
			pSocket.close();
		}  catch (Exception e)  {
			if (debugLevel > 0){
				debugOut.println("Error in ServerProxyThread: " + e);
				e.printStackTrace();
			}
		}

	}
	
	private byte[] getHTTPData (InputStream in, StringBuffer host, StringBuffer url, StringBuffer contactedorigin, boolean waitForDisconnect)
	{
		// get the HTTP data from an InputStream, and return it as
		// a byte array, and also return the Host entry in the header,
		// if it's specified -- note that we have to use a StringBuffer
		// for the 'host' variable, because a String won't return any
		// information when it's used as a parameter like that
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		streamHTTPData(in, bs, host, url, contactedorigin, waitForDisconnect);
		return bs.toByteArray();
	}
	
	private int streamHTTPData(InputStream in, OutputStream out,
			StringBuffer host, StringBuffer url, StringBuffer contactedorigin, boolean waitForDisconnect) {
		// get the HTTP data from an InputStream, and send it to
		// the designated OutputStream
		StringBuffer header = new StringBuffer("");
		StringBuffer temp_header = new StringBuffer("");
		String data = "";
		StringBuffer pre_url= new StringBuffer("");
		StringBuffer post_url = new StringBuffer("");
		int responseCode = 200;
		int contentLength = 0;
		int pos = -1;
		int byteCount = 0;

		try {
			// get the first line of the header, so we know the response code
			data = readLine(in);
			if (data != null) {
				//temp_header.append(data + "\r\n");
				pos = data.indexOf(" ");
//				if ((data.toLowerCase().startsWith("http")) && (pos >= 0)
//						&& (data.indexOf(" ", pos + 1) >= 0)) {
//					String rcString = data.substring(pos + 1,
//							data.indexOf(" ", pos + 1));
//					try {
//						responseCode = Integer.parseInt(rcString);
//					} catch (Exception e) {
//						if (debugLevel > 0)
//							debugOut.println("Error parsing response code "
//									+ rcString);
//					}
//				} else {
					if ((pos >= 0) && (data.indexOf(" ", pos + 1) >= 0)) {
						pre_url.setLength(0);
						pre_url.append(data.substring(0,pos));
						url.setLength(0);
						url.append(data.substring(pos + 1,data.indexOf(" ", pos + 1)));
						post_url.setLength(0);
						post_url.append(data.substring(data.indexOf(" ", pos + 1)+1));
					}
				//}
			}

			// get the rest of the header info
			while ((data = readLine(in)) != null) {
				// the header ends at the first blank line
				if (data.length() == 0)
					break;
				

				// check for the Host header
				pos = data.toLowerCase().indexOf("host:");
				if (pos >= 0) {
					host.setLength(0);
					host.append(data.substring(pos + 5).trim());
				}

				// check for the Content-Length header
				pos = data.toLowerCase().indexOf("content-length:");
				if (pos >= 0)
					contentLength = Integer.parseInt(data.substring(pos + 15)
							.trim());
				
				if(data.indexOf("VSNContactedOrigin:")>=0){
					contactedorigin.setLength(0);
					contactedorigin.append(data.substring(data.indexOf("VSNContactedOrigin:")+19).trim());
					continue;
				}
				temp_header.append(data + "\r\n");
			}
			
			if(url.toString().startsWith(host.toString()) || url.toString().startsWith("http://"+host.toString())){
				String newurl = url.substring(url.toString().indexOf(host.toString())+host.length());
				url.setLength(0);
				url.append(newurl);
			}
			
			// add a blank line to terminate the header info
			header.append(pre_url+" "+url+" "+post_url+"\r\n"+temp_header+"\r\n");
			
			// convert the header to a byte array, and write it to our stream
			out.write(header.toString().getBytes(), 0, header.length());
			// if the header indicated that this was not a 200 response,
			// just return what we've got if there is no Content-Length,
			// because we may not be getting anything else
			if ((responseCode != 200) && (contentLength == 0)) {
				out.flush();
				return header.length();
			}

			// get the body, if any; we try to use the Content-Length header to
			// determine how much data we're supposed to be getting, because
			// sometimes the client/server won't disconnect after sending us
			// information...
			if (contentLength > 0)
				waitForDisconnect = false;

			if ((contentLength > 0) || (waitForDisconnect)) {
				try {
					byte[] buf = new byte[4096];
					int bytesIn = 0;
					while (((byteCount < contentLength) || (waitForDisconnect))
							&& ((bytesIn = in.read(buf)) >= 0)) {
						out.write(buf, 0, bytesIn);
						out.flush();
						byteCount += bytesIn;
					}
				} catch (Exception e) {
					String errMsg = "request Error getting HTTP body: " + e;
					e.printStackTrace();
					if (debugLevel > 0)
						debugOut.println(errMsg);
					// bs.write(errMsg.getBytes(), 0, errMsg.length());
				}
			}
		} catch (Exception e) {
			if (debugLevel > 0)
				debugOut.println("Streamhttp1 Error getting HTTP data: " + e);
		}

		// flush the OutputStream and return
		try {
			out.flush();
		} catch (Exception e) {
		}
		return (header.length() + byteCount);
	}
	
	private int streamHTTPData2 (InputStream in, OutputStream out, 
									StringBuffer host, StringBuffer url, boolean waitForDisconnect)
	{
		// get the HTTP data from an InputStream, and send it to
		// the designated OutputStream
		StringBuffer header = new StringBuffer("");
		String data = "";
		int responseCode = 200;
		int contentLength = 0;
		String contentType = "";
		int pos = -1;
		int byteCount = 0;
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		
		try
		{
			// get the first line of the header, so we know the response code
			data = readLine(in);
			if (data != null)
			{
				header.append(data + "\r\n");
				pos = data.indexOf(" ");
				if ((data.toLowerCase().startsWith("http")) && 
					(pos >= 0) && (data.indexOf(" ", pos+1) >= 0))
				{
					String rcString = data.substring(pos+1, data.indexOf(" ", pos+1));
					try
					{
						responseCode = Integer.parseInt(rcString);
					}  catch (Exception e)  {
						if (debugLevel > 0)
							debugOut.println("Error parsing response code " + rcString);
					}
				}
			}
			
			// get the rest of the header info
			while ((data = readLine(in)) != null)
			{
				// the header ends at the first blank line
				if (data.length() == 0)
					break;
				header.append(data + "\r\n");
				
				// check for the Host header
				pos = data.toLowerCase().indexOf("host:");
				if (pos >= 0)
				{
					host.setLength(0);
					host.append(data.substring(pos + 5).trim());
				}
				
				// check for the Content-Length header
				pos = data.toLowerCase().indexOf("content-length:");
				if (pos >= 0)
					contentLength = Integer.parseInt(data.substring(pos + 15).trim());
				
				// check for the Content-Type header
				pos = data.toLowerCase().indexOf("content-type:");
				if (pos >= 0)
					contentType = data.substring(pos + 13).trim();
			}
			
			// add a blank line to terminate the header info
			header.append("\r\n");
			
			// convert the header to a byte array, and write it to our stream
			out.write(header.toString().getBytes(), 0, header.length());
			out.flush();
			
			// if the header indicated that this was not a 200 response,
			// just return what we've got if there is no Content-Length,
			// because we may not be getting anything else
			if ((responseCode != 200) && (contentLength == 0))
			{
				out.flush();
				return 0;
			}
            
			// get the body, if any; we try to use the Content-Length header to
			// determine how much data we're supposed to be getting, because 
			// sometimes the client/server won't disconnect after sending us
			// information...
			if (contentLength > 0)
				waitForDisconnect = false;
			
			if ((contentLength > 0) || (waitForDisconnect))
			{
				//System.out.println("Trying to get content body");
				try {
					byte[] buf = new byte[4096];
					int bytesIn = 0;
					while ( ((byteCount < contentLength) || (waitForDisconnect)) 
							&& ((bytesIn = in.read(buf)) >= 0) )
					{
						byteCount += bytesIn;
						if(contentLength > 0){
							if(contentType.toLowerCase().contains("video")){
								out.write(buf, 0, bytesIn);
								out.flush();
							}
							else{
								bs.write(buf, 0, bytesIn);
								out.write(buf, 0, bytesIn);
								out.flush();
								
								if(byteCount>=contentLength){
									byte [] response = bs.toByteArray();
									
									MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
																		
									//sha1.update(response, 0, response.length);
									byte [] obdigest = sha1.digest(response);
									sha1.reset();
									//sha1.update(url_string.getBytes(), 0,
									//		url_string.length());
									byte [] udigest = sha1.digest((host.toString()+url.toString()).getBytes());
									
									//convert the byte to hex format method 2
							        StringBuffer objectdigest = new StringBuffer();
							    	for (int i=0;i<obdigest.length;i++) {
							    		objectdigest.append(Integer.toHexString(0xFF & obdigest[i]));
							    	}
							    	
							    	StringBuffer urldigest = new StringBuffer();
							    	for (int i=0;i<udigest.length;i++) {
							    		urldigest.append(Integer.toHexString(0xFF & udigest[i]));
							    	}
							    	
							    	//System.out.println(url.toString());
									//System.out.println("calculated digets before database insertion Digests (url:object): " + urldigest + " : "
									//		+ objectdigest);
									
									try { 		
										Statement stmt2 = conn.createStatement();
										ResultSet rs = stmt2.executeQuery("select * from ServerHashlist where UrlHash='"+urldigest+"'");
										if (rs.next()) {
											//System.out.println("ENTRY EXISTS -  UrlHash: " + rs.getString(1)
											//		+ " ObjectHash:" + rs.getString(2) + " Time:"
											//		+ rs.getString(3)+ " Count:"+rs.getInt(4));
											String old_objecthash = rs.getString(2);
											int occur = rs.getInt(4);
																						
											PreparedStatement psUpdate = conn.prepareStatement("UPDATE ServerHashlist SET Occurrence=?, ObjectHash=?, hashTime=? WHERE UrlHash=?");
											psUpdate.setInt(1, (occur+1));
											psUpdate.setString(2, objectdigest.toString());
											java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
											psUpdate.setTimestamp(3, currentTimestamp);
											psUpdate.setString(4, urldigest.toString());
											psUpdate.executeUpdate();
											
											if(!old_objecthash.equalsIgnoreCase(objectdigest.toString())){
												udpthread.sendhashtoall(urldigest.toString()+"\t"+objectdigest.toString());
											}
										} else {
											PreparedStatement psInsert = conn
													.prepareStatement("insert into ServerHashlist values (?,?,?,?)");

											psInsert.setString(1, urldigest.toString());
											psInsert.setString(2, objectdigest.toString());
											java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
											psInsert.setTimestamp(3, currentTimestamp);
                                            psInsert.setInt(4, 0);
											psInsert.executeUpdate();
											
											udpthread.sendhashtoall(urldigest.toString()+"\t"+objectdigest.toString());
										}
										rs.close();
										
									} catch (Exception e) {
										e.printStackTrace();
									}			
								}
							}
							
						}
						else{
							//bs.write(buf, 0, bytesIn);
							out.write(buf, 0, bytesIn);
							out.flush();
						}	
					}
				}  catch (Exception e)  {
					String errMsg = "Streamhttp2 Error getting HTTP body: " + e;
					e.printStackTrace();
					if (debugLevel > 0)
						debugOut.println(errMsg);
					//bs.write(errMsg.getBytes(), 0, errMsg.length());
				}
			}
		}  catch (Exception e)  {
			if (debugLevel > 0)
				debugOut.println("Error getting HTTP data: " + e);
		}
		
		//flush the OutputStream and return
		try  {  out.flush();  }  catch (Exception e)  {}
		return bs.size();
	}
		
	private String readLine (InputStream in)
	{
		// reads a line of text from an InputStream
		StringBuffer data = new StringBuffer("");
		int c;
		
		try
		{
			// if we have nothing to read, just return null
			in.mark(1);
			if (in.read() == -1)
				return null;
			else
				in.reset();
			
			while ((c = in.read()) >= 0)
			{
				// check for an end-of-line character
				if ((c == 0) || (c == 10) || (c == 13))
					break;
				else
					data.append((char)c);
			}
		
			// deal with the case where the end-of-line terminator is \r\n
			if (c == 13)
			{
				in.mark(1);
				if (in.read() != 10)
					in.reset();
			}
		}  catch (Exception e)  {
			if (debugLevel > 0)
				debugOut.println("Error getting header: " + e);
		}
		
		// and return what we have
		return data.toString();
	}
	
}

