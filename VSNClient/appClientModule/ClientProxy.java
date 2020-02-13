import java.io.*;
import java.net.*;
import java.lang.reflect.Array;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.TimerTask;

public class ClientProxy extends Thread
{
	public static final int DEFAULT_PORT = 8080;
	public static final int DEFAULT_UDP_PORT = 8090;
	
	private ServerSocket server = null;
	private int thisPort = DEFAULT_PORT;
	private String fwdServer = "";
	private int fwdPort = 0;
	private int fwdUDPport = 0;
	private int ptTimeout = ProxyThread.DEFAULT_TIMEOUT;
	private FileOutputStream out;
	private PrintStream debugOut = System.out;
	private String database_driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private Connection conn;
	
	private int UDPport = DEFAULT_UDP_PORT;
	private UDPClientThread udpthread;
	PublicKey publickey = null;
	
	/* here's a main method, in case you want to run this by itself */
	public static void main (String args[])
	{
		int port = 5555;
		String fProxyServer = "127.0.0.1";
		int fProxyPort = 5556;
		int uport = 5600;
		int fUdpPort = 5601;
		
		if(args.length >= 1){
			port = Integer.parseInt(args[0]);
		}
		if(args.length >= 3){
			fProxyServer = args[1];
			fProxyPort = Integer.parseInt(args[2]);
		}
		if(args.length >= 4){
			uport = Integer.parseInt(args[3]);
		}
		if(args.length >= 5){
			fUdpPort = Integer.parseInt(args[4]);
		}
		
		String forward_ip = "";
		try{
			forward_ip = InetAddress.getByName(fProxyServer).getHostAddress();
		}catch(UnknownHostException e){
			System.out.println("UnknownHostException: provided forward proxy server is not valid");
			e.printStackTrace();
			return;
		}catch(Exception e){
			e.printStackTrace();
			return;
		}
		fProxyServer = forward_ip;
		
		// create and start the ClientProxy thread, using a 20 second timeout
		// value to keep the threads from piling up too much
		System.err.println("  **  Starting ClientProxy on port " + port + ". Press CTRL-C to end.  **\n");
		ClientProxy cp = new ClientProxy(port, fProxyServer, fProxyPort, 5,uport, fUdpPort);
		//cp.setDebug(1, debugOut);		// or set the debug level to 2 for tons of output
		try{
			String path = ClientProxy.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			path = URLDecoder.decode(path, "UTF-8");
			System.out.println(path);
			cp.publickey = cp.LoadPublicKey(path,"DSA");
			System.out.println("Got the server's public key");
		}catch(Exception e){
			System.out.println("Unable to read server's public key");
		}
		cp.start();
		
		
		// run forever; if you were calling this class from another
		// program and you wanted to stop the ClientProxy thread at some
		// point, you could write a loop that waits for a certain
		// condition and then calls ClientProxy.closeSocket() to kill
		// the running ClientProxy thread
		while (true)
		{
			try { Thread.sleep(3000); } catch (Exception e) {}
		}
		
		// if we ever had a condition that stopped the loop above,
		// we'd want to do this to kill the running thread
		//cp.closeSocket();
		//return;
	}
	
	
	/* the proxy server just listens for connections and creates
	 * a new thread for each connection attempt (the ProxyThread
	 * class really does all the work)
	 */
	public ClientProxy (int port)
	{
		thisPort = port;
		try{
			out = new FileOutputStream("serverlog.txt", true);
			debugOut = new PrintStream(out);
		}catch(Exception e){
			debugOut = System.out;
			System.out.println("Could not initialize server log file");
		}
	}
	
	public ClientProxy (int port, String proxyServer, int proxyPort)
	{
		thisPort = port;
		fwdServer = proxyServer;
		fwdPort = proxyPort;
		try{
			out = new FileOutputStream("serverlog.txt", true);
			debugOut = new PrintStream(out);
		}catch(Exception e){
			debugOut = System.out;
			System.out.println("Could not initialize server log file");
		}
	}
	
	public ClientProxy (int port, String proxyServer, int proxyPort, int timeout, int udp, int fUdpPort)
	{
		thisPort = port;
		fwdServer = proxyServer;
		fwdPort = proxyPort;
		ptTimeout = timeout;
		UDPport = udp;
		fwdUDPport = fUdpPort;
		try{
			out = new FileOutputStream("clientlog.txt", true);
			debugOut = new PrintStream(out);
		}catch(Exception e){
			debugOut = System.out;
			System.out.println("Could not initialize client log file");
		}
	}
	
	public PublicKey LoadPublicKey(String path, String algorithm)
			throws IOException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		// Read Public Key.
		File filePublicKey = new File(path + "/public.key");
		FileInputStream fis = new FileInputStream(path + "/public.key");
		byte[] encodedPublicKey = new byte[(int) filePublicKey.length()];
		fis.read(encodedPublicKey);
		fis.close();
 
		// Generate KeyPair.
		KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
		X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(
				encodedPublicKey);
		PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
		return publicKey;
	}
	
	/* allow the user to decide whether or not to send debug
	 * output to the console or some other PrintStream
	 */
	public void setDebug (PrintStream out)
	{
		debugOut = out;
	}
	
	
	/* get the port that we're supposed to be listening on
	 */
	public int getPort ()
	{
		return thisPort;
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
	 * to halt a running ClientProxy thread
	 */
	public void closeSocket ()
	{
		try {
			// close the open server socket
			server.close();
			// send it a message to make it stop waiting immediately
			// (not really necessary)
			/*Socket s = new Socket("localhost", thisPort);
			OutputStream os = s.getOutputStream();
			os.write((byte)0);
			os.close();
			s.close();*/
		}  catch(Exception e)  { 
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
				conn = DriverManager.getConnection("jdbc:derby:clientdatabase");
				System.out.println("Database exists");
				DatabaseMetaData dbmd = conn.getMetaData();
				String[] names = { "TABLE" };
				ResultSet rs = dbmd.getTables(null, null, null, names);
				boolean tableExists = false;
				while ((rs.next()) && (tableExists == false)) {

					if (rs.getString("TABLE_NAME").toLowerCase()
							.compareTo("clienthashlist") == 0) {
						System.out.println(rs.getString("TABLE_NAME"));
						tableExists = true;
						break;
					}
				}
				rs.close();
				if (!tableExists) {
					System.out.println("Table does not exist");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ClientHashlist (UrlHash VARCHAR(40) NOT NULL PRIMARY KEY, ObjectHash VARCHAR(40) NOT NULL)");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX hashIndex on ClientHashlist(UrlHash ASC)");
				}
			} catch (SQLException e) {
				if (e.getSQLState().equals("XJ004")) { // DB not found
					System.out.println("Database does not exist, creating database and table");
					conn = DriverManager.getConnection("jdbc:derby:clientdatabase;create=true");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ClientHashlist (UrlHash VARCHAR(40) NOT NULL PRIMARY KEY, ObjectHash VARCHAR(40) NOT NULL)");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX hashIndex on ClientHashlist(UrlHash ASC)");
				} else
					e.printStackTrace();
			}
			 
			udpthread = new UDPClientThread(UDPport, fwdUDPport, fwdServer, conn, debugOut, publickey);
			udpthread.start();
			
			
			// create a server socket, and loop forever listening for
			// client connections
			server = new ServerSocket(thisPort);
			System.out.println("Started ClientProxy on port " + thisPort);
			
			
			
			while (true)
			{
				Socket client = server.accept();
				ProxyThread t = new ProxyThread(client, fwdServer, fwdPort,conn, debugOut);
				t.setTimeout(ptTimeout);
				t.start();
			}
		}  catch (Exception e)  {
		   debugOut.println("ClientProxy Thread error: " + e);
		}
		
		closeSocket();
	}
	
}

class UDPClientThread extends Thread
{
	public static final int DEFAULT_UDP_PORT = 8090;
	public static final int DEFAULT_FWD_UDP_PORT = 8091;
	
	private int UDPport = DEFAULT_UDP_PORT;
	private int fwdUdpPort = DEFAULT_FWD_UDP_PORT;
	private String fwdServer = "";
	private Connection database_conn;
	private PrintStream debugOut;
	private DatagramSocket UDPClient = null;
	private PublicKey publickey = null;
	private Timer timer;
	
	public UDPClientThread(int port, int fwdport, String fwd_server, Connection conn, PrintStream debug, PublicKey pbkey){
		UDPport = port;
		database_conn = conn;
		fwdUdpPort = fwdport;
		fwdServer = fwd_server;
		debugOut = debug;
		publickey = pbkey;
		try{
			UDPClient = new DatagramSocket(UDPport);
		}
		catch(SocketException e){
			System.out.println("Unable to create client UDP socket");
			e.printStackTrace(debugOut);
		}
		if(UDPClient!=null){
			timer = new Timer();
		    timer.schedule(new PeriodicTask(this), 0, //initial delay
		        59 * 1000); //subsequent rate
		}
		
	}
	
	public int getUDPPort()
	{
		return UDPport;
	}
	
	public int getFwdUDPPort()
	{
		return fwdUdpPort;
	}
	
	public void informUDPserver()
	{
		debugOut.println("Sending 'Hello' message to VSN Server");
		String msg = "Hello";
		try{
			DatagramPacket firstpacket  = new DatagramPacket(msg.getBytes(), msg.length(), InetAddress.getByName(fwdServer), fwdUdpPort);
			UDPClient.send(firstpacket);
		}
		catch(UnknownHostException e){
			debugOut.println("Unable to create initial packet");
			e.printStackTrace(debugOut);
		}
		catch(IOException e){
			debugOut.println("Unable to send initial packet ");
			e.printStackTrace(debugOut);
		}
	}
	
	public void closeserver ()
	{
		try {
			UDPClient.close();
		}  catch(Exception e)  { 
			e.printStackTrace(debugOut);
		}	
		UDPClient = null;
	}
	
	public void run(){
		byte[] receiveData; 
	    
		while(true){
			receiveData = new byte[1400];
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			try{
				UDPClient.receive(receivePacket);
				
				String IPAddress = receivePacket.getAddress().getHostAddress(); 
				int port = receivePacket.getPort(); 
				debugOut.println("Received a UDP packet from IP:"+IPAddress+" Port:"+port);
				
				if(IPAddress.equalsIgnoreCase(fwdServer) && port == fwdUdpPort){
					String data = new String(receivePacket.getData());
					
					String [] data_array = data.split("\n");
					String signature = data_array[0];
					String signdata = "";
					for(int i=1;i<data_array.length;i++){
						if(data_array[i].length()>0)
							signdata +=data_array[i]+"\n";
					}
					
					
				    try{
				    	Signature signer = Signature.getInstance("SHA1withDSA");
					    signer.initVerify(publickey);
					    signer.update(signdata.getBytes());
					    
					    if(!signer.verify(signature.getBytes())){
					    	System.out.println("Signature did not match - not processing UDP data");
					    	continue;
					    }
				    }catch(Exception e){
				    	System.out.println("NoSuchAlgorithm/invalidkey/Signature Exception");
				    	continue;
				    }
				    
					for(int i=1;i<data_array.length;i++){
						if(data_array[i].length()>0){
							if(data_array[i].indexOf("\t")<0)
								break;
							String urlhash = data_array[i].substring(0, data_array[i].indexOf("\t"));
							String objecthash = data_array[i].substring(data_array[i].indexOf("\t")+1);
							
							urlhash = urlhash.trim();
							objecthash = objecthash.trim();
							byte [] udigest = urlhash.getBytes();
							byte [] obdigest = objecthash.getBytes();
							
							StringBuffer urldigest = new StringBuffer();
							for (int j=0;j<udigest.length;j++) {
								String t = Integer.toHexString(0xFF & udigest[j]);
								t = (t.length()>1)?t:("0"+t);
								urldigest.append(t);
							}
							urlhash = urldigest.toString();
							
							StringBuffer objectdigest = new StringBuffer();
							for (int j=0;j<obdigest.length;j++) {
								String t = Integer.toHexString(0xFF & obdigest[j]);
								t = (t.length()>1)?t:("0"+t);
								objectdigest.append(t);
							}
							objecthash = objectdigest.toString();
							
							try { 		
								Statement stmt = database_conn.createStatement();
								ResultSet rs = stmt.executeQuery("select * from ClientHashlist where UrlHash='"+urlhash+"'");
								if (rs.next()){
									PreparedStatement psUpdate = database_conn.prepareStatement("UPDATE ClientHashlist SET ObjectHash=? WHERE UrlHash=?");
									psUpdate.setString(1, objecthash);
									psUpdate.setString(2, urlhash);
									psUpdate.executeUpdate();
								}
								else{
									PreparedStatement psInsert = database_conn.prepareStatement("insert into ClientHashlist values (?,?)");
									psInsert.setString(1, urlhash);
									psInsert.setString(2, objecthash);
									psInsert.executeUpdate();
								}
								rs.close();
							} catch (SQLException e) {
								debugOut.println("Unable to update client hashlist from the UDP thread");
								e.printStackTrace(debugOut);
							}
						}
					}
					if(data_array.length == 1)
						debugOut.println("Updated client hashlist by adding/updating "+(data_array.length)+" entries");
					else
						debugOut.println("Updated client hashlist by adding/updating "+(data_array.length-1)+" entries");
				}
			}
			catch(IOException e){
				debugOut.println("Unable to receive UDP packet");
				e.printStackTrace(debugOut);
			}
		}
	}
}

class PeriodicTask extends TimerTask{
	
	UDPClientThread udpthread;
	
	public PeriodicTask(UDPClientThread t){
		udpthread = t;
	}
	
	public void run(){
		udpthread.informUDPserver();
	}
}

/* 
 * The ProxyThread will take an HTTP request from the client
 * socket and send it to either the server that the client is
 * trying to contact, or another proxy server
 */
class ProxyThread extends Thread
{
	private Socket pSocket;
	private String fwdServer = "localhost";
	private int fwdPort = 5556;
	private PrintStream debugOut = System.out;
	private Connection conn;
	// the socketTimeout is used to time out the connection to
	// the remote server after a certain period of inactivity;
	// the value is in milliseconds -- use zero if you don't want 
	// a timeout
	public static final int DEFAULT_TIMEOUT = 20 * 1000;
	private int socketTimeout = DEFAULT_TIMEOUT;

	
	public ProxyThread(Socket s)
	{
		pSocket = s;
	}

	public ProxyThread(Socket s, String proxy, int port, Connection con2, PrintStream debug)
	{
		pSocket = s;
		fwdServer = proxy;
		fwdPort = port;
		conn = con2;
		debugOut = debug;
	}
	
	
	public void setTimeout (int timeout)
	{
		// assume that the user will pass the timeout value
		// in seconds (because that's just more intuitive)
		socketTimeout = timeout * 1000;
	}


	public void setDebug (PrintStream out)
	{
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
			int requestLength = 0;
			int responseLength = 0;
			int pos = -1;
			StringBuffer host = new StringBuffer("");
			String hostName = "";
			String url_string = "";
			int hostPort = 80;
			StringBuffer url = new StringBuffer("");
			boolean contacted_origin = false;
			boolean toVSN = false;
			StringBuffer VSNurldigest = new StringBuffer("");
			StringBuffer VSNobjectdigest = new StringBuffer("");
			StringBuffer vsnrequest = new StringBuffer("");
			StringBuffer filetype = new StringBuffer("");
			// get the header info (the web browser won't disconnect after
			// it's sent a request, so make sure the waitForDisconnect
			// parameter is false)
			request = getHTTPData(clientIn, host, url, false);
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
			
			// either forward this request to another proxy server or
			// send it straight to the Host
			try
			{
				if(hostName.indexOf("google.com")>=0){
					if ((fwdServer.length() > 0) && (fwdPort > 0))
					{
						debugOut.println("google.com to VSN:");
						toVSN = true;
						contacted_origin = true;
						server = new Socket(fwdServer, fwdPort);
					}  else  {
						debugOut.println("google.com to origin server:");
						server = new Socket(hostName, hostPort);
						contacted_origin = true;
						toVSN = false;
					}
				}
				else{
					MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
					byte [] udigest = sha1.digest((host.toString()+url.toString()).getBytes());
			    	StringBuffer urldigest = new StringBuffer();
			    	for (int i=0;i<udigest.length;i++) {
			    		urldigest.append(Integer.toHexString(0xFF & udigest[i]));
			    	}
			    	
					try { 		
						Statement stmt2 = conn.createStatement();
						ResultSet rs = stmt2.executeQuery("select * from ClientHashlist where UrlHash='"+urldigest+"'");
						if (rs.next()) {
							server = new Socket(hostName, hostPort);
							contacted_origin = true;
							toVSN = false;
							debugOut.println("to origin server:"+host+url);
						} else {
							if ((fwdServer.length() > 0) && (fwdPort > 0))
							{
								debugOut.println("to VSN:"+host+url);
								toVSN = true;
								server = new Socket(fwdServer, fwdPort);
							}  else  {
								debugOut.println("to origin server (2):"+host+url);
								server = new Socket(hostName, hostPort);
								contacted_origin = true;
								toVSN = false;
							}
						}
						rs.close();

					} catch (SQLException e) {
						e.printStackTrace(debugOut);
					}
				}
			}  catch (Exception e)  {
				// tell the client there was an error
				debugOut.println("Error connecting to the server");
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
			}
			
			if (server != null)
			{
				server.setSoTimeout(socketTimeout);
				BufferedInputStream serverIn = new BufferedInputStream(server.getInputStream());
				BufferedOutputStream serverOut = new BufferedOutputStream(server.getOutputStream());
				try{
				
					//modifying request header to add if VSN server was contacted before and we are sending the request to VSN server
					if(toVSN){
						String temp_request = new String(request);
						vsnrequest.setLength(0);
						while(temp_request.indexOf("\r\n")>=0){
							String templine = temp_request.substring(0,temp_request.indexOf("\r\n"));
							temp_request = temp_request.substring(temp_request.indexOf("\r\n")+2);
							if(templine.length()!=0){
								vsnrequest.append(templine+"\r\n");
							}
							else{
								break;
							}
						}
						vsnrequest.append("VSNContactedOrigin:"+contacted_origin+"\r\n\r\n");
						vsnrequest.append(temp_request);
						serverOut.write(vsnrequest.toString().getBytes(), 0, vsnrequest.length());
					}
					else{
						// send the request to origin server
						//debugOut.println("request sent to origin server:"+host.toString()+url.toString());
						serverOut.write(request, 0, requestLength);
					}
					
					serverOut.flush();
				
					filetype.setLength(0);
					responseLength = streamHTTPData2(serverIn, clientOut,host,url, true,toVSN,VSNurldigest,VSNobjectdigest,filetype);
					
					if(responseLength==-2){
						debugOut.println("Got redirected by VSN server:"+host+url);
						PreparedStatement psInsert = conn.prepareStatement("insert into ClientHashlist values (?,?)");
						psInsert.setString(1, VSNurldigest.toString());
						psInsert.setString(2, VSNobjectdigest.toString());
						psInsert.executeUpdate();
						
						Socket redirect_socket=null;
						try { 		
							redirect_socket = new Socket(hostName, hostPort);
							contacted_origin = true;
							toVSN = false;
						} catch (Exception e) {
							e.printStackTrace(debugOut);
						}
						if(redirect_socket!=null){
							redirect_socket.setSoTimeout(socketTimeout);
							BufferedInputStream redirect_serverIn = new BufferedInputStream(redirect_socket.getInputStream());
							BufferedOutputStream redirect_serverOut = new BufferedOutputStream(redirect_socket.getOutputStream());
							
							redirect_serverOut.write(request, 0, requestLength);
							redirect_serverOut.flush();
							filetype.setLength(0);
							responseLength = streamHTTPData2(redirect_serverIn, clientOut,host,url, true,toVSN,VSNurldigest,VSNobjectdigest, filetype);
							
							redirect_serverIn.close();
							redirect_serverOut.close();
							redirect_socket.close();
						}
					}
					if(responseLength==-3){
						debugOut.println("Hash did not match:"+host+url);
						Socket third_socket = null;
						try { 		
							third_socket = new Socket(fwdServer, fwdPort);
							toVSN = true;
						} catch (Exception e) {
							e.printStackTrace(debugOut);
						}
						if(third_socket!=null){
							third_socket.setSoTimeout(socketTimeout);
							BufferedInputStream third_serverIn = new BufferedInputStream(third_socket.getInputStream());
							BufferedOutputStream third_serverOut = new BufferedOutputStream(third_socket.getOutputStream());
							
							String temp_request = new String(request);
							vsnrequest.setLength(0);
							while(temp_request.indexOf("\r\n")>=0){
								String templine = temp_request.substring(0,temp_request.indexOf("\r\n"));
								temp_request = temp_request.substring(temp_request.indexOf("\r\n")+2);
								if(templine.length()!=0){
									vsnrequest.append(templine+"\r\n");
								}
								else{
									break;
								}
							}
							vsnrequest.append("VSNContactedOrigin:"+contacted_origin+"\r\n\r\n");
							vsnrequest.append(temp_request);
							
							third_serverOut.write(vsnrequest.toString().getBytes(), 0, vsnrequest.length());
							third_serverOut.flush();
							filetype.setLength(0);
							responseLength = streamHTTPData2(third_serverIn, clientOut,host,url, true,toVSN,VSNurldigest,VSNobjectdigest, filetype);
							
							third_serverIn.close();
							third_serverOut.close();
							third_socket.close();
						}
					}
				}
				catch (SocketTimeoutException ste)
				{
					debugOut.println ("Socket timeout occurred - killing connection");
					String errMsg = "HTTP/1.0 504 Gateway Time-out\r\nContent Type: text/plain\r\n" + 
							"<html><body>Error connecting to the server:\n" + ste + "\n</body></html>\r\n";
					clientOut.write(errMsg.getBytes(), 0, errMsg.length());
				}
				//}
				
				serverIn.close();
				serverOut.close();
			}
	
			long endTime = System.currentTimeMillis();
			debugOut.println(endTime+" Request from " + pSocket.getInetAddress().getHostAddress() + 
					" on Port " + pSocket.getLocalPort() + 
					" to host " + hostName + ":" + hostPort + 
					"\n  "+url_string+
					"\n"+"(" + requestLength + " bytes sent, " + 
					responseLength + " bytes returned, " + 
					"ContentType:"+filetype.toString()+", "+
					Long.toString(endTime - startTime) + " ms elapsed)");
			debugOut.flush();
			
			// close all the client streams so we can listen again
			clientOut.close();
			clientIn.close();
			pSocket.close();
		}  catch (Exception e)  {
			debugOut.println("Error in ProxyThread: " + e);
			e.printStackTrace(debugOut);
		}

	}	
	
	private byte[] getHTTPData (InputStream in, StringBuffer host, StringBuffer url, boolean waitForDisconnect)
	{
		// get the HTTP data from an InputStream, and return it as
		// a byte array, and also return the Host entry in the header,
		// if it's specified -- note that we have to use a StringBuffer
		// for the 'host' variable, because a String won't return any
		// information when it's used as a parameter like that
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		streamHTTPData(in, bs, host, url, waitForDisconnect);
		return bs.toByteArray();
	}
	
	private int streamHTTPData(InputStream in, OutputStream out,
			StringBuffer host, StringBuffer url, boolean waitForDisconnect) {
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
				pos = data.indexOf(" ");
				if ((pos >= 0) && (data.indexOf(" ", pos + 1) >= 0)) {						
					pre_url.setLength(0);
					pre_url.append(data.substring(0,pos));
					url.setLength(0);
					url.append(data.substring(pos + 1,data.indexOf(" ", pos + 1)));
					post_url.setLength(0);
					post_url.append(data.substring(data.indexOf(" ", pos + 1)+1));
				}
			}

			// get the rest of the header info
			while ((data = readLine(in)) != null) {
				// the header ends at the first blank line
				if (data.length() == 0)
					break;
				temp_header.append(data + "\r\n");

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
					String errMsg = "Error getting HTTP body: " + e;
						debugOut.println(errMsg);
					// bs.write(errMsg.getBytes(), 0, errMsg.length());
				}
			}
		} catch (Exception e) {
				debugOut.println("Error getting HTTP data: " + e);
		}

		// flush the OutputStream and return
		try {
			out.flush();
		} catch (Exception e) {
		}
		return (header.length() + byteCount);
	}
	
	private int streamHTTPData2 (InputStream in, OutputStream out, 
									StringBuffer host, StringBuffer url, boolean waitForDisconnect,boolean toVSN,
									StringBuffer VSNurldigest, StringBuffer VSNobjectdigest, StringBuffer filetype)
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
				pos = data.indexOf(" ");
				if ((data.toLowerCase().startsWith("http")) && (pos >= 0) && (data.indexOf(" ", pos+1) >= 0))
				{
					String rcString = data.substring(pos+1, data.indexOf(" ", pos+1));
					try
					{
						responseCode = Integer.parseInt(rcString);
					}  catch (Exception e)  {
							debugOut.println("Error parsing response code " + rcString);
					}
				}
				if(data.startsWith("VSNRedirect:")){
					VSNurldigest.setLength(0);
					VSNobjectdigest.setLength(0);
					
					data = readLine(in);
					VSNurldigest.append(data.substring(13));
					data = readLine(in);
					VSNobjectdigest.append(data.substring(16));
					return -2;
				}
				header.append(data + "\r\n");
			}
			
			// get the rest of the header info
			while ((data = readLine(in)) != null)
			{
				// the header ends at the first blank line
				if (data.length() == 0)
					break;
				header.append(data + "\r\n");
				
				// check for the Content-Length header
				pos = data.toLowerCase().indexOf("content-length:");
				if (pos >= 0)
					contentLength = Integer.parseInt(data.substring(pos + 15).trim());
				
				// check for the Content-Type header
				pos = data.toLowerCase().indexOf("content-type:");
				if (pos >= 0)
					contentType = data.substring(pos + 13).trim();
				filetype.setLength(0);
				filetype.append(contentType);
			}
			
			// add a blank line to terminate the header info
			header.append("\r\n");
			
			// if the header indicated that this was not a 200 response,
			// just return what we've got if there is no Content-Length,
			// because we may not be getting anything else
			if ((responseCode != 200) && (contentLength == 0))
			{
				// convert the header to a byte array, and write it to our stream
				out.write(header.toString().getBytes(), 0, header.length());
				out.flush();
				return 0;
			}
            
			
			// get the body, if any; we try to use the Content-Length header to
			// determine how much data we're supposed to be getting, because 
			// sometimes the client/server won't disconnect after sending us
			// information...
			if (contentLength > 0)
				waitForDisconnect = false;
			
			boolean wroteheader = false;
			
			if ((contentLength > 0) || (waitForDisconnect))
			{
				try {
					byte[] buf = new byte[4096];
					int bytesIn = 0;
					while ( ((byteCount < contentLength) || (waitForDisconnect)) 
							&& ((bytesIn = in.read(buf)) >= 0) )
					{
						byteCount += bytesIn;
						if(contentLength > 0){
							if(contentType.toLowerCase().contains("video")){
								if(!wroteheader){
									// convert the header to a byte array, and write it to our stream
									out.write(header.toString().getBytes(), 0, header.length());
									wroteheader = true;
								}
								out.write(buf, 0, bytesIn);
								out.flush();
							}
							else{
								bs.write(buf, 0, bytesIn);
								
								if(toVSN){
									if(!wroteheader){
										// convert the header to a byte array, and write it to our stream
										out.write(header.toString().getBytes(), 0, header.length());
										wroteheader = true;
									}
									out.write(buf, 0, bytesIn);
									out.flush();
								}
								
								if(byteCount>=contentLength){
									debugOut.println("computing and checking response hash");
									byte [] response = bs.toByteArray();
									MessageDigest sha1 = MessageDigest.getInstance("SHA-1");							
									byte [] obdigest = sha1.digest(response);
									sha1.reset();
									
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
									
									try { 		
										Statement stmt2 = conn.createStatement();										
										ResultSet rs = stmt2.executeQuery("select * from ClientHashlist where UrlHash='"+urldigest+"'");
										if (rs.next()) {
											if(toVSN){
												PreparedStatement psUpdate = conn.prepareStatement("UPDATE ClientHashlist SET ObjectHash=? WHERE UrlHash=?");
												psUpdate.setString(1, objectdigest.toString());
												psUpdate.setString(2, urldigest.toString());
												psUpdate.executeUpdate();
											}
											else if(rs.getString(2).equalsIgnoreCase(objectdigest.toString())){
												debugOut.println("Hash match:"+host+url);
												if(!wroteheader){
													// convert the header to a byte array, and write it to our stream
													out.write(header.toString().getBytes(), 0, header.length());
													wroteheader = true;
												}
												out.write(bs.toByteArray(), 0, bs.size());
												out.flush();
											}
											else{
												return -3;
											}
										} else {
											if(toVSN){
												PreparedStatement psInsert = conn
														.prepareStatement("insert into ClientHashlist values (?,?)");

												psInsert.setString(1, urldigest.toString());
												psInsert.setString(2, objectdigest.toString());
												psInsert.executeUpdate();
											}
										}
										rs.close();
									} catch (Exception e) {
										e.printStackTrace(debugOut);
									}			
								}
							}
							
						}
						else{
							if(!wroteheader){
								// convert the header to a byte array, and write it to our stream
								out.write(header.toString().getBytes(), 0, header.length());
								wroteheader = true;
								out.flush();
							}
							
							//bs.write(buf, 0, bytesIn);
							out.write(buf, 0, bytesIn);
							out.flush();
						}
					}
				}  catch (Exception e)  {
					String errMsg = "Error getting HTTP body: " + e;
						debugOut.println(errMsg);
					//bs.write(errMsg.getBytes(), 0, errMsg.length());
				}
			}
		}  catch (Exception e)  {
				debugOut.println("Error getting HTTP data: " + e);
		}
		
		//flush the OutputStream and return
		try  {  out.flush();  }  catch (Exception e)  {}
		return byteCount;
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
				debugOut.println("Error getting header: " + e);
		}
		
		// and return what we have
		return data.toString();
	}
	
}

