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

public class ServerProxy extends Thread
{
	public static final int DEFAULT_PORT = 8081;
	public static final int DEFAULT_UDP_PORT = 8091;
	
	private ServerSocket server = null;
	private int thisPort = DEFAULT_PORT;
	private int ptTimeout = ServerProxyThread.DEFAULT_TIMEOUT/1000;
	private FileOutputStream out;
	private PrintStream debugOut = System.out;
	private String database_driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private Connection conn;
	
	private double alpha = 26363;
	private int max_users = 100;
	private int hashentrysize = 42;
	private double RTTlatency = 50.0/1000;
	private int m_threshold = (int)((hashentrysize*max_users+1.206*(alpha*RTTlatency+hashentrysize))/(1.4004*alpha*RTTlatency+hashentrysize*2.4004));
	
	KeyPair keypair = null;
	
	private int UDPport = DEFAULT_UDP_PORT;
	private UDPServerThread udpthread;
	
	/* here's a main method, in case you want to run this by itself */
	public static void main (String args[])
	{
		int port = 5556;
		int udp_port = 5601;
		double alpha_main =  26363;
		int maxusers_main = 100;
		
		if(args.length >= 1){
			port = Integer.parseInt(args[0]);
		}
		if(args.length >= 2){
			udp_port = Integer.parseInt(args[1]);
		}
		if(args.length >= 3){
			alpha_main = Double.parseDouble(args[2]);
		}
		if(args.length >= 4){
			maxusers_main = Integer.parseInt(args[3]);
		}

		
		// create and start the ServerProxy thread, using a 5 second timeout
		// value to keep the threads from piling up too much
		System.err.println("  **  Starting Server on port " + port + ". Press CTRL-C to end.  **\n");
		ServerProxy sp = new ServerProxy(port,udp_port, 5, alpha_main, maxusers_main);
		try{
			String path = ServerProxy.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			path = URLDecoder.decode(path, "UTF-8");
			System.out.println(path);
			sp.keypair = sp.LoadKeyPair(path,"DSA");
			System.out.println("Got the Key pair");
		}catch(Exception e){
			System.out.println("Unable to read the public and private keys");
			return;
		}
		//sp.setDebug(1, System.out);		
		sp.start();
		
		// run forever; if you were calling this class from another
		// program and you wanted to stop the ServerProxy thread at some
		// point, you could write a loop that waits for a certain
		// condition and then calls ServerProxy.closeSocket() to kill
		// the running ServerProxy thread
		while (true)
		{
			try { Thread.sleep(3000); } catch (Exception e) {}
		}
		
		// if we ever had a condition that stopped the loop above,
		// we'd want to do this to kill the running thread
		//sp.closeSocket();
		//return;
	}
	
	
	/* the proxy server just listens for connections and creates
	 * a new thread for each connection attempt (the ProxyThread
	 * class really does all the work)
	 */

	public ServerProxy (int port, int udp_port)
	{
		thisPort = port;
		UDPport = udp_port;
		try{
			out = new FileOutputStream("serverlog.txt", true);
			debugOut = new PrintStream(out);
		}catch(Exception e){
			debugOut = System.out;
			System.out.println("Could not initialize server log file");
		}
		
		setMThreshold();
	}
	
	public ServerProxy (int port, int udp_port, int timeout, double alphamain, int maxusersmain)
	{
		thisPort = port;
		UDPport = udp_port;
		ptTimeout = timeout;
		alpha = alphamain;
		max_users = maxusersmain;
		try{
			out = new FileOutputStream("serverlog.txt", true);
			debugOut = new PrintStream(out);
		}catch(Exception e){
			debugOut = System.out;
			System.out.println("Could not initialize server log file");
		}
		setMThreshold();
		System.out.println("M_Threshold:"+m_threshold);
	}
	
	public void setMThreshold(){
		m_threshold = (int)((hashentrysize*max_users+1.206*(alpha*RTTlatency+hashentrysize))/(1.4004*alpha*RTTlatency+hashentrysize*2.4004));
	}
	
	public KeyPair LoadKeyPair(String path, String algorithm)
			throws IOException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		// Read Public Key.
		File filePublicKey = new File(path + "/public.key");
		FileInputStream in = new FileInputStream(path + "/public.key");
		byte[] encodedPublicKey = new byte[(int) filePublicKey.length()];
		in.read(encodedPublicKey);
		in.close();
 
		// Read Private Key.
		File filePrivateKey = new File(path + "/private.key");
		in = new FileInputStream(path + "/private.key");
		byte[] encodedPrivateKey = new byte[(int) filePrivateKey.length()];
		in.read(encodedPrivateKey);
		in.close();
 
		// Generate KeyPair.
		KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
		X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(
				encodedPublicKey);
		PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
 
		PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(
				encodedPrivateKey);
		PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
 
		return new KeyPair(publicKey, privateKey);
	}
	
	
	/* allow the user to decide whether or not to send debug
	 * output to the console or some other PrintStream
	 */
	public void setDebug (PrintStream out)
	{
		debugOut = out;
	}
	
	public int getPort ()
	{
		return thisPort;
	}
	
	public int getUDPPort()
	{
		return UDPport;
	}
	
	/* return whether or not the socket is currently open*/
	
	public boolean isRunning ()
	{
		if (server == null)
			return false;
		else
			return true;
	}
	 
	
	/* closeSocket will close the open ServerSocket; use this
	 * to halt a running ServerProxy thread
	 */
	public void closeSocket ()
	{
		try {
			// close the open server socket
			server.close();
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
				e.printStackTrace(debugOut);
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
					String createtable = "CREATE TABLE ServerHashlist (UrlHash VARCHAR(40) NOT NULL PRIMARY KEY, ObjectHash VARCHAR(40) NOT NULL, Occurrence INT DEFAULT 0";
					for(int ct=0;ct<m_threshold;ct++){
						createtable += ", IP"+(ct+1)+" VARCHAR(15)";
					}
					createtable = createtable + ")";
					
					debugOut.println(createtable);
					
					stmt.executeUpdate(createtable);
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX hashIndex on ServerHashlist(UrlHash ASC)");
				}
			} catch (SQLException e) {
				if (e.getSQLState().equals("XJ004")) { // DB not found
					System.out.println("Database does not exist, creating database and table");
					conn = DriverManager.getConnection("jdbc:derby:serverdatabase;create=true");
					Statement stmt = conn.createStatement();
					String createtable = "CREATE TABLE ServerHashlist (UrlHash VARCHAR(40) NOT NULL PRIMARY KEY, ObjectHash VARCHAR(40) NOT NULL, Occurrence INT DEFAULT 0";
					for(int ct=0;ct<m_threshold;ct++){
						createtable += ", IP"+(ct+1)+" VARCHAR(15)";
					}
					createtable = createtable + ")";
					stmt.executeUpdate(createtable);
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
					stmt.executeUpdate("CREATE TABLE ClientDatabase (IP VARCHAR(15) NOT NULL, Port INT DEFAULT "+(DEFAULT_UDP_PORT-1)+", Added TIMESTAMP NOT NULL, PRIMARY KEY (IP))");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX IPIndex on ClientDatabase(IP ASC)");
				}
			} catch (SQLException e) {
				if (e.getSQLState().equals("XJ004")) { // DB not found
					System.out.println("Database does not exist, creating database and table");
					conn = DriverManager.getConnection("jdbc:derby:serverdatabase;create=true");
					Statement stmt = conn.createStatement();
					stmt.executeUpdate("CREATE TABLE ClientDatabase (IP VARCHAR(15) NOT NULL, Port INT DEFAULT "+(DEFAULT_UDP_PORT-1)+", Added TIMESTAMP NOT NULL, PRIMARY KEY (IP))");
					Statement stmt2 = conn.createStatement();
					stmt2.executeUpdate("CREATE INDEX IPIndex on ClientDatabase(IP ASC)");
				} else
					e.printStackTrace();
			}
			// create a server socket, and loop forever listening for
			// client connections
			server = new ServerSocket(thisPort);
			debugOut.println("Started server on port " + thisPort);
			
			try{
				udpthread = new UDPServerThread(UDPport,conn,m_threshold, debugOut, keypair);
				udpthread.start();
			}
			catch(SocketException e){
				System.out.println("Could not create UDP server socket");
			}
			catch(Exception e){
				e.printStackTrace(debugOut);
				return;
			}
			
			System.out.println("Started UDP server on port "+UDPport);
			
			while (true)
			{
				Socket client = server.accept();
				String clientip = client.getInetAddress().getHostAddress();
				
				try { 		
					Statement stmt2 = conn.createStatement();
					ResultSet rs = stmt2.executeQuery("select * from ClientDatabase where IP='"+clientip+"'");
					if (! rs.next()) {
						//debugOut.println("Client entry exists - IP: " + rs.getString(1)+" Port:"+rs.getInt(2));
					//} else {
						PreparedStatement psInsert = conn.prepareStatement("insert into ClientDatabase values (?,?,?)");

						psInsert.setString(1, clientip);
						psInsert.setInt(2, DEFAULT_UDP_PORT-1);
						java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
						psInsert.setTimestamp(3, currentTimestamp);

						psInsert.executeUpdate();
						debugOut.println("Added client database entry with ip:"+clientip+" port:"+(DEFAULT_UDP_PORT-1));
						
						udpthread.sendoldhashes(clientip, DEFAULT_UDP_PORT-1);
					}
					rs.close();
				} catch (Exception e) {
					debugOut.println("clientip="+clientip+"\n");
					e.printStackTrace(debugOut);
				}
				
				ServerProxyThread t = new ServerProxyThread(client, conn, udpthread, m_threshold, debugOut);
				t.setDebug(debugOut);
				t.setTimeout(ptTimeout);
				t.start();
			}
		}  catch (Exception e)  {
				debugOut.println("VSNServer Proxy Thread error: " + e);
		}
		
		closeSocket();
	}
	
}

class UDPServerThread extends Thread
{
	public static final int DEFAULT_UDP_PORT = 8091;
	private int UDPport = DEFAULT_UDP_PORT;
	private DatagramSocket UDPserver = null;
	private int m_threshold = 0;
	private PrintStream debugOut;
	private Connection database_conn;
	KeyPair keypair = null;
	
	public UDPServerThread(int port, Connection conn, int m, PrintStream debug, KeyPair keypair_arg) throws SocketException{
		UDPport = port;
		database_conn = conn;
		m_threshold = m;
		debugOut = debug;
		UDPserver = new DatagramSocket(UDPport);
		KeyPair keypair = keypair_arg;
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
			e.printStackTrace(debugOut);
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
				String IPAddress = receivePacket.getAddress().getHostAddress(); 
				int port = receivePacket.getPort(); 
				debugOut.println("Received UDP 'Hello' request from IP:"+IPAddress+" Port:"+port);
				try { 		
					Statement stmt = database_conn.createStatement();
					ResultSet rs = stmt.executeQuery("select * from ClientDatabase where IP='"+IPAddress+"'");
					if (! rs.next()) {
						PreparedStatement psInsert = database_conn.prepareStatement("insert into ClientDatabase values (?,?,?)");

						psInsert.setString(1, IPAddress);
						psInsert.setInt(2, port);
						java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
						psInsert.setTimestamp(3, currentTimestamp);

						psInsert.executeUpdate();
						debugOut.println("Inserted into ClientDatabase IP:"+IPAddress+" Port:"+port);
						sendoldhashes(IPAddress, port);
					}
					else{
						PreparedStatement psUpdate = database_conn.prepareStatement("UPDATE ClientDatabase SET Port=?, Added=? WHERE IP=?");
						psUpdate.setInt(1,port);
						java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
						psUpdate.setTimestamp(2, currentTimestamp);
						psUpdate.setString(3, IPAddress);
						psUpdate.executeUpdate();
						
						debugOut.println("Updated clientdatabase with IP:"+IPAddress+" Port:"+port);
					}
					rs.close();
					
				} catch (SQLException e) {
					debugOut.println("Unable to add client details for the UDP packet with IP:"+IPAddress+" port:"+port);
					e.printStackTrace(debugOut);
				}
			}
			catch(IOException e){
				debugOut.println("Unable to receive UDP packet");
				e.printStackTrace(debugOut);
			}
		}
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	public void sendoldhashes(String ip, int port){
		try {
			debugOut.println("Sending old hashes to IP:"+ip+" Port:"+port);
			Statement stmt = database_conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from ServerHashlist where Occurrence >="+m_threshold);
			int num = 0;
			int count = 0;
			//java sql timestamp of the format: yyyy-mm-dd hh:mm:ss.fffffffff
			//considering 75 bytes (2 sha1 digests (40 bytes) + timestamp (30 bytes) +3 tabs (3 bytes)+ 1 newline (1 byte) ) per hash entry, and max 1400 MTU for the UDP packet
			//UPDATE Oct 6th 2012: We dont need to send timestamps. Considering 42 bytes (2 sha1 digests (40 bytes))+1 tabs (1 byte)+ 1 newline (1 byte) ) per hash entry, and max 1400 MTU for the UDP packet
			//UPDATE Oct 6th 2012: sending signature at the beginning of the hashes
			int MAX_per_packet = 30; 
			String sendhashdata = "";
			while (rs.next()) {
				String one = new String(hexStringToByteArray(rs.getString(1)));
				String two = new String(hexStringToByteArray(rs.getString(2)));
				sendhashdata += one+"\t"+two+"\n";
				num++;
				count++;
				if(num==MAX_per_packet){
					byte [] sendData = sendhashdata.getBytes();
					Signature signer = Signature.getInstance("SHA1withDSA");
				    signer.initSign(keypair.getPrivate());
				    signer.update(sendData);
				    byte [] signature = signer.sign();
				    
				    StringBuffer sigdigest = new StringBuffer();
					for (int i=0;i<signature.length;i++) {
						String t = Integer.toHexString(0xFF & signature[i]);
						t = (t.length()>1)?t:("0"+t);
						sigdigest.append(t);
					}
					
					sendhashdata = sigdigest+"\n"+ sendhashdata;
					sendData = sendhashdata.getBytes();
					DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,InetAddress.getByName(ip),port);
					UDPserver.send(sendPacket);
					sendhashdata = "";
					num = 0;
					//debugOut.println("UDP: Sent 18 hash entries to IP:"+ip+" port:"+port);
				}
			}
			if(num>0){
				byte [] sendData = sendhashdata.getBytes();
				Signature signer = Signature.getInstance("SHA1withDSA");
			    signer.initSign(keypair.getPrivate());
			    signer.update(sendData);
			    byte [] signature = signer.sign();
			    
			    StringBuffer sigdigest = new StringBuffer();
				for (int i=0;i<signature.length;i++) {
					String t = Integer.toHexString(0xFF & signature[i]);
					t = (t.length()>1)?t:("0"+t);
					sigdigest.append(t);
				}
				
				sendhashdata = sigdigest+"\n"+ sendhashdata;
				sendData = sendhashdata.getBytes();
				DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,InetAddress.getByName(ip),port);
				UDPserver.send(sendPacket);
				sendhashdata = "";
				num = 0;
			}
			debugOut.println("UDP: Sent "+count+" hash entries to IP:"+ip+" port:"+port);
			rs.close();
		} catch (Exception e) {
			debugOut.println("Exception in sending old hashes to IP:"+ip);
			e.printStackTrace(debugOut);
		}
	}
	
	public void sendhashtoall(String newhash, boolean exclude){
		debugOut.println("Added the entry "+newhash+" to push list and sending it to all users");
		String clientip;
		int port;
		String urlhash = newhash.substring(0,newhash.indexOf("\t"));
		String objhash = newhash.substring(newhash.indexOf("\t")+1);
		String [] ip = new String [m_threshold];
		try {
			if(!exclude){
				Statement stmt1 = database_conn.createStatement();
				ResultSet rs1 = stmt1.executeQuery("select * from ServerHashlist where UrlHash='"+urlhash+"'");
				if(rs1.next()){
					for(int i=0;i<m_threshold;i++){
						ip[i] = rs1.getString(4+i);
					}
				}
			}
			
			String one = new String(hexStringToByteArray(urlhash));
			String two = new String(hexStringToByteArray(objhash));
			String sendhashdata = one+"\t"+two;
			byte [] sendData = sendhashdata.getBytes();
			Signature signer;
			byte [] signature = null;
			try{
				signer = Signature.getInstance("SHA1withDSA");
				signer.initSign(keypair.getPrivate());
				signer.update(sendData);
				signature = signer.sign();
			}catch(Exception e){System.out.println("NoSuchAlgorithm/invalidkey/Signature Exception");}
			
		    
		    StringBuffer sigdigest = new StringBuffer();
			for (int i=0;i<signature.length;i++) {
				String t = Integer.toHexString(0xFF & signature[i]);
				t = (t.length()>1)?t:("0"+t);
				sigdigest.append(t);
			}
			
			sendhashdata = sigdigest+"\n"+ one+"\t"+two;
			sendData = sendhashdata.getBytes();
			
			Statement stmt2 = database_conn.createStatement();
			ResultSet rs = stmt2.executeQuery("select * from ClientDatabase");
			while(rs.next()) {
				clientip = rs.getString(1);
				port = rs.getInt(2);
				
				if(!Arrays.asList(ip).contains(clientip)){					
					try{
						DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,InetAddress.getByName(clientip),port);
						UDPserver.send(sendPacket);
					}
					catch(Exception e){
						debugOut.println("Something wrong in pushing new hash entry to a user");
						e.printStackTrace(debugOut);
					}
				}
			}
			rs.close();
		} catch (SQLException e) {
			debugOut.println("Unable to read data from clientdatabase while sending new hash entry to all users");
			e.printStackTrace(debugOut);
		}
	}
}

class ServerProxyThread extends Thread
{
	private Socket clientSocket;
	private PrintStream debugOut = System.out;
	private Connection conn;
	private UDPServerThread udpthread;
	private int m_threshold = 0;
	
	// the socketTimeout is used to time out the connection to
	// the remote server after a certain period of inactivity;
	// the value is in milliseconds -- use zero if you don't want 
	// a timeout
	public static final int DEFAULT_TIMEOUT = 5 * 1000;
	private int socketTimeout = DEFAULT_TIMEOUT;

	public ServerProxyThread(Socket s, Connection con2, UDPServerThread udp, int m, PrintStream debug)
	{
		clientSocket = s;
		conn = con2;
		udpthread = udp;
		m_threshold = m;
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
			BufferedInputStream clientIn = new BufferedInputStream(clientSocket.getInputStream());
			BufferedOutputStream clientOut = new BufferedOutputStream(clientSocket.getOutputStream());
			
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
			StringBuffer contactedorigin = new StringBuffer("");
			StringBuffer filetype = new StringBuffer("");
			
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
			//debugOut.println("gotrequest url:"+url+"\nhost:"+host+"\nhostport:"+hostPort);			
	    	
			// either forward this request to another proxy server or
			// send it straight to the Host
			
			if(contactedorigin.toString().equalsIgnoreCase("false")){
				MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
				byte [] udigest = sha1.digest((host.toString()+url.toString()).getBytes());

				StringBuffer urldigest = new StringBuffer();
				for (int i=0;i<udigest.length;i++) {
					String t = Integer.toHexString(0xFF & udigest[i]);
					t = (t.length()>1)?t:("0"+t);
					urldigest.append(t);
				}

				//debugOut.println("computed urlhash:"+urldigest.toString());
				try { 		
					Statement stmt2 = conn.createStatement();
					ResultSet rs = stmt2.executeQuery("select * from ServerHashlist where UrlHash='"+urldigest+"'");
					int num = 0;
					if (rs.next()) {
						debugOut.println("ENTRY EXISTS -  UrlHash: " + rs.getString(1)
								+ " ObjectHash:" + rs.getString(2) + " Count:"+rs.getInt(3));
						int m = rs.getInt(3);
						if(m<m_threshold){							
							String [] ip = new String[m_threshold];
							for(int ip_c = 0;ip_c<m_threshold;ip_c++){
								ip[ip_c] = rs.getString(4+ip_c);
							}
							
							String clientip = clientSocket.getInetAddress().getHostAddress();
							if(!Arrays.asList(ip).contains(clientip)){
								Statement stmt3 = conn.createStatement();
								int updatecount = stmt3.executeUpdate("UPDATE ServerHashlist SET Occurrence="+(m+1)+", IP"+(m+1)+"='"+clientip+"' WHERE UrlHash='"+urldigest+"'");
								if(m+1==m_threshold){
									String objectdigest = rs.getString(2);
									udpthread.sendhashtoall(urldigest+"\t"+objectdigest, false);
								}
							}
						}
						debugOut.println("redirecting user to origin");
						String redirectmsg ="VSNRedirect:true\r\nVSNurldigest:"+rs.getString(1)+"\r\nVSNobjectdigest:"+rs.getString(2)+"\r\ncontent-length:0\r\n\r\n";
						clientOut.write(redirectmsg.getBytes());
						clientOut.flush();

						clientOut.close();
						clientIn.close();
						clientSocket.close();
						return;
					}
					rs.close();
				} catch (Exception e) {
					e.printStackTrace(debugOut);
				}
			}

			try
			{
				server = new Socket(hostName, hostPort);
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
					String clientip = clientSocket.getInetAddress().getHostAddress();
					filetype.setLength(0);
					responseLength = streamHTTPData2(serverIn, clientOut,host,url, true,clientip, filetype);
				}
				catch (SocketTimeoutException ste)
				{
					debugOut.println ("Socket timeout occurred - killing connection");
					String errMsg_body = "<html><head><title>504 Gateway Time-out</title></head>"+
					        "<body><h1>VSN ERROR MESSAGE: Gateway Time-out</h1>"+
					        "<p>Connecting to the server("+hostName+") timedout as the server was not responsive. Please try again later.</p>"+
					        "</body></html>\r\n";
					String errMsg_header = "HTTP/1.0 504 Gateway Time-out\r\n"+
			                "Content-Type: text/html\r\n"+
							"Content-Length: "+errMsg_body.length()+"\r\n"+
					        "Connection: close\r\n\r\n";
					String errMsg = errMsg_header+errMsg_body;
					clientOut.write(errMsg.getBytes(), 0, errMsg.length());
				}
				
				serverIn.close();
				serverOut.close();
			}
			
			
			// if the user wants debug info, send them debug info; however,
			// keep in mind that because we're using threads, the output won't
			// necessarily be synchronous

			long endTime = System.currentTimeMillis();
			debugOut.println(endTime+" Request from " + clientSocket.getInetAddress().getHostAddress() + 
					" on Port " + clientSocket.getLocalPort() + 
					" to host " + hostName + ":" + hostPort + 
					"\n  "+url_string+
					"\n"+"(" + requestLength + " bytes sent, " + 
					responseLength + " bytes returned, " + 
					"ContentType:"+filetype+", "+
					Long.toString(endTime - startTime) + " ms elapsed)");
			debugOut.flush();
			
			
			// close all the client streams so we can listen again
			clientOut.close();
			clientIn.close();
			clientSocket.close();
		}  catch (Exception e)  {
				debugOut.println("Error in ServerProxyThread: " + e);
				e.printStackTrace(debugOut);
			
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
					e.printStackTrace(debugOut);
					debugOut.println(errMsg);
					// bs.write(errMsg.getBytes(), 0, errMsg.length());
				}
			}
		} catch (Exception e) {
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
									StringBuffer host, StringBuffer url, boolean waitForDisconnect,String clientip, StringBuffer filetype)
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
			//reading blank lines before the header, as some website like studentuniverse.com were appending blank lines before 302 response header
			data = readLine(in);
			while(data!=null && data.length()==0){
				data = readLine(in);
			}
			
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
							debugOut.println("Error parsing response code " + rcString);
					}
				}
			}
			else{
				return 0;
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
				filetype.append(contentType);
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
				//debugOut.println("Trying to get content body");
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
							    		String t = Integer.toHexString(0xFF & obdigest[i]);
							    		t = (t.length()>1)?t:("0"+t);
							    		objectdigest.append(t);
							    	}
							    	
							    	StringBuffer urldigest = new StringBuffer();
							    	for (int i=0;i<udigest.length;i++) {
							    		String t = Integer.toHexString(0xFF & udigest[i]);
							    		t = (t.length()>1)?t:("0"+t);
							    		urldigest.append(t);
							    	}
									
									try { 		
										Statement stmt2 = conn.createStatement();
										ResultSet rs = stmt2.executeQuery("select * from ServerHashlist where UrlHash='"+urldigest+"'");
										if (rs.next()) {
											//debugOut.println("ENTRY EXISTS -  UrlHash: " + rs.getString(1)
											//		+ " ObjectHash:" + rs.getString(2) + " Count:"+rs.getInt(3));
											String old_objecthash = rs.getString(2);
											int m = rs.getInt(3);
											if(m<m_threshold){												
												String [] ip = new String[m_threshold];
												for(int ip_c = 0;ip_c<m_threshold;ip_c++){
													ip[ip_c] = rs.getString(4+ip_c);
												}
												
												if(!Arrays.asList(ip).contains(clientip)){
													PreparedStatement psUpdate = conn.prepareStatement("UPDATE ServerHashlist SET ObjectHash=?, Occurrence=?, IP"+(m+1)+"=? WHERE UrlHash=?");
													psUpdate.setString(1, objectdigest.toString());
													psUpdate.setInt(2, m+1);
													psUpdate.setString(3, clientip);
													psUpdate.setString(4, urldigest.toString());
													psUpdate.executeUpdate();
													
													if(m+1==m_threshold){
														if(!old_objecthash.equalsIgnoreCase(objectdigest.toString())){
															udpthread.sendhashtoall(urldigest.toString()+"\t"+objectdigest.toString(), true);
														}
														else{
															udpthread.sendhashtoall(urldigest.toString()+"\t"+objectdigest.toString(), false);
														}
													}
												}
											}
											else{
												PreparedStatement psUpdate = conn.prepareStatement("UPDATE ServerHashlist SET ObjectHash=? WHERE UrlHash=?");
												psUpdate.setString(1, objectdigest.toString());
												psUpdate.setString(2, urldigest.toString());
												psUpdate.executeUpdate();
												
												if(!old_objecthash.equalsIgnoreCase(objectdigest.toString())){
													udpthread.sendhashtoall(urldigest.toString()+"\t"+objectdigest.toString(), true);
												}
											}
										} else {
											String insertstatement = "insert into ServerHashlist values (?,?,?";
											for(int psi=0;psi<m_threshold;psi++){
                                            	insertstatement +=",?";
                                            }
											insertstatement +=")";
											PreparedStatement psInsert = conn.prepareStatement(insertstatement);
											
											psInsert.setString(1, urldigest.toString());
											psInsert.setString(2, objectdigest.toString());
                                            psInsert.setInt(3, 1);
                                            psInsert.setString(4, clientip);
                                            for(int psi=1;psi<m_threshold;psi++){
                                            	psInsert.setString(4+psi, "");
                                            }
											psInsert.executeUpdate();
											if(m_threshold==1)
												udpthread.sendhashtoall(urldigest.toString()+"\t"+objectdigest.toString(), false);
										}
										rs.close();
										
									} catch (Exception e) {
										e.printStackTrace(debugOut);
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
					e.printStackTrace(debugOut);
					debugOut.println(errMsg);
					//bs.write(errMsg.getBytes(), 0, errMsg.length());
				}
			}
		}  catch (Exception e)  {
			debugOut.println("Streamhttp2(b) Error getting HTTP data: " + e);
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

