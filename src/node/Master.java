package node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import util.Constants;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import dfs.FileTransmitServer;

/**
 * The master node, contains scheduler
 * 
 * @author zhuolinl dil1
 * 
 */
public class Master {
	// pool to record the slave socket
	public static ConcurrentHashMap<SocketAddress, Socket> slavePool 
	= new ConcurrentHashMap<SocketAddress, Socket>();

	public static void main(String[] args) {
		// fill up the constants
		try {
			new Constants(args[0]);
		} catch (IOException e) {
			System.out.println("configure file missing!");
			System.exit(1);
		}

		// open file transmit server
		InetSocketAddress addr = new InetSocketAddress(
				Constants.FiledispatchPort);
		HttpServer server = null;
		try {
			server = HttpServer.create(addr, 0);
			final HttpHandler handler = new FileTransmitServer();
			server.createContext("/", handler);
			server.start();
		} catch (IOException e) {
			System.out.println("Create Server Failed");
		}

		// start main routine
		executing();

		server.stop(0);
		System.exit(0);
	}

	private static void executing() {
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(Constants.MainRoutingPort);
		} catch (IOException e) {
			System.out.println("Server Socket cannot be openned");
			System.exit(0);
		}

		int i = 0;
		// manage join, leave and fail
		while (true) {
			Socket sock = null;
			try {
				sock = serverSock.accept();
				slavePool.put(sock.getRemoteSocketAddress(), sock);
				i++;
				if (i == 2)
					break;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(0);
			}
		}

//		// split file, for test
//		ArrayList<Socket> freeMappers = new ArrayList<Socket>();
//		for (Socket sock : slavePool.values()) {
//			freeMappers.add(sock);
//		}
//		try {
//			FileSplit.fileDispatch(freeMappers, Constants.FS_LOCATION
//					+ "story1.txt", 2, 2);
//			System.out.println(FileSplit.splitLayout);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		while(true){}
	}
}
