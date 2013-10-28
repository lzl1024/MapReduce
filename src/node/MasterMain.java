package node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
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
public class MasterMain {
	// pool to record the slave socket
	public static ConcurrentHashMap<SocketAddress, Socket> slavePool = new ConcurrentHashMap<SocketAddress, Socket>();

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

		// start active listener
		new MasterPassiveListener().start();

		// keep alive
		new KeepAliveProcess().start();

		// start main routine
		executing();

		server.stop(0);
		System.exit(0);
	}

	@SuppressWarnings("resource")
	private static void executing() {
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(Constants.MainRoutingPort);
		} catch (IOException e) {
			System.out.println("Master Server Socket cannot be openned");
			System.exit(0);
		}

		// int i = 0;
		// manage join, leave and fail
		while (true) {
			Socket sock = null;
			try {
				sock = serverSock.accept();
				sock.setSoTimeout(Constants.RegularTimout);
				slavePool.put(sock.getRemoteSocketAddress(), sock);
				// i++;
				// if (i == 2)
				// break;
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}

		// // split file, for test
		// ArrayList<Socket> freeMappers = new ArrayList<Socket>();
		// for (Socket sock : slavePool.values()) {
		// freeMappers.add(sock);
		// }
		// try {
		// FileSplit.fileDispatch(freeMappers, Constants.FS_LOCATION
		// + "story1.txt", 2, 2);
		// System.out.println(FileSplit.splitLayout);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// while(true){}
	}

	/**
	 * Handle Failure or Quit situation 1. For workers mapper work: each work
	 * select other proper node to do first check node with the file split. Then
	 * change 2. For workers reduce work: find another node, and announce
	 * everyone the replacer in the content of NODE_FAIL message
	 * 
	 * @param content
	 */
	public static void handleLeave(ArrayList<SocketAddress> content) {
		// TODO Auto-generated method stub

	}
}
