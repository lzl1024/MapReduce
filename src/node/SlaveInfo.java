package node;

import java.net.Socket;
import java.net.SocketAddress;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;

import socket.Message;
import socket.Message.MSG_TYPE;

public class SlaveInfo {

	private Socket socket;
	// the layout of mapper tasks, value is split name
	private CopyOnWriteArrayList<String> mapperTasks;
	// the layout of reducer tasks, value is jobID
	private CopyOnWriteArrayList<Integer> reducerTasks;
	// the private port for this slave
	private int port;
	// the private socketAddress for communication between slaves
	private SocketAddress sockAddr;

	public SlaveInfo() {
		mapperTasks = new CopyOnWriteArrayList<String>();
		reducerTasks = new CopyOnWriteArrayList<Integer>();
	}

	public SlaveInfo(Socket sock) {
		mapperTasks = new CopyOnWriteArrayList<String>();
		reducerTasks = new CopyOnWriteArrayList<Integer>();
		socket = sock;
	}

	public SlaveInfo(Socket sock, int port) {
		mapperTasks = new CopyOnWriteArrayList<String>();
		reducerTasks = new CopyOnWriteArrayList<Integer>();
		socket = sock;
		this.port = port;
		Socket tmpSock;
		System.out.println("socketAddr : " + socket.getInetAddress() + "port :"
				+ port);
		try {
			tmpSock = new Socket(socket.getInetAddress(), port);
			new Message(MSG_TYPE.NOTIFY_PORT, null).send(tmpSock, null, -1);
			this.sockAddr = tmpSock.getRemoteSocketAddress();
			tmpSock.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Slave listen port unavailable");
			e.printStackTrace();
		}

	}

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public int getPort() {
		return this.port;
	}

	public void setPort(int newPort) {
		this.port = newPort;
	}

	public SocketAddress getSocketAddr() {
		return this.sockAddr;
	}

	public void setSocketAddr(SocketAddress newSockAddr) {
		this.sockAddr = newSockAddr;
	}

	public CopyOnWriteArrayList<String> getMapperTasks() {
		return mapperTasks;
	}

	public void setMapperTasks(CopyOnWriteArrayList<String> mapperTasks) {
		this.mapperTasks = mapperTasks;
	}

	public CopyOnWriteArrayList<Integer> getReducerTasks() {
		return reducerTasks;
	}

	public void setReducerTasks(CopyOnWriteArrayList<Integer> reducerTasks) {
		this.reducerTasks = reducerTasks;
	}

	/**
	 * 
	 * two comparator to sort according to reducerTask and mapperTask
	 * 
	 */
	public static class ReducerPrio implements Comparator<SlaveInfo> {

		@Override
		public int compare(SlaveInfo info1, SlaveInfo info2) {
			if (info1.reducerTasks.size() < info2.reducerTasks.size()) {
				return 1;
			} else if (info1.reducerTasks.size() > info2.reducerTasks.size()) {
				return -1;
			}
			return 0;
		}

	}

	public static class MapperPrio implements Comparator<SlaveInfo> {

		@Override
		public int compare(SlaveInfo info1, SlaveInfo info2) {
			if (info1.mapperTasks.size() < info2.mapperTasks.size()) {
				return 1;
			} else if (info1.mapperTasks.size() > info2.mapperTasks.size()) {
				return -1;
			}
			return 0;
		}

	}

	public String toString() {
		return this.mapperTasks.toString() + " \n"
				+ this.reducerTasks.toString() + " \n" + this.sockAddr + " "
				+ this.port;
	}
}
