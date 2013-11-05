package node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.KeyStore.Entry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Job;

import socket.ChangeReduceMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import dfs.FileSplit;
import dfs.FileTransmitServer;

/**
 * The master node, contains scheduler
 * 
 * @author zhuolinl dil1
 * 
 */
public class MasterMain {
	// pool to record the slave socket
	public static ConcurrentHashMap<SocketAddress, SlaveInfo> slavePool = new ConcurrentHashMap<SocketAddress, SlaveInfo>();
	public static int curPort;
	public static void main(String[] args) {
		// fill up the constants
		
		try {
			new Constants(args[0]);
			curPort = Constants.startPort;
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

		// start scheduler
		Scheduler scheduler = new Scheduler();
		scheduler.start();

		// keep alive process start
		new MasterKeepAlive().start();

		// start main routine
		executing(scheduler);

		server.stop(0);
		System.exit(0);
	}

	@SuppressWarnings("resource")
	private static void executing(Scheduler scheduler) {
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(Constants.MainRoutingPort);
		} catch (IOException e) {
			System.out.println("Master Server Socket cannot be openned");
			System.exit(0);
		}

		// int i = 0;
		// manage join
		while (true) {
			Socket sock = null;
			try {
				sock = serverSock.accept();
				System.out.println("sock RemoteAddr" + sock.getRemoteSocketAddress());
				sock.setSoTimeout(Constants.RegularTimout);
				curPort ++;
				System.out.println("curPort is" + curPort);
				if(curPort > Constants.endPort) {
					System.out.println("Port pool used up.");
					System.exit(0);
				}
				new Message(MSG_TYPE.NOTIFY_PORT, curPort).send(sock, null, -1);
				
				if (Message.receive(sock, null, -1).getType() == MSG_TYPE.NOTIFY_PORT) {
					slavePool.put(sock.getRemoteSocketAddress(),
						new SlaveInfo(sock, MasterMain.curPort));
				} else {
					throw new Exception();
				}

				// i++;
				// if (i == 2)
				// break;
			} catch (Exception e) {
				e.printStackTrace();
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
	 * everyone the replacer and the original one in the content of NODE_FAIL_ACK message
	 * 
	 * @param content
	 */
	public static void handleLeave(ArrayList<SocketAddress> removeList) {
		// TODO reschedule its works
		ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>(
                MasterMain.slavePool.values());
		
		for(SocketAddress sockAddr : removeList) {
			//move its reduce jobs to other hosts
			for(Integer reduceTask :slavePool.get(sockAddr).getReducerTasks()) {
				Collections.sort(slaveList, new SlaveInfo.ReducerPrio());
		        int i;
				for (i = 0; i < slaveList.size(); i++) {
		        	if(slaveList.get(i).getReducerTasks().size() < Constants.IdealReducerNum) {
		        		Scheduler.inviteReducer(slaveList.get(i).getSocket(),
			                    Constants.IdealMapperNum, new Job(reduceTask), i + 1);	
		        		for(SlaveInfo info : slaveList) {
		        			Socket sock = info.getSocket();
		        			ChangeReduceMsg msg = new ChangeReduceMsg(sockAddr, slaveList.get(i).getSocketAddr());
		        			try {
								new Message(MSG_TYPE.CHANGE_REDUCELIST, msg).send(sock, null, -1);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
		        		}
		        		break;
		        	}		            
		        }
		        if(i == slaveList.size()) {
		        	System.out.println("No more reducer available.");
		        }
			}
			//move its map jobs to other hosts
			for(String fileSplit : slavePool.get(sockAddr).getMapperTasks()) {
				boolean flag = false;
				for(java.util.Map.Entry<SocketAddress, ArrayList<String>> entry : FileSplit.splitLayout.entrySet()) {
					if(entry.getValue().contains(fileSplit)) {
						Scheduler.inviteMapper(entry.getKey(), new Job(Scheduler.MapperJob.get(fileSplit).getMapperClass()),
								Scheduler.MapperJob.get(fileSplit).getReudcerList(), fileSplit);	
						flag = true;
						break;
					}
				}
				if(!flag) {
					// TODO: what if no host has this piece of file(no mapper candidate)
				}
			}
		}
		// delete the slave from pool
		for (SocketAddress add : removeList) {
			MasterMain.slavePool.remove(add);
		}
	}
}
