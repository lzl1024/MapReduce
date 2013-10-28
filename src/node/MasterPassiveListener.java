package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import mapreduce.Job;
import socket.Message;
import util.Constants;

/**
 * Master listen active message from slave
 */
public class MasterPassiveListener extends Thread {
	@SuppressWarnings({ "resource", "unchecked" })
	public void run() {
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(Constants.SlaveActivePort);
		} catch (IOException e) {
			System.out.println("Master Listener Socket cannot be openned");
			System.exit(0);
		}

		while (true) {
			try {
				Socket sock = serverSock.accept();
				sock.setSoTimeout(Constants.RegularTimout);

				Message msg = Message.receive(sock, null, -1);
				switch (msg.getType()) {
				// listen if one slave quit, send back quit message, handle it
				// and remove it from slavePool
				case SLAVE_QUIT:
					MasterMain.slavePool.remove(sock.getRemoteSocketAddress());
					ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
					tmp.add(sock.getRemoteSocketAddress());
					MasterMain.handleLeave(tmp);
					msg.send(sock, null, -1);
					break;
				// mapper will find its reducer fail, master handles the fail
				// send back fail_ack
				case NODE_FAIL:
					tmp = (ArrayList<SocketAddress>) msg.getContent();
					for (SocketAddress add : tmp) {
						MasterMain.slavePool.remove(add);
					}
					MasterMain.handleLeave((ArrayList<SocketAddress>) msg
							.getContent());
					new Message(Message.MSG_TYPE.NODE_FAIL_ACK, null).send(
							sock, null, -1);
					break;
				// new Job comes
				case NEW_JOB:
					try {
						handleJob((Job) msg.getContent());
						new Message(Message.MSG_TYPE.WORK_COMPELETE, null).send(
								sock, null, -1);
					} catch (Exception e) {
						e.printStackTrace();
						new Message(Message.MSG_TYPE.WORK_FAIL, null).send(
								sock, null, -1);
					}
					break;
				default:
					throw new IOException();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Handle a mapreduce Job include schedule the mapper and
	 * reducer, execute mapreduce facility
	 * @param content
	 */
	private void handleJob(Job content) throws Exception{
		// TODO Auto-generated method stub

	}

}
