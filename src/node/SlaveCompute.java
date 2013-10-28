package node;

import java.net.Socket;
import java.net.SocketTimeoutException;

import dfs.FileTransmitServer;

import socket.Message;
import util.Constants;

/**
 * The computation routine of the slave
 */
public class SlaveCompute extends Thread {

	private Socket sockToMaster;

	// constructor
	public SlaveCompute(Socket sockToMaster) {
		this.sockToMaster = sockToMaster;
	}

	public void run() {
		while (true) {
			Message msgIn = null;
			try {
				msgIn = Message.receive(sockToMaster, null, -1);
			} catch (SocketTimeoutException e) {
				System.out.println("receive timeout");
				System.exit(0);
			}
			Message msgOut = null;

			switch (msgIn.getType()) {
			case FILE_SPLIT_REQ:
				getFileSplit((String) msgIn.getContent());
				msgOut = new Message(Message.MSG_TYPE.FILE_SPLIT_ACK, null);
				break;
			default:
				break;
			}

			msgOut.send(sockToMaster, null, -1);
			
			System.out.println("send back success");
		}
	}

	/**
	 * get File split from master file system
	 * 
	 * @param splitName
	 */
	private void getFileSplit(String splitName) {
		String masterUrl = Constants.HTTP_PREFIX + Constants.MasterIp + ":"
				+ Constants.FiledispatchPort + "/" + splitName;
		FileTransmitServer.httpDownload(masterUrl, splitName);

	}
}
