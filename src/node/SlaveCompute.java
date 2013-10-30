package node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import mapreduce.MapperPerform;
import mapreduce.ReducerPerform;
import socket.MapperAckMsg;
import socket.Message;
import socket.ReducerAckMsg;
import util.Constants;
import dfs.FileTransmitServer;

/**
 * The computation routine of the slave
 */
public class SlaveCompute extends Thread {

	private Socket sockToMaster;
	// failed reducer task, wait for new reducer to send files
	// key: socketAddress, value: splitName
	public static HashMap<SocketAddress, ArrayList<String>> failedCache = new HashMap<SocketAddress, ArrayList<String>>();

	// constructor
	public SlaveCompute(Socket sockToMaster) {
		this.sockToMaster = sockToMaster;
	}

	public void run() {
		while (true) {
			Message msgIn = null;
			try {
				msgIn = Message.receive(sockToMaster, null, -1);

				switch (msgIn.getType()) {
				case FILE_SPLIT_REQ:
					// get the file split name, download the split and send ack
					// back
					getFileSplit((String) msgIn.getContent());

					new Message(Message.MSG_TYPE.FILE_SPLIT_ACK, null).send(
							sockToMaster, null, -1);
					break;
				case KEEP_ALIVE:
					// Keep alive poll, send the message back
					msgIn.send(sockToMaster, null, -1);
					break;
				case MAPPER_REQ:
					// Create a thread to perform mapper task
					new MapperPerform((MapperAckMsg) msgIn.getContent())
							.start();
					break;
				case REDUCER_REQ:
					// Create a thread to perform reducer task
					new ReducerPerform((ReducerAckMsg) msgIn.getContent())
							.start();
					break;
				case NODE_FAIL_ACK:
					// node repaired
					recover(msgIn.getContent());
					break;
				case NOTIFY_PORT:
					new SlaveListen((Integer)msgIn.getContent()).start();
					break;
				default:
					break;
				}

			} catch (Exception e) {
				System.out.println("Connection to master broke");
				System.exit(0);
			}
			System.out.println(msgIn.getType() + " :handle success");
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

	/**
	 * fail node recover, check failedCache, send file to new reducer and update
	 * failedCache (failure may happen again). Also, when update cache, need to
	 * go through the failedCache to see if no split is here now. if no, send
	 * MAPPER_COMLETE to info master
	 * 
	 * @param content
	 */
	private void recover(Object content) {
		// TODO Auto-generated method stub
		content = content;
	}
}
