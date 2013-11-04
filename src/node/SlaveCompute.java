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
import socket.ChangeReduceMsg;
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
	public static ArrayList<Thread> mapperThreadList = new ArrayList<Thread>();
	public static HashMap<Integer, Thread> waitingThreadMap = new HashMap<Integer, Thread>();
	public static HashMap<Integer, Integer> fileLeftMap = new HashMap<Integer, Integer>();
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
					Thread newMapper = new MapperPerform((MapperAckMsg) msgIn.getContent());
					this.mapperThreadList.add(newMapper);
							newMapper.start();
					break;
				case REDUCER_REQ:
					// Create a thread to perform reducer task
					ReducerAckMsg msgContent = (ReducerAckMsg) msgIn.getContent();
					int jobID = msgContent.getJobID();
					Thread newReducer = new ReducerPerform(msgContent);
					waitingThreadMap.put(jobID, newReducer);
					fileLeftMap.put(jobID, msgContent.getfileNames().size());
					break;
				case NODE_FAIL_ACK:
					// node repaired
					recover(msgIn.getContent());
					break;
				case NOTIFY_PORT:
					new SlaveListen((Integer)msgIn.getContent()).start();
					break;
				case CHANGE_REDUCELIST:
					new SlaveChangeReduce((ChangeReduceMsg)msgIn.getContent()).start();
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
