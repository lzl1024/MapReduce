package node;

import java.net.Socket;

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
					mapperTask((MapperAckMsg)msgIn.getContent());
					break;
				case REDUCER_REQ:
					reducerTask((ReducerAckMsg)msgIn.getContent());
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
	 * Handler reducer task
	 * @param content
	 */
	private void reducerTask(ReducerAckMsg content) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Handler mapper task
	 * @param content
	 */
	private void mapperTask(MapperAckMsg content) {
		// TODO Auto-generated method stub
		
	}
}
