package node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

public class SlaveListen extends Thread {
	private ServerSocket ListenSocket = null;
	
	public SlaveListen(int port) {
		try {
			this.ListenSocket = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void run() {
		while(true) {
			Socket sock = null;
			try {
				sock = ListenSocket.accept();
				Message msgIn = Message.receive(sock, null, -1);
				if(msgIn.getType() == MSG_TYPE.FILE_DOWNLOAD) {
					String receiveFileName = (String) msgIn.getContent();
					receiveFile(receiveFileName, sock);
					String[] array = receiveFileName.split("_");
					int jobId = Integer.parseInt(array[0]);
					SlaveCompute.fileLeftMap.put(jobId, SlaveCompute.fileLeftMap.get(jobId) - 1);
					if(SlaveCompute.fileLeftMap.get(jobId) == 0) {
						SlaveCompute.waitingThreadMap.get(jobId).start();
						SlaveCompute.waitingThreadMap.remove(jobId);
						SlaveCompute.fileLeftMap.remove(jobId);
					}
				}
				else {
					System.out.println("type is not FILE_DOWNLOAD.");
				}
				sock.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	/**
	 * Receive a file split from the remote
	 * 
	 * @param content
	 * @throws IOException 
	 */
	private void receiveFile(String fileName, Socket sock) throws IOException {
		DataInputStream sockData = 
				new DataInputStream(new BufferedInputStream(sock.getInputStream()));  
        DataOutputStream file = 
        		new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));  
		byte[] buf = new byte[Constants.BufferSize];
		
		int readNum;    
        while ((readNum = sockData.read(buf)) != -1) { 
        	file.write(buf, 0, readNum);    
        }     
		file.close();		
	}
}
