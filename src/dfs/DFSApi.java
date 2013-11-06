package dfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

/**
 * 
 * The api of the standalone distributed file system
 * 
 */
public class DFSApi {

    public static void put(String fileName) {
    	
    }

    public static void get(String fileName) throws Exception {
    	String newfileName = Constants.FS_LOCATION + fileName + "_";
    	Socket socket = new Socket(Constants.MasterIp, Constants.SlaveActivePort);
    	new Message(MSG_TYPE.FILE_REQ, newfileName).send(socket, null, -1);
    	Message msgIn = Message.receive(socket, null, -1);
    	if(msgIn.getType() != MSG_TYPE.FILE_REQ) {
    		System.out.println("This is not the message we want!");
    	}
    	@SuppressWarnings("unchecked")
		ArrayList<CompleteMsg> compleMsg = (ArrayList<CompleteMsg>)msgIn.getContent();
    	
    	//receive and merge files
    	DataOutputStream outStream = new DataOutputStream(new FileOutputStream(fileName));
		int read_num;
    	for(CompleteMsg msg : compleMsg) {
    		
    		SocketAddress sockAddr = msg.getSockAddr();
    		String realFileName = msg.getSplitName();
    		System.out.println("sockAddr" + sockAddr + " filename is" + realFileName);
    		Socket sock = new Socket();
    		sock.connect(sockAddr);
    		
    		DataInputStream inSocket = new DataInputStream(sock.getInputStream());
			byte[] buf = new byte[Constants.BufferSize];
			while((read_num = inSocket.read(buf)) != -1) {
				outStream.write(buf, 0, read_num);
				outStream.flush();
			}
			
			sock.close();
			inSocket.close();
    		
    		//new UserDownload(sockAddr, realFileName).start();
    	}
    	outStream.close();
    }

    public static void delete(String fileName) {

    }

    /*public static void main(String[] args) {
    	FileSplit.splitLayout.put(arg0, arg1)
    }
    */
}
