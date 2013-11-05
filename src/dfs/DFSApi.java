package dfs;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import mapreduce.UserDownload;

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
    	Socket socket = new Socket(Constants.MasterIp, Constants.MainRoutingPort);
    	new Message(MSG_TYPE.FILE_REQ, fileName).send(socket, null, -1);
    	Message msgIn = Message.receive(socket, null, -1);
    	if(msgIn.getType() != MSG_TYPE.FILE_REQ) {
    		System.out.println("This is not the message we want!");
    	}
    	ArrayList<CompleteMsg> compleMsg = (ArrayList<CompleteMsg>)msgIn.getContent();
    	for(CompleteMsg msg : compleMsg) {
    		SocketAddress sockAddr = msg.getSockAddr();
    		String realFileName = msg.getSplitName();
    		new UserDownload(sockAddr, realFileName).start();
    	}
    	socket.close();
    }

    public static void delete(String fileName) {

    }

    private static void get(String fileName, int num) {
    	
    }
}
