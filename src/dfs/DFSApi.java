package dfs;

import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import mapreduce.UserDownload;
import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

/**
 * 
 * The api of the stand alone distributed file system
 * 
 */
public class DFSApi {

    public static void put(String fileName) {

    }

    @SuppressWarnings("unchecked")
    public static void get(String fileName) throws Exception {
        String newfileName = Constants.FS_LOCATION + fileName + "_";
        Socket socket = new Socket(Constants.MasterIp,
                Constants.SlaveActivePort);
        
        //request split information
        new Message(MSG_TYPE.FILE_REQ, newfileName).send(socket, null, -1);
        Message msgIn = Message.receive(socket, null, -1);
        if (msgIn.getType() != MSG_TYPE.FILE_REQ) {
            System.out.println("This is not the message we want!");
        }

        ArrayList<CompleteMsg> compleMsg = (ArrayList<CompleteMsg>) msgIn
                .getContent();

        // request files
        for (CompleteMsg msg : compleMsg) {

            SocketAddress sockAddr = msg.getSockAddr();
            String realFileName = msg.getSplitName();
            System.out.println("sockAddr" + sockAddr + " filename is"
                    + realFileName);

            new UserDownload(sockAddr, realFileName).start();
        }

        socket.close();

    }

    public static void delete(String fileName) {

    }
}
