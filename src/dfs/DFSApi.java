package dfs;

import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

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

    public static void put(String fileName) throws Exception {
        Socket socket = new Socket(Constants.MasterIp,
                Constants.SlaveActivePort);

        // send file to master
        new Message(MSG_TYPE.PUT_FILE, fileName).send(socket, null, -1);

    }

    @SuppressWarnings("unchecked")
    public static void get(String fileName) throws Exception {
        String newfileName = Constants.FS_LOCATION + fileName + "_";
        Socket socket = new Socket(Constants.MasterIp,
                Constants.SlaveActivePort);

        // request split information
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

            Socket sock = new Socket();
            sock.connect(sockAddr);
            new Message(MSG_TYPE.GET_FILE, realFileName).send(sock, null, -1);
            new FileTransmitServer.SlaveDownload(sock, realFileName).start();
        }

        socket.close();

    }

    public static void delete(String fileName) {

    }
}
