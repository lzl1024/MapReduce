package dfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;

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
        new Message(MSG_TYPE.PUT_FILE_FS, fileName).send(socket, null, -1);

        FileTransmitServer.sendFile(Constants.FS_LOCATION + fileName, socket);
        if (!socket.isClosed()) {
            socket.close();
        }
    }

    @SuppressWarnings("unchecked")
    public static void get(String fileName, String newName, boolean getSplits)
            throws Exception {
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

        // get splits
        if (getSplits) {
            // request files
            for (CompleteMsg msg : compleMsg) {

                SocketAddress sockAddr = msg.getSockAddr();
                String realFileName = msg.getSplitName();
                System.out.println("sockAddr" + sockAddr + " filename is"
                        + realFileName);

                Socket sock = new Socket();
                sock.connect(sockAddr);
                new Message(MSG_TYPE.GET_FILE, realFileName).send(sock, null,
                        -1);
                new FileTransmitServer.SlaveDownload(sock, realFileName)
                        .start();
            }
        } else {
            // get merged files
            Collections.sort(compleMsg, new CompleteMsg.NamePrio());
            // receive and merge files
            DataOutputStream outStream = new DataOutputStream(
                    new FileOutputStream(Constants.FS_LOCATION + newName));
            for (CompleteMsg msg : compleMsg) {

                SocketAddress sockAddr = msg.getSockAddr();
                String realFileName = msg.getSplitName();
                Socket sock = new Socket();
                sock.connect(sockAddr);
                new Message(MSG_TYPE.GET_FILE, realFileName).send(sock, null,
                        -1);

                DataInputStream inSocket = new DataInputStream(
                        sock.getInputStream());
                byte[] buf = new byte[Constants.BufferSize];
                int read_num;
                while ((read_num = inSocket.read(buf)) != -1) {
                    outStream.write(buf, 0, read_num);
                    outStream.flush();
                }

                sock.close();
                inSocket.close();
                System.out.println("receive file successfully");
            }
            outStream.close();
        }

        socket.close();

    }

    public static void delete(String fileName) {
        Socket socket;
        try {
            socket = new Socket(Constants.MasterIp, Constants.SlaveActivePort);
            // send file to master
            new Message(MSG_TYPE.DELETE_FILE, fileName).send(socket, null, -1);
            Message.receive(socket, null, -1);
            socket.close();
        } catch (Exception e) {
            System.out.println("fail to commuicate with Master");
            System.exit(-1);
        }

    }
    
    public static String readRecord(Long recordNum, String fileName) {
        Socket socket;
        String record = null;
        try {
            socket = new Socket(Constants.MasterIp, Constants.SlaveActivePort);
            // send file to master
            new Message(MSG_TYPE.RANDOM_RECORD, new RecordWrapper(recordNum, fileName))
            .send(socket, null, -1);
            record = (String) Message.receive(socket, null, -1).getContent();
            socket.close();
        } catch (Exception e) {
            System.out.println("fail to commuicate with Master");
            System.exit(-1);
        }
        
        return record;
    }
    
}
