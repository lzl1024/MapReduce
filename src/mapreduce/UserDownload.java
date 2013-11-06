package mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.net.Socket;
import java.net.SocketAddress;

import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

public class UserDownload extends Thread {

    private SocketAddress sockAddr;
    private String fileName;

    public UserDownload(SocketAddress sockAddr, String fileName) {
        this.sockAddr = sockAddr;
        this.fileName = fileName;
    }

    /**
     * Download file from other slaves
     */
    public void run() {

        Socket sock = new Socket();
        try {
            sock.connect(sockAddr);
            new Message(MSG_TYPE.GET_FILE, fileName).send(sock, null, -1);
            DataInputStream inSocket = new DataInputStream(
                    sock.getInputStream());
            DataOutputStream outStream = new DataOutputStream(
                    new FileOutputStream(fileName));
            int read_num;
            byte[] buf = new byte[Constants.BufferSize];
            while ((read_num = inSocket.read(buf)) != -1) {
                outStream.write(buf, 0, read_num);
                outStream.flush();
            }
            inSocket.close();
            outStream.close();
        } catch (Exception e) {
            System.out.println("receive file unsuccessfully");
            e.printStackTrace();
        }

        System.out.println("receive file successfully");
    }
}
