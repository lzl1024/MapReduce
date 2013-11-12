package dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;

import util.Constants;

/**
 * This is the server handler to distribute file splits
 * 
 * @author zhuolinl dil1
 * 
 */
public class FileTransmitServer {
    /**
     * send files between slaves
     * 
     * @param fileName
     * @param socket
     * @throws IOException
     */
    public static void sendFile(String fileName, Socket socket)
            throws IOException {
        System.out.println("send File: " + fileName + ", sock: "
                + socket.getRemoteSocketAddress());

        DataInputStream file = null;
        DataOutputStream sockdata = null;
        try {
            file = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(fileName)));
            sockdata = new DataOutputStream(socket.getOutputStream());
            byte[] buf = new byte[Constants.BufferSize];
            int read_num;
            while ((read_num = file.read(buf)) != -1) {
                sockdata.write(buf, 0, read_num);
            }
        } catch (FileNotFoundException e) {
            System.out.println("The file does not exist");
            byte[] buf = new byte[Constants.BufferSize];
            sockdata.write(buf, 0, 0);
        }
System.out.println("SEND FILE ENDS!");
        sockdata.flush();
        file.close();

    }

    /**
     * Receive a file split from the remote
     * 
     * @param content
     * @throws Exception
     */
    public static void receiveFile(String fileName, Socket sock)
            throws Exception {

        DataInputStream sockData = new DataInputStream(new BufferedInputStream(
                sock.getInputStream()));
        DataOutputStream file = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(fileName)));
        byte[] buf = new byte[Constants.BufferSize];

        int readNum;
        boolean total = false;
        while ((readNum = sockData.read(buf)) != -1) {
            file.write(buf, 0, readNum);
            if (readNum != 0) {
                total = true;
            }
        }
        file.close();

        if (!total) {
            System.out.println("The file has been deleted on the other side : " + fileName);
            throw new Exception();
        } else {
            System.out.println("receive File: " + fileName + ", sock: "
                    + sock.getRemoteSocketAddress());
        }
    }

    /**
     * Download file from other slaves
     */
    public static class SlaveDownload extends Thread {

        private Socket sock;
        private String fileName;

        public SlaveDownload(Socket sock, String fileName) {
            this.sock = sock;
            this.fileName = fileName;
        }

        public void run() {
            try {
                FileTransmitServer.receiveFile(fileName, sock);
                if (!sock.isClosed()) {
                    sock.close();
                }
            } catch (Exception e) {
                System.out.println("receive file unsuccessfully, with "
                        + sock.getRemoteSocketAddress());
            }
        }
    }

    // Thread to send file
    public static class SlaveSendFile extends Thread {

        private String fileName;
        private Socket sock;

        public SlaveSendFile(Socket sock, String fileName) {
            this.fileName = fileName;
            this.sock = sock;
        }

        public void run() {
            try {
                FileTransmitServer.sendFile(fileName, sock);
                if (!sock.isClosed()) {
                    sock.close();
                }
            } catch (Exception e) {
                System.out.println("Fail to send file with "
                        + sock.getRemoteSocketAddress());
            }

        }
    }
}
