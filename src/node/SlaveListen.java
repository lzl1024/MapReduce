package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import socket.Message;
import util.Constants;
import dfs.FileTransmitServer;

public class SlaveListen extends Thread {
    private ServerSocket ListenSocket = null;

    public SlaveListen(int port) {
        try {
            this.ListenSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Cannot open listen socket");
            System.exit(-1);
        }
    }

    public void run() {
        while (true) {
            Socket sock = null;
            try {

                sock = ListenSocket.accept();
                Message msgIn = Message.receive(sock, null, -1);

                switch (msgIn.getType()) {
                case FILE_DOWNLOAD:
                    String receiveFileName = (String) msgIn.getContent();

                    FileTransmitServer.receiveFile(receiveFileName
                            + Constants.REFUCE_FILE_SUFFIX, sock);

                    System.out.println("receiveFilename in slaveListen"
                            + receiveFileName);

                    int jobId = Integer.parseInt(receiveFileName.substring(
                            Constants.FS_LOCATION.length(),
                            receiveFileName.indexOf("_")));

                    System.out.println("JobID in slavelisten" + jobId);
                    SlaveCompute.fileLeftMap.put(jobId,
                            SlaveCompute.fileLeftMap.get(jobId) - 1);

                    // update slave file map to start specific reducer
                    if (SlaveCompute.fileLeftMap.get(jobId) == 0) {
                        SlaveCompute.waitingThreadMap.get(jobId).start();
                        SlaveCompute.waitingThreadMap.remove(jobId);
                        SlaveCompute.fileLeftMap.remove(jobId);
                    }

                    if (!sock.isClosed()) {
                        sock.close();
                    }
                    break;
                case NOTIFY_PORT:
                    // try out this listen port
                    System.out.println("Slave listen port open!");
                    msgIn.send(sock, null, -1);
                    break;
                case GET_FILE:
                    System.out
                            .println("Slave now is requested to send files to user");

                    new FileTransmitServer.SlaveSendFile(sock,
                            (String) msgIn.getContent()).start();
                    break;
                default:
                    System.out.println("type is not defined");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
