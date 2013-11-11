package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;
import dfs.FileTransmitServer;

public class SlaveListen extends Thread {
    public static ServerSocket ListenSocket = null;
    public static SocketAddress sockComMsg;
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
            Message msgIn = null;
            try {
                sock = ListenSocket.accept();
                sockComMsg = sock.getLocalSocketAddress();
System.out.println("sock Remote" + sock.getRemoteSocketAddress());
                msgIn = Message.receive(sock, null, -1);
            } catch (Exception e) {
                e.printStackTrace();
                try {
					sock.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                continue;
            }
            switch (msgIn.getType()) {
            case FILE_DOWNLOAD:
            	CompleteMsg msgCom = (CompleteMsg) msgIn.getContent();
                String receiveFileName = msgCom.getSplitName();

                try {
System.out.println("sock in SlaveListen line 52 is " + sock + " fileName: "+ receiveFileName);
                    FileTransmitServer.receiveFile(receiveFileName
                            + Constants.REDUCE_FILE_SUFFIX, sock);

                } catch (Exception e) {
                    try {
                        // send node fail message to inform master
                        Socket failSock = new Socket(Constants.MasterIp,
                                Constants.SlaveActivePort);
                        ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                        tmp.add(msgCom.getSockAddr());

                        new Message(MSG_TYPE.NODE_FAIL, tmp).send(failSock,
                                null, -1);
                    } catch (Exception e1) {
                        System.out.println("Failed to connect with the Master");
                        System.exit(-1);
                    }
                }

                System.out.println("receiveFilename in slaveListen "
                        + receiveFileName);

                int jobId = Integer.parseInt(receiveFileName.substring(
                        Constants.FS_LOCATION.length(),
                        receiveFileName.indexOf("_")));

                System.out.println("JobID in slavelisten " + jobId);
                SlaveCompute.fileLeftMap.put(jobId,
                        SlaveCompute.fileLeftMap.get(jobId) - 1);

                // update slave file map to start specific reducer
                if (SlaveCompute.fileLeftMap.get(jobId) == 0) {
                    // start waiting threads
                    for (Thread thread : SlaveCompute.waitingThreadMap
                            .get(jobId)) {
                        thread.start();
                    }
                    SlaveCompute.waitingThreadMap.remove(jobId);
                    SlaveCompute.fileLeftMap.remove(jobId);
                }

                if (!sock.isClosed()) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        System.out.println("Socket close failed");
                    }
                }
                break;
            case NOTIFY_PORT:
                // try out this listen port
                System.out.println("Slave listen port open!");
                try {
                    msgIn.send(sock, null, -1);
                } catch (Exception e) {
                    System.out.println("liten port failed");
                    System.exit(-1);
                }
                break;
            case GET_FILE:
                System.out
                        .println("Slave now is requested to send files to user");

               new FileTransmitServer.SlaveSendFile(sock,
                        (String) msgIn.getContent()).start();

                break;
            case FILE_SPLIT_REQ:
                try {
                    FileTransmitServer.receiveFile((String) msgIn.getContent(),
                            sock);
                    if (!sock.isClosed()) {
                        sock.close();
                    }
                } catch (Exception e) {
                    System.out
                            .println("Failed to connect with master : FILE_SPLIT_REQ");
                }

                break;
            default:
                System.out.println("Type is not defined :" + msgIn.getType());
            }
        }
    }
}
