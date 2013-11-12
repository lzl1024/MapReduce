package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;

import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;
import dfs.FileTransmitServer;

/**
 * 
 * The slave will listen this port for message
 *
 */
public class SlaveListen extends Thread {
    public static ServerSocket ListenSocket = null;
    public static SocketAddress sockComMsg;
    
    public SlaveListen(int port) {
        try {
            ListenSocket = new ServerSocket(port);
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
                msgIn = Message.receive(sock, null, -1);
            } catch (Exception e) {
                e.printStackTrace();
                try {
					sock.close();
				} catch (IOException e1) {
					System.out.println("Failed to close the listen socket");
					e1.printStackTrace();
				}
                continue;
            }
            switch (msgIn.getType()) {
            case FILE_DOWNLOAD:
            // request to download a file on the other side
            	CompleteMsg msgCom = (CompleteMsg) msgIn.getContent();
                String receiveFileName = msgCom.getSplitName();

                try {
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

                int jobId = Integer.parseInt(receiveFileName.substring(
                        Constants.FS_LOCATION.length(),
                        receiveFileName.indexOf("_")));

                if(SlaveCompute.fileComeMap.containsKey(jobId)) {
                	SlaveCompute.fileComeMap.get(jobId).add(receiveFileName);
                }else {
                	//SlaveCompute.waitingThreadMap.put(jobId, new ArrayList<Thread>());
                	HashSet<String> newSet = new HashSet<String>();
                	newSet.add(receiveFileName);
                	SlaveCompute.fileComeMap.put(jobId, newSet);
                }

                // update slave file map to start specific reducer
                if (SlaveCompute.fileLeftMap.containsKey(jobId) && SlaveCompute.fileLeftMap.get(jobId) == 
                		SlaveCompute.fileComeMap.get(jobId).size()) {
                    // start waiting threads
                    for (Thread thread : SlaveCompute.waitingThreadMap
                            .get(jobId)) {
                        thread.start();
                    }
                    SlaveCompute.waitingThreadMap.remove(jobId);
                    SlaveCompute.fileLeftMap.remove(jobId);
                    SlaveCompute.fileComeMap.remove(jobId);
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
                        .println("Slave now is requested to send files to user: " + (String) msgIn.getContent());

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
                	e.printStackTrace();
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
