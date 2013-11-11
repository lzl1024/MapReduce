package node;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import mapreduce.MapperPerform;
import mapreduce.ReducerPerform;
import socket.ChangeReduceMsg;
import socket.CompleteMsg;
import socket.MapperAckMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import socket.RecordWrapperMsg;
import socket.ReducerAckMsg;
import util.Constants;
import dfs.DeleteFileThread;
import dfs.FileTransmitServer;

/**
 * The computation routine of the slave
 */
public class SlaveCompute extends Thread {

    public static Socket sockToMaster;
    // failed reducer task, wait for new reducer to send files
    // key: socketAddress, value: splitName
    public static HashMap<SocketAddress, ArrayList<String>> failedCache = new HashMap<SocketAddress, ArrayList<String>>();
    public static ArrayList<Thread> mapperThreadList = new ArrayList<Thread>();
    public static HashMap<Integer, ArrayList<Thread>> waitingThreadMap = new HashMap<Integer, ArrayList<Thread>>();
    public static HashMap<Integer, Integer> fileLeftMap = new HashMap<Integer, Integer>();

    // constructor
    public SlaveCompute(Socket sock) {
        sockToMaster = sock;
    }

    public void run() {
        System.out.println("sockToMaster: " + sockToMaster.getLocalPort());
        while (true) {
            Message msgIn = null;
            try {
                msgIn = Message.receive(sockToMaster, null, -1);

                switch (msgIn.getType()) {
                case KEEP_ALIVE:
                    // Keep alive poll, send the message back
                    msgIn.send(sockToMaster, null, -1);
                    break;
                case MAPPER_REQ:
                    // Create a thread to perform mapper task
                    Thread newMapper = new MapperPerform(
                            (MapperAckMsg) msgIn.getContent());
                    mapperThreadList.add(newMapper);
                    newMapper.start();
                    break;
                case REDUCER_REQ:

                    new Message(MSG_TYPE.REDUCER_REQ, null).send(sockToMaster,
                            null, -1);

                    /*if(SlaveListen.ListenSocket.getLocalPort() == 9003) {
                    	System.exit(0);
                    }
                    */
                    // Create a thread to perform reducer task
                    ReducerAckMsg msgContent = (ReducerAckMsg) msgIn
                            .getContent();
                    
                    int jobID = msgContent.getJobID();
                    Thread newReducer = new ReducerPerform(msgContent);

                    // add the reducer to map and wait mapper to complete
                    if (waitingThreadMap.containsKey(jobID)) {
                        waitingThreadMap.get(jobID).add(newReducer);
                        fileLeftMap.put(jobID, fileLeftMap.get(jobID)
                                + msgContent.getfileNames().size());
                    } else {
                        ArrayList<Thread> threadList = new ArrayList<Thread>();
                        threadList.add(newReducer);
                        waitingThreadMap.put(jobID, threadList);
                        fileLeftMap
                                .put(jobID, msgContent.getfileNames().size());
                    }

                    System.out.println(fileLeftMap);
                    break;
                case NOTIFY_PORT:
                	System.out.println("curPort is" + (Integer) msgIn.getContent());
                    new SlaveListen((Integer) msgIn.getContent()).start();
                    // send back to master same msg
                    msgIn.send(sockToMaster, null, -1);
                    break;
                case CHANGE_REDUCELIST:
                    new SlaveChangeReduce((ChangeReduceMsg) msgIn.getContent())
                            .start();
                    break;
                case DELETE_FILE:
                    new DeleteFileThread((String) msgIn.getContent()).start();
                    break;
                case RANDOM_RECORD:
                    new Message(MSG_TYPE.RANDOM_RECORD,
                            findRecord((RecordWrapperMsg) msgIn.getContent()))
                            .send(sockToMaster, null, -1);
                    break;
                case FILE_DOWNLOAD:
                    // get file from another slave
                    CompleteMsg downloadMsg = (CompleteMsg) msgIn.getContent();
                    Socket downloadSocket = new Socket();
                    try {
                        downloadSocket.connect(downloadMsg.getSockAddr());
System.out.println("download soceket in line 127" + downloadSocket);                        
                        new Message(MSG_TYPE.GET_FILE,
                                downloadMsg.getSplitName()).send(
                                downloadSocket, null, -1);
                        new FileTransmitServer.SlaveDownload(downloadSocket,
                                downloadMsg.getSplitName()).start();               

                    } catch (Exception e) {
                    	e.printStackTrace();
                        try {
                            // send node fail message to inform master
                            Socket failSock = new Socket(Constants.MasterIp,
                                    Constants.SlaveActivePort);
                            ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                            tmp.add(downloadSocket.getRemoteSocketAddress());

                            new Message(MSG_TYPE.NODE_FAIL, tmp).send(failSock,
                                    null, -1);
                            
                        } catch (Exception e1) {
                            System.out
                                    .println("Failed to connect with the Master");
                            System.exit(-1);
                        }
                    }
                    new Message(MSG_TYPE.FILE_DOWNLOAD, null).send(sockToMaster, null, -1);
                    break;
                default:
                    break;
                }
            } catch (Exception e) {
                System.out.println("Connection to master broke");
                System.exit(0);
            }
            System.out.println(msgIn.getType() + " :handle success");
        }
    }

    private String findRecord(RecordWrapperMsg content) {
        long last = content.getRecordNum();
        String fileName = content.getFileName();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));

            String line = null;
            while (last-- > 0 && (line = reader.readLine()) != null)
                ;
            reader.close();
            return line;
        } catch (Exception e) {
            System.out.println("Get record from " + fileName + " error");
        }
        return null;
    }
}
