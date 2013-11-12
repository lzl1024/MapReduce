package mapreduce;

import io.Context;
import io.LongWritable;
import io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import node.SlaveCompute;
import node.SlaveListen;
import socket.CompleteMsg;
import socket.MapperAckMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;
import dfs.FileTransmitServer;

public class MapperPerform extends Thread {

    private ArrayList<SocketAddress> reducerList;
    private String mapperClass;
    private String splitName;
    public static ConcurrentHashMap<SocketAddress, ArrayList<String>> sentFileMap = 
    		new ConcurrentHashMap<SocketAddress, ArrayList<String>>();
    public MapperPerform(MapperAckMsg mapperAck) {
        System.out.println("MapperAckMsg is" + mapperAck.toString());
        this.reducerList = mapperAck.getReudcerList();
        this.mapperClass = mapperAck.getMapperClass();
        this.splitName = mapperAck.getSplitName();
    }

    /**
     * Main routine to perform mapper
     */
    public void run() {
        // open the record file
        BufferedReader reader = null;
        try {
            FileReader fd = new FileReader(this.splitName);
            reader = new BufferedReader(fd);
            
            // reflect the mapper class
            Class<?> obj = Class.forName(Constants.Class_PREFIX
                    + this.mapperClass);
            Constructor<?> objConstructor = obj.getConstructor();
            Mapper mapper = (Mapper) objConstructor.newInstance();

            String record;
            long i = 0;

            Context context = new Context(reducerList.size(), splitName);
            // process records line by line
            while ((record = reader.readLine()) != null) {
                mapper.map(new LongWritable(++i), new Text(record), context);
            }
            // close context
            if (!context.isClose()) {
                context.close();
            }
            reader.close();
            fd.close();

            
            // send each splits to reducers
            sendSplits();
           
            // send complete to master
            Socket sock = new Socket(Constants.MasterIp,
                    Constants.SlaveActivePort);

            CompleteMsg mapperComMsg = new CompleteMsg(splitName,
                    SlaveCompute.sockToMaster.getLocalSocketAddress(), null);
            new Message(MSG_TYPE.MAPPER_COMPLETE, mapperComMsg).send(sock,
                    null, -1);
            Message.receive(sock, null, -1);
            sock.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Internal Error!");
        }
    }

    /**
     * Send the splits to its reducer according the jobs' reducer list
     * @return
     */
    private boolean sendSplits() {
        ArrayList<SocketAddress> failNode = new ArrayList<SocketAddress>();
        for (int i = 0; i < reducerList.size(); i++) {
            Socket socket = new Socket();
            SocketAddress add = reducerList.get(i);
            String fileName = splitName + "_" + (i + 1);

            try {
                socket.connect(add);
                
                new Message(MSG_TYPE.FILE_DOWNLOAD, new CompleteMsg(fileName, SlaveListen.sockComMsg, null)).send(socket,
                        null, -1);
                if(sentFileMap.containsKey(socket.getRemoteSocketAddress())) {
                	ArrayList<String> tmpList = sentFileMap.get(socket.getRemoteSocketAddress());
                	tmpList.add(fileName);
                	
                }
                else {
                	ArrayList<String> newList = new ArrayList<String>();
                	newList.add(fileName);
                	sentFileMap.put(socket.getRemoteSocketAddress(), newList);
                }
                	
                // send file
                FileTransmitServer.sendFile(fileName, socket);
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (Exception e) {
                // reducer fail
                System.out.println("Reducer failed");
                if (SlaveCompute.failedCache.containsKey(add)) {
                    SlaveCompute.failedCache.get(add).add(fileName);
                } else {
                    ArrayList<String> tmp = new ArrayList<String>();
                    tmp.add(fileName);
                    SlaveCompute.failedCache.put(add, tmp);
                }
                failNode.add(add);
                
                System.out.println(SlaveCompute.failedCache);
            }
        }

        if (failNode.size() > 0) {
            // send to master
            Socket masterSocket;
            try {
                masterSocket = new Socket(Constants.MasterIp,
                        Constants.SlaveActivePort);
                new Message(MSG_TYPE.NODE_FAIL, failNode).send(masterSocket,
                        null, -1);
                Message.receive(masterSocket, null, -1);
                masterSocket.close();
            } catch (Exception e) {
                System.out.println("Failed to connect with the Master");
                System.exit(-1);
            }

            return false;
        }
        return true;
    }

    public ArrayList<SocketAddress> getReduceList() {
        return this.reducerList;
    }
}
