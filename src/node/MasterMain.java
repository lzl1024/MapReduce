package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Job;
import node.SlaveInfo.reduceTaskUnit;
import socket.ChangeReduceMsg;
import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;
import dfs.FileSplit;

/**
 * The master node, contains scheduler
 * 
 * @author zhuolinl dil1
 * 
 */
public class MasterMain {
    // pool to record the slave socket
    public static ConcurrentHashMap<SocketAddress, SlaveInfo> slavePool = new ConcurrentHashMap<SocketAddress, SlaveInfo>();
    // key : slave to slave port, value : slave to master port
    public static ConcurrentHashMap<SocketAddress, SocketAddress> listenToActive = new ConcurrentHashMap<SocketAddress, SocketAddress>();
    public static int curPort;
    //key is listenToActive value is slavePool
    public static ConcurrentHashMap<SocketAddress, SocketAddress> failedActiveMap = new ConcurrentHashMap<SocketAddress, SocketAddress>();
    //key is slavePool
    public static ConcurrentHashMap<SocketAddress, SocketAddress> failedMap = new ConcurrentHashMap<SocketAddress, SocketAddress>();
    public static void main(String[] args) {
        // fill up the constants

        try {
            new Constants(args[0]);
            curPort = Constants.startPort;
        } catch (IOException e) {
            System.out.println("configure file missing!");
            System.exit(1);
        }

        // start scheduler
        Scheduler scheduler = new Scheduler();
        scheduler.start();

        // keep alive process start
        new MasterKeepAlive().start();

        new MasterManager().start();

        // start main routine
        executing(scheduler);
        System.exit(0);
    }

    @SuppressWarnings("resource")
    private static void executing(Scheduler scheduler) {
        ServerSocket serverSock = null;
        try {
            serverSock = new ServerSocket(Constants.MainRoutingPort);
        } catch (IOException e) {
            System.out.println("Master Server Socket cannot be openned");
            System.exit(0);
        }

        // manage join
        while (true) {
            Socket sock = null;
            try {
                sock = serverSock.accept();
                System.out.println("sock RemoteAddr"
                        + sock.getRemoteSocketAddress());
                sock.setSoTimeout(Constants.RegularTimout);
                curPort++;
                System.out.println("curPort is" + curPort);
                if (curPort > Constants.endPort) {
                    System.out.println("Port pool used up.");
                    System.exit(0);
                }
                new Message(MSG_TYPE.NOTIFY_PORT, curPort).send(sock, null, -1);

                if (Message.receive(sock, null, -1).getType() == MSG_TYPE.NOTIFY_PORT) {
                    slavePool.put(sock.getRemoteSocketAddress(), new SlaveInfo(
                            sock, MasterMain.curPort));
                    listenToActive.put(
                            slavePool.get(sock.getRemoteSocketAddress())
                                    .getSocketAddr(), sock
                                    .getRemoteSocketAddress());
                } else {
                    throw new Exception();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handle Failure or Quit situation 1. For workers mapper work: each work
     * select other proper node to do first check node with the file split. Then
     * change 2. For workers reduce work: find another node, and announce
     * everyone the replacer and the original one in the content of
     * NODE_FAIL_ACK message
     * 
     * @param content
     */
    public synchronized static HashMap<SocketAddress, SocketAddress> handleLeave(ArrayList<SocketAddress> removeListorg) {
    	
    	synchronized(MasterMain.failedActiveMap) {
    		synchronized(MasterMain.slavePool) {
    			synchronized(MasterMain.listenToActive) {
    	HashMap<SocketAddress, SocketAddress> map = new HashMap<SocketAddress, SocketAddress>();
    	ArrayList<SocketAddress> removeList = new ArrayList<SocketAddress>();
    	System.out.println("removeListorg is" + removeListorg);
    	for(SocketAddress delete : removeListorg) {
    		System.out.println("delete is" + delete);
    		System.out.println("MasterMain.failedMap" + MasterMain.failedActiveMap);
    		if(!MasterMain.failedActiveMap.containsValue(delete)) {
    			removeList.add(delete);
    		}
    	}

        System.out.println("removeList is" + removeList);

        ArrayList<SlaveInfo> removed = new ArrayList<SlaveInfo>();
        for (SocketAddress add : removeList) {
            removed.add(slavePool.get(add));
        }

        // delete the slave from pool, in case of the new mapper fail

        for (SocketAddress add : removeList) {
System.out.println("listenToActive is" + MasterMain.listenToActive);
System.out.println("add is" + add);
System.out.println("slavePool is" + MasterMain.slavePool);
			MasterMain.failedActiveMap.put(MasterMain.slavePool.get(add)
						.getSocketAddr(), add);
			MasterMain.failedMap.put(add, MasterMain.slavePool.get(add)
						.getSocketAddr());
            MasterMain.listenToActive.remove(MasterMain.slavePool.get(add)
                        .getSocketAddr());
            MasterMain.slavePool.remove(add);
                
                
        }
        

        ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>(
                MasterMain.slavePool.values());

        if (slavePool.size() == 0) {
            System.out.println("Leave all!");
            System.exit(0);
        }

        for (int k = 0; k < removeList.size(); k++) {
            SocketAddress sockAddr = removeList.get(k);
            // relocate its files
            handleFile(sockAddr);

            // move its map jobs to other hosts
            for (String fileSplit : removed.get(k).getMapperTasks()) {
                ArrayList<SocketAddress> sockAddrList = FileSplit.splitLayout
                        .get(fileSplit);

                if (sockAddrList != null && sockAddrList.size() > 0) {
                    Collections.sort(slaveList, new SlaveInfo.MapperPrio());
                    Scheduler
                            .inviteMapper(slaveList.get(0).getSocket()
                                    .getRemoteSocketAddress(), new Job(
                                    Scheduler.MapperJob.get(fileSplit)
                                            .getMapperClass()),
                                    Scheduler.MapperJob.get(fileSplit)
                                            .getReudcerList(), fileSplit);

                } else {
                    System.out.println(fileSplit
                            + " cannot find a mapper to handle, job fails");
                }
            }

            // failed node list
            ArrayList<SocketAddress> failList = new ArrayList<SocketAddress>();
            
            // change with balanced load
            Collections.sort(slaveList, new SlaveInfo.ReducerPrio());
System.out.println("NEW ADD ITEM IN MAP" + MasterMain.failedMap.get(sockAddr) + "  " + slaveList.get(0).getSocketAddr());
            map.put(MasterMain.failedMap.get(sockAddr), slaveList.get(0).getSocketAddr());
System.out.println("MAP AFTER ADDING " + map);
            for (SlaveInfo info : slaveList) {
                Socket sock = info.getSocket();
                ChangeReduceMsg msg = new ChangeReduceMsg(MasterMain.failedMap.get(sockAddr),
                        slaveList.get(0).getSocketAddr());
                try {
                    new Message(MSG_TYPE.CHANGE_REDUCELIST, msg).send(sock,
                            null, -1);
                } catch (Exception e) {
                    failList.add(sock.getRemoteSocketAddress());
                }
            }
            // move its reduce jobs to other hosts
            for (reduceTaskUnit reduceTask : removed.get(k).getReducerTasks()) {
                
                Scheduler.inviteReducer(slaveList.get(0).getSocket(), 
                        Scheduler.jobPool.get(reduceTask.jobID).getJob(), reduceTask.index, null);
                
            }

            // failed again, handle leave
            if (failList.size() > 0) {
                handleLeave(failList);
            }
        }
System.out.println("MAP BEFORE RETURN" + map);
        return map;
    			}
    		}
    	}
    	
    }

    /**
     * send failed node file to others
     * 
     * @param sockAddr
     */
    private static void handleFile(SocketAddress sockAddr) {
        // search all the files it have
        ArrayList<SlaveInfo> slave = new ArrayList<SlaveInfo>(
                MasterMain.slavePool.values());
        int it = 0;

        for (Entry<String, ArrayList<SocketAddress>> entry : FileSplit.splitLayout
                .entrySet()) {
            if (entry.getValue().contains(sockAddr)) {
                // find file, and get receiver
                entry.getValue().remove(sockAddr);
                if (entry.getValue().size() == 0) {
                    System.out.println("No replica any more : "
                            + entry.getKey());
                    System.exit(-1);
                }

                int cur = 0;
                Socket reciever = null;
                while (cur < slave.size()) {
                    Socket tmp = slave.get(it).getSocket();
                    if (!entry.getValue()
                            .contains(tmp.getRemoteSocketAddress())) {
                        reciever = tmp;
                        break;
                    }
                    cur++;
                    it = (it + 1) % slave.size();
                }

                if (cur == slave.size()) {
                    System.out.println("All of other has file : "
                            + entry.getKey());
                    continue;
                }

                // find sender active address
                SocketAddress sender = slavePool.get(entry.getValue().get(0))
                        .getSocketAddr();
                CompleteMsg msg = new CompleteMsg(entry.getKey(), sender, null);
                try {
                    new Message(MSG_TYPE.FILE_DOWNLOAD, msg).send(reciever,
                            null, -1);
                    // update layout
                    FileSplit.splitLayout.get(entry.getKey()).add(
                            reciever.getRemoteSocketAddress());
                    Message.receive(reciever, null, -1);
                    
                    
                } catch (Exception e) {
                    ArrayList<SocketAddress> add = new ArrayList<SocketAddress>();
                    add.add(reciever.getRemoteSocketAddress());
                    handleLeave(add);
                }
            }
        }
        System.out.println("handle file: " + FileSplit.splitLayout);
    }
}
