package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import mapreduce.Job;
import mapreduce.JobInfo;
import socket.MapperAckMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import socket.ReducerAckMsg;
import util.Constants;
import dfs.FileSplit;

/**
 * Master listen active message from slave
 */
public class Scheduler extends Thread {
    public static HashMap<Integer, JobInfo> jobPool = new HashMap<Integer, JobInfo>();

    @SuppressWarnings({ "resource", "unchecked" })
    public void run() {
        ServerSocket serverSock = null;
        try {
            serverSock = new ServerSocket(Constants.SlaveActivePort);
        } catch (IOException e) {
            System.out.println("Master Listener Socket cannot be openned");
            System.exit(0);
        }

        while (true) {
            try {
                Socket sock = serverSock.accept();
                sock.setSoTimeout(Constants.RegularTimout);

                Message msg = Message.receive(sock, null, -1);
                switch (msg.getType()) {
                // listen if one slave quit, send back quit message, handle it
                // and remove it from slavePool
                case SLAVE_QUIT:
                    ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                    tmp.add(sock.getRemoteSocketAddress());
                    MasterMain.handleLeave(tmp);
                    msg.send(sock, null, -1);
                    break;
                // mapper will find its reducer fail, master handles the fail
                // send back fail_ack
                case NODE_FAIL:
                    ArrayList<SocketAddress> content = (ArrayList<SocketAddress>) msg
                            .getContent();
                    try {
                        new Message(Message.MSG_TYPE.NODE_FAIL_ACK, null).send(
                                sock, null, -1);
                    } catch (Exception e) {
                        content.add(sock.getRemoteSocketAddress());
                    }
                    MasterMain.handleLeave(content);
                    break;
                // new Job comes
                case NEW_JOB:
                    try {
                        handleJob((Job) msg.getContent(), sock);
                    } catch (Exception e) {
                        e.printStackTrace();
                        new Message(Message.MSG_TYPE.WORK_FAIL, null).send(
                                sock, null, -1);
                    }
                    break;
                // one reducer complete the work
                case REDUCER_COMPLETE:
                    JobInfo jobInfo = jobPool.get((Integer) msg.getContent());
                    int remain = jobInfo.getRemainWorks() - 1;
                    jobInfo.setRemainWorks(remain);
                    // update slave pool
                    MasterMain.slavePool.get(sock.getRemoteSocketAddress())
                            .getReducerTasks()
                            .remove(new Integer(jobInfo.getJob().getJobID()));

                    // complete the whole work
                    if (remain == 0) {
                        deleteFile(jobInfo.getOutSplitName(),
                                jobInfo.getJob());
                        new Message(Message.MSG_TYPE.WORK_COMPELETE, null)
                                .send(jobInfo.getSock(), null, -1);
                    }
                    new Message(MSG_TYPE.REDUCER_COMPLETE, null).send(sock,
                            null, -1);
                    break;
                // one mapper complete the work
                case MAPPER_COMPLETE:
                    // update file layout and slavePool
                    MasterMain.slavePool.get(sock.getRemoteSocketAddress())
                            .getMapperTasks().remove((String) msg.getContent());
                    FileSplit.splitLayout.get(sock.getRemoteSocketAddress())
                            .remove((String) msg.getContent());
                    new Message(MSG_TYPE.MAPPER_COMPLETE, null).send(sock,
                            null, -1);
                    break;
                default:
                    throw new IOException();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handle a mapreduce Job include schedule the mapper and reducer, execute
     * mapreduce facility
     * 
     * @param sock
     * 
     * @param content
     */
    public void handleJob(Job job, Socket jobSock) throws Exception {
        if (!job.generateJobID()) {
            throw new Exception("Fail to generate Job ID");
        }

        // get free mappers
        ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>(
                MasterMain.slavePool.values());
        Collections.sort(slaveList, new SlaveInfo.MapperPrio());
        ArrayList<Socket> freeMappers = new ArrayList<Socket>();

        int i = 0;
        while (i < slaveList.size()
                && slaveList.get(i).getMapperTasks().size() <= Constants.IdealMapperJobs) {
            freeMappers.add(slaveList.get(i).getSocket());
            i++;
        }

        // split files
        HashMap<String, ArrayList<SocketAddress>> layout;
        try {
            layout = FileSplit.fileDispatch(freeMappers, Constants.FS_LOCATION
                    + job.getInputFile(), Constants.ReplFac,
                    Constants.IdealMapperNum, job.getJobID());
            System.out.println("Current FS Layout: " + FileSplit.splitLayout);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Split File Failed");
        }

        // schedule reducers
        ArrayList<SocketAddress> reducerList = new ArrayList<SocketAddress>();
        Collections.sort(slaveList, new SlaveInfo.ReducerPrio());
        for (i = 0; i < Constants.IdealReducerNum && i < slaveList.size(); i++) {
            reducerList.add(slaveList.get(i).getSocketAddr());
            inviteReducer(slaveList.get(i).getSocket(),
                    Constants.IdealMapperNum, job, i + 1);
        }

        // schedule mappers
        HashSet<SocketAddress> takedSock = new HashSet<SocketAddress>();
        // if every one in the list has a job, choose index failIterate
        int failIterate = 0;
        for (Entry<String, ArrayList<SocketAddress>> entry : layout.entrySet()) {
            // go through to find an available socket
            boolean flag = false;
            for (SocketAddress sock : entry.getValue()) {
                if (!takedSock.contains(sock)) {
                    inviteMapper(sock, job, reducerList, entry.getKey());
                    takedSock.add(sock);
                    // update slave info

                    flag = true;
                    break;
                }
            }
            // if not find, take the failIterate
            if (!flag) {
                SocketAddress tmp = entry.getValue().get(
                        failIterate % entry.getValue().size());
                failIterate++;
                inviteMapper(tmp, job, reducerList, entry.getKey());
            }
        }

        // add to job pool
        JobInfo jobInfo = new JobInfo(job, jobSock, Constants.IdealReducerNum);
        jobPool.put(job.getJobID(), jobInfo);
    }

    /**
     * add a reducer task
     * 
     * @param socket
     * 
     * @param idealMapperNum
     * @param job
     * @param index
     */
    private void inviteReducer(Socket socket, int idealMapperNum, Job job,
            int index) {
        ReducerAckMsg msg = new ReducerAckMsg(idealMapperNum, job, index);
        try {
            new Message(MSG_TYPE.REDUCER_REQ, msg).send(socket, null, -1);
            // update slave info
            MasterMain.slavePool.get(socket.getRemoteSocketAddress())
                    .getReducerTasks().add(job.getJobID());
        } catch (Exception e) {
            System.out.println("Invite reducer failed");
            ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
            tmp.add(socket.getRemoteSocketAddress());
            MasterMain.handleLeave(tmp);
        }
    }

    /**
     * add a mapper task
     * 
     * @param sock
     * @param job
     * @param reducerList
     * @param splitName
     */
    private void inviteMapper(SocketAddress sock, Job job,
            ArrayList<SocketAddress> reducerList, String splitName) {
        MapperAckMsg msg = new MapperAckMsg(splitName, job.getMapperClass(),
                reducerList);
        Socket socket = MasterMain.slavePool.get(sock).getSocket();
        try {
            new Message(MSG_TYPE.MAPPER_REQ, msg).send(socket, null, -1);
            // update slave info
            MasterMain.slavePool.get(socket.getRemoteSocketAddress())
                    .getMapperTasks().add(splitName);
        } catch (Exception e) {
            System.out.println("Invite mapper failed");
            ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
            tmp.add(socket.getRemoteSocketAddress());
            MasterMain.handleLeave(tmp);
        }

    }

    /**
     * delete all the file split in file system
     * 
     * @param outSplitName
     * @param job
     */
    private void deleteFile(ArrayList<String> outSplitName, Job job) {
        System.out.println(outSplitName);
        outSplitName = outSplitName;
        //TODO: we can do this as soon as work is done.
    }
}
