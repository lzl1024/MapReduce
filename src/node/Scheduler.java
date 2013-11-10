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
import socket.CompleteMsg;
import socket.MapperAckMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import socket.RecordWrapperMsg;
import socket.ReducerAckMsg;
import util.Constants;
import dfs.FileSplit;
import dfs.FileTransmitServer;

/**
 * Master listen active message from slave
 */
public class Scheduler extends Thread {
    public static HashMap<Integer, JobInfo> jobPool = new HashMap<Integer, JobInfo>();
    public static HashMap<String, MapperAckMsg> MapperJob = new HashMap<String, MapperAckMsg>();

    public static HashSet<Integer> killedJob = new HashSet<Integer>();

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
            Socket sock = null;
            Message msg = null;
            try {
                sock = serverSock.accept();
                sock.setSoTimeout(Constants.RegularTimout);

                msg = Message.receive(sock, null, -1);
            } catch (Exception e1) {
                System.out.println("Failed to receive message");
                continue;
            }

            switch (msg.getType()) {
            // listen if one slave quit, send back quit message, handle it
            // and remove it from slavePool
            case SLAVE_QUIT:
                ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                tmp.add((SocketAddress) msg.getContent());
                MasterMain.handleLeave(tmp);
                try {
                    msg.send(sock, null, -1);
                } catch (Exception e1) {
                }
                break;
            // mapper will find its reducer fail, master handles the fail
            // send back fail_ack
            case NODE_FAIL:
                ArrayList<SocketAddress> content = new ArrayList<SocketAddress>();

                for (SocketAddress add : (ArrayList<SocketAddress>) msg
                        .getContent()) {
                    content.add(MasterMain.listenToActive.get(add));
                }
                MasterMain.handleLeave(content);
                break;
            // new Job comes
            case NEW_JOB:
                try {
                    handleJob((Job) msg.getContent(), sock);
                } catch (Exception e) {
                    try {
                        new Message(Message.MSG_TYPE.WORK_FAIL, null).send(
                                sock, null, -1);
                    } catch (Exception e1) {
                        System.out.println("Cannot connect to user: WORK FAIL");
                    }
                }
                break;
            // one reducer complete the work
            case REDUCER_COMPLETE:
                System.out.println("receive a REDUCER_COMPLETE");
                CompleteMsg comMsg = (CompleteMsg) msg.getContent();
                ArrayList<SocketAddress> newList = new ArrayList<SocketAddress>();
                newList.add(comMsg.getSockAddr());
                FileSplit.splitLayout.put(comMsg.getSplitName(), newList);
                // add the splits number in slave
                MasterMain.slavePool.get(comMsg.getSockAddr()).setSplits(
                        MasterMain.slavePool.get(comMsg.getSockAddr())
                                .getSplits() + 1);

                JobInfo jobInfo = jobPool.get(comMsg.getJobID());
                int remain = jobInfo.getRemainWorks() - 1;
                jobInfo.setRemainWorks(remain);
                // update slave pool
                MasterMain.slavePool.get(comMsg.getSockAddr())
                        .getReducerTasks()
                        .remove(new Integer(jobInfo.getJob().getJobID()));

                // complete the whole work
                if (remain == 0) {
                    System.out.println(FileSplit.splitLayout);

                    Integer jobID = jobInfo.getJob().getJobID();
                    // delete the file in master and in itself
                    deleteFile(Integer.toString(jobID));
                    try {
                        if (killedJob.contains(jobID)) {
                            new Message(Message.MSG_TYPE.WORK_KILLED, jobID)
                                    .send(jobInfo.getSock(), null, -1);
                            synchronized (killedJob) {
                                killedJob.remove(jobID);
                            }
                        } else {
                            new Message(Message.MSG_TYPE.WORK_COMPLETE, jobID)
                                    .send(jobInfo.getSock(), null, -1);
                        }
                    } catch (Exception e) {
                        System.out.println("Cannot connect to user: WORK DONE");
                    }
                    jobPool.remove(jobID);
                }

                try {
                    new Message(MSG_TYPE.REDUCER_COMPLETE, null).send(sock,
                            null, -1);
                } catch (Exception e) {
                    System.out.println("Reducer complete msg failed");
                }
                break;
            // one mapper complete the work
            case MAPPER_COMPLETE:
                System.out.println("receive a MAPPER_COMPLETE");
                // update file layout and slavePool
                CompleteMsg receiveMsg = (CompleteMsg) msg.getContent();

                MasterMain.slavePool.get(receiveMsg.getSockAddr())
                        .getMapperTasks().remove(receiveMsg.getSplitName());

                System.out.println("After Mapper FS Layout: "
                        + FileSplit.splitLayout);

                try {
                    new Message(MSG_TYPE.MAPPER_COMPLETE, null).send(sock,
                            null, -1);
                } catch (Exception e) {
                    System.out
                            .println("Send back message fail: MAPPER_COMPLETE");
                }
                break;

            case FILE_REQ:
                // reply the get file request and choose tell user which
                // node to find
                ArrayList<CompleteMsg> result = findFiles((String) msg
                        .getContent());
                try {
                    new Message(MSG_TYPE.FILE_REQ, result).send(sock, null, -1);
                } catch (Exception e) {
                    System.out.println("Fail to connect with user: FILE_REQ");
                }
                break;
            case PUT_FILE:
                // user put request, upload file to the master
                // wait for split by map task
                try {
                    FileTransmitServer.receiveFile((String) msg.getContent(),
                            sock);
                } catch (Exception e) {
                    System.out.println("Fail to connect with user: PUT_FILE");
                }
                break;
            case PUT_FILE_FS:
                // user put request, upload file to the master and split
                // the file by split load balance
                try {
                    FileTransmitServer.receiveFile(Constants.FS_LOCATION
                            + (String) msg.getContent(), sock);
                } catch (Exception e) {
                    System.out
                            .println("Fail to connect with user: PUT_FILE_FS");
                }

                // sort slave according to file load and dispatch
                ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>(
                        MasterMain.slavePool.values());
                Collections.sort(slaveList, new SlaveInfo.FilePrio());
                ArrayList<SocketAddress> targets = new ArrayList<SocketAddress>();
                for (SlaveInfo slave : slaveList) {
                    targets.add(slave.getSocketAddr());
                }

                try {
                    FileSplit.fileDispatch(targets, (String) msg.getContent(),
                            -1, null, null);
                } catch (Exception e) {
                    System.out.println("Failed to dispatch file");
                }
                break;
            case DELETE_FILE:
                try {
                    new Message(MSG_TYPE.DELETE_FILE, null)
                            .send(sock, null, -1);
                } catch (Exception e) {
                    System.out
                            .println("Fail to connect with user: DELETE_FILE");
                }
                deleteFile((String) msg.getContent());
                break;
            case RANDOM_RECORD:
                try {
                    new Message(MSG_TYPE.RANDOM_RECORD,
                            findRecord((RecordWrapperMsg) msg.getContent()))
                            .send(sock, null, -1);
                } catch (Exception e) {
                    System.out
                            .println("Fail to connect with user: RANDOM_RECORD");
                }
                break;
            default:
                System.out.println("Undefined Message :" + msg.getType());
            }

        }
    }

    /**
     * find the record in file splits layout
     * 
     * @param recordWrapper
     * @return
     */
    private String findRecord(RecordWrapperMsg recordWrapper) {
        Long last = 0L;
        ArrayList<Long> singleLayout = FileSplit.recordLayout.get(recordWrapper
                .getFileName());
        int i;
        for (i = 0; i < singleLayout.size(); i++) {
            if (last < recordWrapper.getRecordNum()
                    && singleLayout.get(i) >= recordWrapper.getRecordNum()) {
                break;
            }
            last = singleLayout.get(i);
        }

        // find which file has the record, go and get it
        if (i < singleLayout.size()) {
            String fileName = Constants.FS_LOCATION
                    + recordWrapper.getFileName() + "_" + (i + 1);
            last = recordWrapper.getRecordNum() - last;
            Socket sock = null;
            try {
                 sock = MasterMain.slavePool.get(
                        FileSplit.splitLayout.get(fileName).get(0)).getSocket();

                RecordWrapperMsg msgContent = new RecordWrapperMsg(last,
                        fileName);
                new Message(MSG_TYPE.RANDOM_RECORD, msgContent).send(sock,
                        null, -1);

                return (String) Message.receive(sock, null, -1).getContent();
            } catch (Exception e) {
                System.out.println("Get record from " + fileName + " error");
                ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                tmp.add(sock.getRemoteSocketAddress());
                MasterMain.handleLeave(tmp);
            }
        }

        return null;
    }

    /**
     * find file splits in file system
     * 
     * @param filename
     * @return
     */
    private ArrayList<CompleteMsg> findFiles(String filename) {
        String reqFileName = filename;
        ArrayList<CompleteMsg> result = new ArrayList<CompleteMsg>();

        for (String e : FileSplit.splitLayout.keySet()) {
            if (e.startsWith(reqFileName)) {
                ArrayList<SocketAddress> sockAddrList = FileSplit.splitLayout
                        .get(e);
                SocketAddress targetAddr = sockAddrList.get(0);
                result.add(new CompleteMsg(e, MasterMain.slavePool.get(
                        targetAddr).getSocketAddr(), null));

            }
        }
        System.out.println(result);

        return result;
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

        // add to job pool
        JobInfo jobInfo = new JobInfo(job, jobSock, Constants.IdealReducerNum);
        jobPool.put(job.getJobID(), jobInfo);

        // get free mappers
        ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>(
                MasterMain.slavePool.values());
        Collections.sort(slaveList, new SlaveInfo.MapperPrio());
        ArrayList<SocketAddress> freeMappers = new ArrayList<SocketAddress>();

        int i = 0;
        while (i < slaveList.size()
                && slaveList.get(i).getMapperTasks().size() <= Constants.IdealMapperJobs) {
            freeMappers.add(slaveList.get(i).getSocketAddr());
            System.out.println("free Mapper is "
                    + slaveList.get(i).getSocketAddr());
            i++;

        }

        // split files
        HashMap<String, ArrayList<SocketAddress>> layout;
        try {
            layout = FileSplit.fileDispatch(freeMappers, job.getInputFile(),
                    job.getJobID(), job.getRecordBegin(), job.getRecordEnd());
            System.out.println("Current FS Layout: " + FileSplit.splitLayout);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Split File Failed");
        }

        // schedule reducers
        ArrayList<SocketAddress> reducerList = new ArrayList<SocketAddress>();
        Collections.sort(slaveList, new SlaveInfo.ReducerPrio());

        for (i = 0; i < slaveList.size()
                && slaveList.get(i).getReducerTasks().size() < Constants.IdealReducerJobs; i++) {
            if (reducerList.size() < Constants.IdealReducerNum) {
                reducerList.add(slaveList.get(i).getSocketAddr());
                inviteReducer(slaveList.get(i).getSocket(), job, i + 1);
            } else {
                break;
            }
        }

        if (reducerList.size() == 0) {
            System.out.println("All reducers are full.");
            System.exit(0);
        } else {
            System.out.println("reducer size is " + reducerList.size());
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
    public static void inviteReducer(Socket socket, Job job, int index) {
        ReducerAckMsg msg = new ReducerAckMsg(Constants.IdealMapperNum, job,
                index);
        try {
            // update slave info
            MasterMain.slavePool.get(socket.getRemoteSocketAddress())
                    .getReducerTasks().add(job.getJobID());
            new Message(MSG_TYPE.REDUCER_REQ, msg).send(socket, null, -1);
            Message.receive(socket, null, -1);

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
    public static void inviteMapper(SocketAddress sock, Job job,
            ArrayList<SocketAddress> reducerList, String splitName) {
        MapperAckMsg msg = new MapperAckMsg(splitName, job.getMapperClass(),
                reducerList);
        MapperJob.put(splitName, msg);
        Socket socket = MasterMain.slavePool.get(sock).getSocket();
        try {
            // update slave info
            MasterMain.slavePool.get(socket.getRemoteSocketAddress())
                    .getMapperTasks().add(splitName);
            new Message(MSG_TYPE.MAPPER_REQ, msg).send(socket, null, -1);

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
    private void deleteFile(String fileName) {

        fileName = fileName + "_";
        ArrayList<SocketAddress> failList = new ArrayList<SocketAddress>();

        // update splitLayout in case of concurrent problem
        ArrayList<String> deleteList = new ArrayList<String>();
        String newFileName = Constants.FS_LOCATION + fileName;

        for (String name : FileSplit.splitLayout.keySet()) {
            if (name.startsWith(newFileName)) {
                deleteList.add(name);
            }
        }
        synchronized (FileSplit.splitLayout) {
            for (String name : deleteList) {
                FileSplit.splitLayout.remove(name);
            }
        }

        // let slaves to search their local fs to delete file
        for (SlaveInfo slave : MasterMain.slavePool.values()) {
            try {
                new Message(MSG_TYPE.DELETE_FILE, fileName).send(
                        slave.getSocket(), null, -1);
            } catch (Exception e) {
                System.out.println("delete file failed");
                failList.add(slave.getSocket().getRemoteSocketAddress());
            }
        }

        System.out.println("After delete:" + FileSplit.splitLayout);
        if (failList.size() > 0) {
            MasterMain.handleLeave(failList);
        }

    }
}
