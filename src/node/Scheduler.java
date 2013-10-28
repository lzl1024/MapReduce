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
import socket.Message;
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
					new Message(Message.MSG_TYPE.NODE_FAIL_ACK, null).send(
							sock, null, -1);
					MasterMain.handleLeave((ArrayList<SocketAddress>) msg
							.getContent());
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
					JobInfo jobInfo = jobPool.get((Integer)msg.getContent());
					int remain = jobInfo.getRemainWorks() - 1;
					jobInfo.setRemainWorks(remain);
					
					//complete the whole work
					if (remain == 0) {
						concatOutFile(jobInfo.getOutSplitName(), jobInfo.getJob());
						new Message(Message.MSG_TYPE.WORK_COMPELETE, null)
						.send(jobInfo.getSock(), null, -1);
					}
					break;
				default:
					throw new IOException();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void concatOutFile(ArrayList<String> outSplitName, Job job) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Handle a mapreduce Job include schedule the mapper and reducer, execute
	 * mapreduce facility
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
		while (slaveList.get(i).getMapperTasks().size() <= Constants.IdealMapperJobs) {
			freeMappers.add(slaveList.get(i).getSocket());
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
		// Collections.sort(s)

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

	private void inviteMapper(SocketAddress sock, Job job,
			ArrayList<SocketAddress> reducerList, String key) {
		// TODO Auto-generated method stub

	}
}
