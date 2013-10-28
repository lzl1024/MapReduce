package mapreduce;

import java.net.Socket;
import java.util.ArrayList;

public class JobInfo {

	private int remainWorks;
	private Socket sock;
	private ArrayList<String> outSplitName;
	private Job job;

	public JobInfo(Job job, Socket sock, int remainWorks) {
		this.job = job;
		this.sock = sock;
		this.remainWorks = remainWorks;
		outSplitName = new ArrayList<String>();
		for (int i = 1; i <= remainWorks; i++) {
			outSplitName.add(job.getJobID()+"_"+i);
		}
	}

	public int getRemainWorks() {
		return remainWorks;
	}

	public void setRemainWorks(int remainWorks) {
		this.remainWorks = remainWorks;
	}

	public Socket getSock() {
		return sock;
	}

	public void setSock(Socket sock) {
		this.sock = sock;
	}

	public ArrayList<String> getOutSplitName() {
		return outSplitName;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}
}
