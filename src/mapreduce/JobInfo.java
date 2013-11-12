package mapreduce;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;

import util.Constants;

public class JobInfo {

    private Socket sock;
    private ArrayList<String> outSplitName;
    private Job job;
	private HashSet<String> mapperJobSet;


	
    public JobInfo(Job job, Socket sock, int remainWorks) {
        this.job = job;
        this.sock = sock;

        outSplitName = new ArrayList<String>();
        mapperJobSet = new HashSet<String>();
        
        for (int i = 1; i <= remainWorks; i++) {
            outSplitName.add(job.getJobID() + "##_" + i + "_1");
            mapperJobSet.add(Constants.FS_LOCATION + job.getJobID() + "##_" + i + "_1");
        }
    }
    
    public HashSet<String> getMapperJobSet() {
		return mapperJobSet;
	}

	public void setMapperJobSet(HashSet<String> mapperJobSet) {
		this.mapperJobSet = mapperJobSet;
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

    @Override
    public String toString() {
        return "[sock=" + sock
                + ", outSplitName=" + outSplitName + ", job=" + job + "]";
    }
}
