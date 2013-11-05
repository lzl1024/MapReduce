package mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

import dfs.DFSApi;

import node.Scheduler;
import socket.Message;
import util.Constants;

/**
 * 
 * Mapreduce Job class
 * 
 */
public class Job implements Serializable {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private int jobID;
    private String jobName;
    private String MapperClass;
    private String ReducerClass;
    private String inputFile;
    private String outputFile;
    private Class<?> ReducerKeyClass;
    private Class<?> ReducerValueClass;

    public Job() {}
    public Job(int jobId) {
    	this.jobID = jobId;
    }
    public Job(String mapperClass) {
    	this.MapperClass = mapperClass;
    }
    public int getJobID() {
        return jobID;
    }
    
    public void setJobID(int jobID) {
        this.jobID = jobID;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getMapperClass() {
        return MapperClass;
    }

    public void setMapperClass(String mapperClass) {
        MapperClass = mapperClass;
    }

    public String getReducerClass() {
        return ReducerClass;
    }

    public void setReducerClass(String reducerClass) {
        ReducerClass = reducerClass;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public Class<?> getReducerKeyClass() {
        return ReducerKeyClass;
    }

    public void setReducerKeyClass(Class<?> reducerKeyClass) {
        ReducerKeyClass = reducerKeyClass;
    }

    public Class<?> getReducerValueClass() {
        return ReducerValueClass;
    }

    public void setReducerValueClass(Class<?> reducerValueClass) {
        ReducerValueClass = reducerValueClass;
    }

    /**
     * Wait job to complete
     * 
     * @throws Exception
     */
    public void waitForCompletion(String config) throws Exception {
        // fill up the constants
        try {
            new Constants(config);
        } catch (IOException e) {
            System.out.println("configure file missing!");
            System.exit(1);
        }

        Socket sock = new Socket(Constants.MasterIp, Constants.SlaveActivePort);
        // send the job to master and wait for completion
        new Message(Message.MSG_TYPE.NEW_JOB, this).send(sock, null, -1);
        Message msgIn = Message.receive(sock, null, -1);
        if (msgIn.getType() != Message.MSG_TYPE.WORK_COMPELETE) {
            sock.close();
            throw new Exception("Job failed");
        }
        else {
        	String jobID = (String)msgIn.getContent();
        	DFSApi.get(jobID + "##_");
        }
        sock.close();
    }

    public boolean generateJobID() {
        // generate job ID
        int i = 0;
        while (i < 1000
                && Scheduler.jobPool.containsKey(jobID = (int) (Math.random()
                        * Constants.Random_Base))) {
            i++;
        }
        return i == 1000 ? false : true;
    }
}
