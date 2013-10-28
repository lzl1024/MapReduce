package mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

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
	private Class<?> MapperClass;
	private Class<?> ReducerClass;
	private String inputFile;
	private String outputFile;

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

	public Class<?> getMapperClass() {
		return MapperClass;
	}

	public void setMapperClass(Class<?> mapperClass) {
		MapperClass = mapperClass;
	}

	public Class<?> getReducerClass() {
		return ReducerClass;
	}

	public void setReducerClass(Class<?> reducerClass) {
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
		//send the job to master and wait for completion
		new Message(Message.MSG_TYPE.NEW_JOB, this).send(sock, null, -1);
		if (Message.receive(sock, null, -1).getType() != Message.MSG_TYPE.WORK_COMPELETE) {
			sock.close();
			throw new Exception("Job failed");
		}
		sock.close();
	}

}
