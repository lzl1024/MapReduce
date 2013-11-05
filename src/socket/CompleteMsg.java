package socket;

import java.io.Serializable;
import java.net.SocketAddress;

public class CompleteMsg implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String splitName;
	private SocketAddress sockAddr;
	private Integer jobID;
	
	public CompleteMsg(String splitName, SocketAddress sockAddress, Integer jobID) {
		this.splitName = splitName;
		this.sockAddr = sockAddress;
		this.jobID = jobID;
	}
	
	public Integer getJobID() {
		return jobID;
	}

	public void setJobID(Integer jobID) {
		this.jobID = jobID;
	}

	public String getSplitName() {
		return splitName;
	}
	public void setSplitName(String splitName) {
		this.splitName = splitName;
	}
	public SocketAddress getSockAddr() {
		return sockAddr;
	}
	public void setSockAddr(SocketAddress sockAddr) {
		this.sockAddr = sockAddr;
	}

	

}
