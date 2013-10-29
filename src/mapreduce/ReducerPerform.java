package mapreduce;

import java.util.ArrayList;

import socket.ReducerAckMsg;

public class ReducerPerform extends Thread {

    private int mapperNum;
    private String ReducerClass;
    private ArrayList<String> fileNames;

	public ReducerPerform(ReducerAckMsg reducerAck) {
		this.mapperNum = reducerAck.getMapperNum();
		this.ReducerClass = reducerAck.getReducerClass();
		this.fileNames = reducerAck.getfileNames();
	}

	public void run(){
		
	}
}
