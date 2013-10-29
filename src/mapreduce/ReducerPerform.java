package mapreduce;

import socket.ReducerAckMsg;

public class ReducerPerform extends Thread {

	private ReducerAckMsg reducerAck;

	public ReducerPerform(ReducerAckMsg reducerAck) {
		this.reducerAck = reducerAck;
	}

	public void run(){
		
	}
}
