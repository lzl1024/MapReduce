package mapreduce;

import io.Context;
import io.IntWritable;
import io.Text;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import node.SlaveCompute;
import socket.MapperAckMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

public class MapperPerform extends Thread {

	private ArrayList<SocketAddress> reducerList;
	private String mapperClass;
	private String splitName;

	public MapperPerform(MapperAckMsg mapperAck) {
		this.reducerList = mapperAck.getReudcerList();
		this.mapperClass = mapperAck.getMapperClass();
		this.splitName = mapperAck.getSplitName();
	}

	public void run() {
		// open the record file
		BufferedReader reader = null;
		try {
			FileReader fd = new FileReader(this.splitName);
			reader = new BufferedReader(fd);

			// reflect the mapper class
			Class<?> obj = Class.forName(Constants.Class_PREFIX
					+ this.mapperClass);
			Constructor<?> objConstructor = obj.getConstructor();
			Mapper mapper = (Mapper) objConstructor.newInstance();

			String record;
			int i = 0;
			Context context = new Context(reducerList.size(), splitName);
			// process records line by line
			while ((record = reader.readLine()) != null) {
				mapper.map(new IntWritable(++i), new Text(record), context);
			}
			// close context
			if (!context.isClose()) {
				context.close();
			}
			reader.close();
			fd.close();

			// send each splits to reducers
			if (sendSplits()) {
				// send complete to master
				Socket sock = new Socket(Constants.MasterIp,
						Constants.SlaveActivePort);
				new Message(MSG_TYPE.MAPPER_COMPLETE, splitName).send(sock,
						null, -1);
				Message.receive(sock, null, -1);
				sock.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Internal Error!");
		}
	}

	private boolean sendSplits() throws Exception {
		ArrayList<SocketAddress> failNode = new ArrayList<SocketAddress>();
		for (int i = 0; i < reducerList.size(); i++) {
			Socket socket = new Socket();
			SocketAddress add = reducerList.get(i);
			String fileName = splitName + "_" + (i + 1);
			try {
				socket.connect(add);
				new Message(MSG_TYPE.FILE_DOWNLOAD, fileName).send(socket,
						null, -1);

				// send file
				DataInputStream file = new DataInputStream(
						new BufferedInputStream(new FileInputStream(fileName)));
				DataOutputStream sockdata = new DataOutputStream(
						socket.getOutputStream());
				byte[] buf = new byte[Constants.BufferSize]; 
				int read_num;
				while ((read_num = file.read(buf)) != -1) {  
					sockdata.write(buf, 0, read_num);  
                }
				sockdata.flush();
				file.close();

			} catch (IOException e) {
				// reducer fail
				System.out.println("Reducer failed");
				if (SlaveCompute.failedCache.containsKey(add)) {
					SlaveCompute.failedCache.get(add).add(fileName);
				} else {
					ArrayList<String> tmp = new ArrayList<String>();
					tmp.add(fileName);
					SlaveCompute.failedCache.put(add, tmp);
				}
				failNode.add(add);
			}
		}

		if (failNode.size() > 0) {
			// send to master
			Socket masterSocket = new Socket(Constants.MasterIp,
					Constants.SlaveActivePort);
			new Message(MSG_TYPE.NODE_FAIL, failNode).send(masterSocket, null,
					-1);
			Message.receive(masterSocket, null, -1);
			masterSocket.close();
		}
		return true;
	}
	public ArrayList<SocketAddress> getReduceList() {
		return this.reducerList;
	}
}
