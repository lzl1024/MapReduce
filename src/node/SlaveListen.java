package node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import socket.Message;
import socket.Message.MSG_TYPE;
import util.Constants;

public class SlaveListen extends Thread {
	private ServerSocket ListenSocket = null;

	public class SlaveSendFile extends Thread {

		private String fileName;
		private Socket sock;

		public SlaveSendFile(Socket sock, String fileName) {
			this.fileName = fileName;
			this.sock = sock;
		}

		public void run() {
			try {
				SlaveListen.sendFile(fileName, sock);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public SlaveListen(int port) {
		try {
			this.ListenSocket = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() {
		while (true) {
			Socket sock = null;
			try {

				sock = ListenSocket.accept();
				Message msgIn = Message.receive(sock, null, -1);
				System.out.println("socket in listen"
						+ sock.getRemoteSocketAddress());
				System.out.println("socket in listen"
						+ sock.getLocalSocketAddress());
				System.out.println("msgIn in listen" + msgIn.getType());
				if (msgIn.getType() == MSG_TYPE.FILE_DOWNLOAD) {
					String receiveFileName = (String) msgIn.getContent();

					receiveFile(receiveFileName + Constants.REFUCE_FILE_SUFFIX,
							sock);

					System.out.println("receiveFilename in slaveListen"
							+ receiveFileName);
					int jobId = Integer.parseInt(receiveFileName.substring(
							Constants.FS_LOCATION.length(),
							receiveFileName.indexOf("_")));
					System.out.println("JobID in slavelisten" + jobId);
					SlaveCompute.fileLeftMap.put(jobId,
							SlaveCompute.fileLeftMap.get(jobId) - 1);
					if (SlaveCompute.fileLeftMap.get(jobId) == 0) {
						SlaveCompute.waitingThreadMap.get(jobId).start();
						SlaveCompute.waitingThreadMap.remove(jobId);
						SlaveCompute.fileLeftMap.remove(jobId);
					}
				} else if (msgIn.getType() == MSG_TYPE.NOTIFY_PORT) {
					System.out.println("Slave listen port open!");
				} else if (msgIn.getType() == MSG_TYPE.GET_FILE) {
					System.out
							.println("Slave now is requested to send files to user");

					new SlaveSendFile(sock, (String) msgIn.getContent())
							.start();

				} else {
					System.out.println("type is not FILE_DOWNLOAD.");
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/**
	 * Receive a file split from the remote
	 * 
	 * @param content
	 * @throws IOException
	 */
	public static void receiveFile(String fileName, Socket sock)
			throws IOException {
		DataInputStream sockData = new DataInputStream(new BufferedInputStream(
				sock.getInputStream()));
		DataOutputStream file = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(fileName)));
		byte[] buf = new byte[Constants.BufferSize];

		int readNum;
		while ((readNum = sockData.read(buf)) != -1) {
			file.write(buf, 0, readNum);
		}
		file.close();
	}

	/**
	 * send files between slaves
	 * 
	 * @param fileName
	 * @param socket
	 * @throws IOException
	 */
	public static void sendFile(String fileName, Socket socket)
			throws IOException {
		DataInputStream file = new DataInputStream(new BufferedInputStream(
				new FileInputStream(fileName)));
		DataOutputStream sockdata = new DataOutputStream(
				socket.getOutputStream());
		byte[] buf = new byte[Constants.BufferSize];
		int read_num;
		while ((read_num = file.read(buf)) != -1) {
			sockdata.write(buf, 0, read_num);
		}
		sockdata.flush();
		file.close();
		socket.close();
	}
}
