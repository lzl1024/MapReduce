package node;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.Socket;

import util.Constants;

public class SlaveSendFile extends Thread{

	private String fileName;
	private Socket sock;
	public SlaveSendFile(Socket sock, String fileName) {
		this.fileName = fileName;
		this.sock = sock;
	}
	public void run() {
		try {
			DataInputStream file = new DataInputStream(new BufferedInputStream(
					new FileInputStream(fileName)));
			System.out.println("filename in SlaveSendFile "+ fileName);
			DataOutputStream sockdata = new DataOutputStream(sock.getOutputStream());
			byte[] buf = new byte[Constants.BufferSize];
			int read_num;
			while((read_num = file.read(buf)) != -1) {
				sockdata.write(buf, 0, read_num);
			}
			sockdata.close();
			file.close();
			sock.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
