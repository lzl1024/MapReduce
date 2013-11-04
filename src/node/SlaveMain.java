package node;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import socket.Message;
import util.Constants;

/**
 * The command line used by slave
 */
public class SlaveMain {
	public static void main(String[] args) {
		// fill up the constants
		try {
			new Constants(args[0]);
		} catch (IOException e) {
			System.out.println("configure file missing!");
			System.exit(1);
		}

		Socket slaveSock = null;
		try {
			slaveSock = new Socket(Constants.MasterIp,
					Constants.MainRoutingPort);
		} catch (Exception e) {
			System.out.println("Fail to connect to the server");
			System.exit(1);
		}

		// clean up file system
		File dir = new File(Constants.FS_LOCATION);
		for (File file : dir.listFiles()) {
			file.delete();
		}

		new SlaveCompute(slaveSock).start();
		new SlaveListen(MasterMain.curPort).start();
		String cmdInput = "";
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		// usage
		System.out.println("Use: 'quit' to quit a slave");
		// slave command Line
		while (!cmdInput.equals("quit")) {
			System.out.print("cmd% ");
			try {
				cmdInput = in.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// send quit message to master
		try {
			Socket quitSock = new Socket(Constants.MasterIp,
					Constants.SlaveActivePort);
			Message msg = new Message(Message.MSG_TYPE.SLAVE_QUIT, null);
			quitSock.setSoTimeout(Constants.RegularTimout);
			msg.send(quitSock, null, -1);
			if (Message.receive(quitSock, null, -1).getType() != Message.MSG_TYPE.SLAVE_QUIT) {
				throw new Exception();
			}

			quitSock.close();
			if (!slaveSock.isClosed()) {
				slaveSock.close();
			}
		} catch (Exception e) {
			System.out.println("Salve Quit Problem");
		}
		
		System.exit(0);
	}
}
