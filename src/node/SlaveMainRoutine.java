package node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import util.Constants;

/**
 * The command line used by slave
 */
public class SlaveMainRoutine {
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
			slaveSock = new Socket(Constants.MasterIp, Constants.MainRoutingPort);
		} catch (Exception e) {
			System.out.println("Fail to connect to the server");
			System.exit(1);
		}

		new SlaveCompute(slaveSock).start();
		
		String cmdInput = "";
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        //usage
        System.out.println("Use: 'quit' to quit a slave");
        //slave command Line
        while (!cmdInput.equals("quit")) {
            System.out.print("cmd% ");
            try {
                cmdInput = in.readLine();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
	}
}
