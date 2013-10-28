package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Constants {

	/**
	 * Uninitialized parameters
	 */
	// the port for dfs to dispatch files
	public static int FiledispatchPort;
	// timeout time when client download a file
	public static int FileDownloadTimeout;
	// the time out value for regular communication
	public static int RegularTimout;
	// the port the master running on
	public static int MainRoutingPort;
	// the ip address of the master
	public static String MasterIp;
	// the number of mapper
	public static int IdealMapperNum;
	// the number of reducer
	public static int IdealReducerNum;
	// the port that slave actively send msg to master
	public static int SlaveActivePort;

	/**
	 * Initialized parameters
	 */
	public static String FS_LOCATION = "fs/";
	public static String HTTP_PREFIX = "http://";
	public static int KEEP_ALIVE_INT = 5000;

	/**
	 * Parse the configuration file
	 * 
	 * @param ConfigFile
	 * @throws IOException
	 */
	public Constants(String ConfigFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(ConfigFile));
		HashMap<String, String> args = new HashMap<String, String>();
		String line;

		while ((line = reader.readLine()) != null) {
			int mark = line.indexOf("=");
			if (mark != -1) {
				args.put(line.substring(0, mark).trim(),
						line.substring(mark + 1).trim());
			}
		}

		reader.close();
		putArgs(args);
	}

	/**
	 * put arguments into accordingly variable
	 * 
	 * @param args
	 * @throws IOException
	 */
	private void putArgs(HashMap<String, String> args) {

		try {
			FiledispatchPort = Integer.parseInt(args.get("FiledispatchPort"));
			MainRoutingPort = Integer.parseInt(args.get("MainRoutingPort"));
			FileDownloadTimeout = Integer.parseInt(args
					.get("FileDownloadTimeout"));
			IdealMapperNum = Integer.parseInt(args.get("IdealMapperNum"));
			IdealReducerNum = Integer.parseInt(args.get("IdealReducerNum"));
			SlaveActivePort = Integer.parseInt(args.get("SlaveActivePort"));
			RegularTimout = Integer.parseInt(args.get("RegularTimout"));

			MasterIp = args.get("MasterIp");
			if (MasterIp == null) {
				throw new Exception();
			}

		} catch (Exception e) {
			System.out.println("Parameter missing or in wrong format");
		}
	}
}
