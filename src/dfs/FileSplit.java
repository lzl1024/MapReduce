package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import node.MasterMain;
import socket.Message;
import util.Constants;

/**
 * 
 * Class to split the file in the distributed file system
 * 
 */
public class FileSplit {
	/**
	 * The layout that which host get which split. Master can allocate work
	 * according to this layout
	 */
	public static ConcurrentHashMap<SocketAddress, ArrayList<String>> splitLayout = new ConcurrentHashMap<SocketAddress, ArrayList<String>>();

	/**
	 * split the file according to its file name and replication factor
	 * 
	 * @param fileName
	 * @param replNum
	 * @param jobID
	 * @return file split names
	 * @throws IOException
	 */
	private static String[] splitFile(String fileName, int replNum, int jobID)
			throws IOException {
		File file = new File(fileName);
		PrintWriter[] pwList = new PrintWriter[replNum];
		String[] splitNames = new String[replNum];

		if (!file.exists()) {
			throw new IOException("File cannot found");
		}

		// get split names
		for (int i = 1; i <= replNum; i++) {
			splitNames[i - 1] = jobID + "_" + fileName + "_" + i;
			pwList[i - 1] = new PrintWriter(new FileWriter(splitNames[i - 1]),
					true);
		}

		// split file
		int i = 0;
		BufferedReader read = new BufferedReader(new FileReader(fileName));
		String record;
		while ((record = read.readLine()) != null) {
			pwList[i].println(record);
			i = (i + 1) % replNum;
		}

		// close files
		read.close();
		for (PrintWriter pw : pwList) {
			pw.close();
		}

		return splitNames;
	}

	/**
	 * Split a file into pieces and dispatch them to different available hosts
	 * 
	 * @param freeMappers
	 * @param fileName
	 * @param replFac
	 * @param mapperNum
	 * @return the inverse layout that key is filename and value is socket list
	 * @throws Exception
	 * @throws IOException
	 */
	public static HashMap<String, ArrayList<SocketAddress>> fileDispatch(
			ArrayList<Socket> freeMappers, String fileName, int replFac,
			int mapperNum, int jobID) throws Exception {
		// split the file
		String[] fileSplits = splitFile(fileName, mapperNum, jobID);
		ArrayList<SocketAddress> failedMappers = new ArrayList<SocketAddress>();
		HashMap<String, ArrayList<SocketAddress>> returnLayout = new HashMap<String, ArrayList<SocketAddress>>();

		int mapperPointer = 0;
		// set time out
		while (mapperPointer < freeMappers.size()) {
			try {
				freeMappers.get(mapperPointer).setSoTimeout(
						Constants.FileDownloadTimeout);
				mapperPointer++;
			} catch (Exception e) {
				// add to fail mapper list if timeout
				failedMappers.add(freeMappers.get(mapperPointer)
						.getRemoteSocketAddress());
				freeMappers.remove(mapperPointer);
			}
		}

		// send file splits to different host use round robin when free mapper
		// not
		// enough or failed
		mapperPointer = 0;
		for (String fileSplit : fileSplits) {
			Message downloadREQ = new Message(Message.MSG_TYPE.FILE_SPLIT_REQ,
					fileSplit);
			ArrayList<SocketAddress> splitSock = new ArrayList<SocketAddress>();

			// dispatch replicas
			for (int i = 0; i < replFac; i++) {
				try {
					downloadREQ.send(freeMappers.get(mapperPointer), null, -1);
					if (Message.receive(freeMappers.get(mapperPointer), null,
							-1).getType() != Message.MSG_TYPE.FILE_SPLIT_ACK) {
						throw new Exception("Host Fail");
					}

					// download success, add record that which mapper get split
					SocketAddress key = freeMappers.get(mapperPointer)
							.getRemoteSocketAddress();
					if (splitLayout.containsKey(key)) {
						splitLayout.get(key).add(fileSplit);
					} else {
						ArrayList<String> tmp = new ArrayList<String>();
						tmp.add(fileSplit);
						splitLayout.put(key, tmp);
					}

					// add to entry
					splitSock.add(key);

					mapperPointer = (mapperPointer + 1) % freeMappers.size();
				} catch (Exception e) {
					// add to fail mapper list if timeout
					failedMappers.add(freeMappers.get(mapperPointer)
							.getRemoteSocketAddress());
					freeMappers.remove(mapperPointer);

					// too many free mappers fail!
					if (freeMappers.size() < replFac) {
						throw new Exception("Mapper not enough, the Whole Job Fails");
					}
				}
			}

			returnLayout.put(fileSplit, splitSock);
		}

		mapperPointer = 0;
		// restore time out
		while (mapperPointer < freeMappers.size()) {
			try {
				freeMappers.get(mapperPointer).setSoTimeout(
						Constants.RegularTimout);
				mapperPointer++;
			} catch (Exception e) {
				// add to fail mapper list if timeout
				failedMappers.add(freeMappers.get(mapperPointer)
						.getRemoteSocketAddress());
				freeMappers.remove(mapperPointer);
			}
		}

		MasterMain.handleLeave(failedMappers);

		return returnLayout;
	}

	// for test
	public static void main(String[] args) throws IOException {
		new Constants(args[0]);
		splitFile("src/fs/story1.txt", 2, 1);
	}

}
