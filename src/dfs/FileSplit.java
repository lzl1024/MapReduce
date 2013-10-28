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
import java.util.concurrent.ConcurrentHashMap;

import socket.Message;
import util.Constants;

/**
 * 
 * Class to split the file in the distributed file system
 *
 */
public class FileSplit {
	/**
	 * The layout that which host get which split.
	 * Master can allocate work according to this layout
	 */
	public static ConcurrentHashMap<SocketAddress, ArrayList<String>> splitLayout =
			new ConcurrentHashMap<SocketAddress, ArrayList<String>>();

    /**
     * split the file according to its file name and replication factor
     * @param fileName
     * @param replNum
     * @return file split names
     * @throws IOException
     */
    private static String[] splitFile(String fileName, int replNum) throws IOException{
        File file = new File(fileName);
        PrintWriter[] pwList = new PrintWriter[replNum];
        String[] splitNames = new String[replNum];
        
        if (!file.exists()) {
            throw new IOException("File cannot found");
        }
        
        //get split names
        for (int i=1; i<=replNum; i++) {
            splitNames[i-1] = fileName+i;
            pwList[i-1] = new PrintWriter(new FileWriter(splitNames[i-1]), true);
        }
        
        //split file
        int i = 0;
        BufferedReader read = new BufferedReader(new FileReader(fileName));
        String record;
        while((record = read.readLine()) != null){
            pwList[i].println(record);
            i = (i + 1) % replNum;
        }
        
        //close files
        read.close();
        for (PrintWriter pw: pwList) {
            pw.close();
        }

        return splitNames;
    }


    /**
     * Split a file into pieces and dispatch them to different available hosts
     * @param freeMappers
     * @param fileName
     * @param replFac
     * @param mapperNum
     * @return failed mappers should be handled in other class
     * @throws Exception 
     * @throws IOException
     */
    public static ArrayList<Socket> fileDispatch(ArrayList<Socket> freeMappers, 
    		String fileName, int replFac, int mapperNum) throws Exception {
    	//split the file
    	String[] fileSplits = splitFile(fileName, mapperNum);
    	ArrayList<Socket> failedMappers = new ArrayList<Socket>();
    	
    	int mapperPointer = 0;
    	//set time out
    	while(mapperPointer < freeMappers.size()) {
    		try{
    			freeMappers.get(mapperPointer).setSoTimeout(Constants.FileDownloadTimeout);
    			mapperPointer++;
    		} catch (Exception e) {
    			//add to fail mapper list if timeout
    			failedMappers.add(freeMappers.get(mapperPointer));
    			freeMappers.remove(mapperPointer);
    		}
    	}
    	
    	//send file splits to different host use round robin when free mapper not 
    	//enough or failed
    	mapperPointer = 0;
    	for(String fileSplit : fileSplits) {
    		Message downloadREQ = new Message(Message.MSG_TYPE.FILE_SPLIT_REQ, fileSplit);
    		
    		//dispatch replicas
    		for (int i = 0; i < replFac; i++) {
    			try{
        			downloadREQ.send(freeMappers.get(mapperPointer), null, -1);
        			if (Message.receive(freeMappers.get(mapperPointer), null, -1).getType()
        					!= Message.MSG_TYPE.FILE_SPLIT_ACK) {
        				throw new Exception("Host Fail");
        			}
        			
        			//download success, add record that which mapper get split
        			SocketAddress key = freeMappers.get(mapperPointer).getRemoteSocketAddress();
        			if (splitLayout.containsKey(key)) {
        				splitLayout.get(key).add(fileSplit);
        			} else {
        				ArrayList<String> tmp = new ArrayList<String>();
        				tmp.add(fileSplit);
        				splitLayout.put(key, tmp);
        			}
        			mapperPointer = (mapperPointer + 1) % freeMappers.size();
        		} catch (Exception e) {
        			//add to fail mapper list if timeout
        			failedMappers.add(freeMappers.get(mapperPointer));
        			freeMappers.remove(mapperPointer);
        			
        			//too many free mappers fail!
        			if (freeMappers.size() < replFac) {
        				throw new Exception("The Whole Job Fails");
        			}		
        		}
    		}
    	}
    	
    	return failedMappers;
    }

    //for test
    public static void main(String[] args) throws IOException{
    	new Constants(args[0]);
        splitFile("src/fs/story1.txt",2);
    }

}
