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
import socket.Message.MSG_TYPE;
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
    public static ConcurrentHashMap<String, ArrayList<SocketAddress>> splitLayout = new ConcurrentHashMap<String, ArrayList<SocketAddress>>();

    /**
     * The layout of records range, key: fileName, value: record number of each
     * split
     */
    public static ConcurrentHashMap<String, ArrayList<Long>> recordLayout = new ConcurrentHashMap<String, ArrayList<Long>>();

    /**
     * split the file according to its file name and replication factor
     * 
     * @param fileName
     * @param replNum
     * @param jobID
     * @param recordEnd
     * @param recordBegin
     * @return file split names
     * @throws IOException
     */
    private static ArrayList<String> splitFile(String fileName, int jobID,
            Long recordBegin, Long recordEnd) throws IOException {
        ArrayList<String> splitNames = new ArrayList<String>();
        int i = 0;
        String splitName;
        long currentSize = 0;
        BufferedReader reader = new BufferedReader(new FileReader(
                Constants.FS_LOCATION + fileName));
        String line = null;
        PrintWriter pw = null;

        // record layout
        long records = 1L;
        ArrayList<Long> lineLayout = new ArrayList<Long>();

        // ignore first records
        while (recordBegin != null && recordBegin > records
                && (line = reader.readLine()) != null) {
            records++;
        }

        // ignore back records if needed
        while ((line = reader.readLine()) != null
                && (recordEnd == null || records <= recordEnd)) {
            // new split need to be add
            if (i == 0 || currentSize + line.length() > Constants.ChunkSize) {
                if (i != 0) {
                    lineLayout.add(records);
                    pw.close();
                }
                i++;

                // jobID < 0 stand alone
                if (jobID >= 0) {
                    splitName = Constants.FS_LOCATION + jobID + "_" + fileName
                            + "_" + i;
                } else {
                    splitName = Constants.FS_LOCATION + fileName + "_" + i;
                }

                splitNames.add(splitName);
                pw = new PrintWriter(new FileWriter(splitName));
                currentSize = 0;
            }

            pw.println(line);
            currentSize += line.length();
            records++;
        }
        reader.close();
        pw.close();

        // update layout
        lineLayout.add(records - 1);
        recordLayout.put(fileName, lineLayout);

        // update MapperNum
        Constants.IdealMapperNum = i;

        // delete the original file
        new File(Constants.FS_LOCATION + fileName).delete();

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
            ArrayList<SocketAddress> freeMappers, String fileName, int jobID,
            Long recordBegin, Long recordEnd) throws Exception {
        // split the file
        ArrayList<String> fileSplits = splitFile(fileName, jobID, recordBegin,
                recordEnd);

        ArrayList<SocketAddress> failedMappers = new ArrayList<SocketAddress>();
        HashMap<String, ArrayList<SocketAddress>> returnLayout = new HashMap<String, ArrayList<SocketAddress>>();

        int mapperPointer = 0;

        // send file splits to different host use round robin when free mapper
        // not enough or failed
        mapperPointer = 0;
        for (String fileSplit : fileSplits) {
            Message downloadREQ = new Message(MSG_TYPE.FILE_SPLIT_REQ,
                    fileSplit);
            ArrayList<SocketAddress> splitSock = new ArrayList<SocketAddress>();

            // dispatch replicas
            for (int i = 0; i < Constants.ReplFac; i++) {
            	SocketAddress key = null;
                try {
                    key = MasterMain.listenToActive
                            .get(freeMappers.get(mapperPointer));
                    if(MasterMain.failedActiveMap.containsKey(freeMappers.get(mapperPointer))) {
                    	key = MasterMain.failedActiveMap.get(freeMappers.get(mapperPointer));
                    	throw new Exception();
                    }
                    // download success, add record that which mapper get split
      
                    if (splitLayout.containsKey(fileSplit)) {
                        splitLayout.get(fileSplit).add(key);
                    } else {
                        ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                        tmp.add(key);
                        splitLayout.put(fileSplit, tmp);
                    }
                    
                    Socket fileSocket = new Socket();

                    fileSocket.connect(freeMappers.get(mapperPointer));
                    downloadREQ.send(fileSocket, null, -1);
                    
                    // add the splits number in slave
                    MasterMain.slavePool.get(key).setSplits(
                            MasterMain.slavePool.get(key).getSplits() + 1);

                    FileTransmitServer.sendFile(fileSplit, fileSocket);
                    if (!fileSocket.isClosed()) {
                        fileSocket.close();
                    }

                    // add to entry
                    splitSock.add(key);

                    mapperPointer = (mapperPointer + 1) % freeMappers.size();
                } catch (Exception e) {
                    // add to fail mapper list if timeout
                    failedMappers.add(key);
                    freeMappers.remove(mapperPointer);
                    mapperPointer = mapperPointer % freeMappers.size();

                    // too many free mappers fail!
                    if (freeMappers.size() < Constants.ReplFac) {
                        throw new Exception(
                                "Mapper not enough, the Whole Job Fails");
                    }
                }
            }

            returnLayout.put(fileSplit, splitSock);
        }

        MasterMain.handleLeave(failedMappers);

        // delete file splits in master
        for (String file : fileSplits) {
            new File(file).delete();
        }

        return returnLayout;
    }

    // for test
    public static void main(String[] args) throws IOException {
        new Constants(args[0]);
        System.out.println(splitFile("src/fs/harrypotter.txt", 1, null, null));

    }

}
