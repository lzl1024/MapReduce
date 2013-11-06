package dfs;

import java.io.BufferedReader;
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
    public static ConcurrentHashMap<String, ArrayList<SocketAddress>> splitLayout = new ConcurrentHashMap<String, ArrayList<SocketAddress>>();

    /**
     * split the file according to its file name and replication factor
     * 
     * @param fileName
     * @param replNum
     * @param jobID
     * @return file split names
     * @throws IOException
     */
    private static ArrayList<String> splitFile(String fileName, int jobID)
            throws IOException {
        ArrayList<String> splitNames = new ArrayList<String>();
        int i = 0;
        String splitName;
        long currentSize = 0;
        BufferedReader reader = new BufferedReader(new FileReader(
                Constants.FS_LOCATION + fileName));
        String line = null;
        PrintWriter pw = null;

        while ((line = reader.readLine()) != null) {
            // new split need to be add
            if (i == 0 || currentSize + line.length() > Constants.ChunkSize) {
                if (i != 0) {
                    pw.close();
                }
                i++;
                splitName = Constants.FS_LOCATION + jobID + "_" + fileName
                        + "_" + i;
                splitNames.add(splitName);
                pw = new PrintWriter(new FileWriter(splitName));
                currentSize = 0;
            }

            pw.println(line);
            currentSize += line.length();
        }
        reader.close();

        // update MapperNum
        Constants.IdealMapperNum = i;

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
            ArrayList<Socket> freeMappers, String fileName, int jobID)
            throws Exception {
        // split the file
        ArrayList<String> fileSplits = splitFile(fileName, jobID);
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
        // not enough or failed
        mapperPointer = 0;
        for (String fileSplit : fileSplits) {
            Message downloadREQ = new Message(Message.MSG_TYPE.FILE_SPLIT_REQ,
                    fileSplit);
            ArrayList<SocketAddress> splitSock = new ArrayList<SocketAddress>();

            // dispatch replicas
            for (int i = 0; i < Constants.ReplFac; i++) {
                try {
                    downloadREQ.send(freeMappers.get(mapperPointer), null, -1);
                    if (Message.receive(freeMappers.get(mapperPointer), null,
                            -1).getType() != Message.MSG_TYPE.FILE_SPLIT_ACK) {
                        throw new Exception("Host Fail");
                    }

                    // download success, add record that which mapper get split
                    SocketAddress key = freeMappers.get(mapperPointer)
                            .getRemoteSocketAddress();
                    if (splitLayout.containsKey(fileSplit)) {
                        splitLayout.get(fileSplit).add(key);
                    } else {
                        ArrayList<SocketAddress> tmp = new ArrayList<SocketAddress>();
                        tmp.add(key);
                        splitLayout.put(fileSplit, tmp);
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
                    if (freeMappers.size() < Constants.ReplFac) {
                        throw new Exception(
                                "Mapper not enough, the Whole Job Fails");
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
        System.out.println(splitFile("src/fs/harrypotter.txt", 1));

    }

}
