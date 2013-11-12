package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Constants {

    /**
     * Uninitialized parameters
     */
    // the port that slave actively send msg to master
    public static int SlaveActivePort;
    // the port the master running on
    public static int MainRoutingPort;
    // timeout time when client download a file
    public static int FileDownloadTimeout;
    // the time out value for regular communication
    public static int RegularTimout;
    // the ip address of the master
    public static String MasterIp;
    // the number of mapper
    public static int IdealMapperNum;
    // the number of reducer
    public static int IdealReducerNum;
    // the ideal number of mapper job in one slave
    public static int IdealMapperJobs;
    // the ideal number of reducer job in one slave
    public static int IdealReducerJobs;
    // the replication factor
    public static int ReplFac;
    // the start port
    public static int startPort;
    // the end port
    public static int endPort;
    // chunk size
    public static long ChunkSize;

    /**
     * Initialized parameters
     */
    public static String FS_LOCATION = "fs/";
    public static String HTTP_PREFIX = "http://";
    public static int KEEP_ALIVE_INT = 5000;
    public static int Random_Base = 1000;
    public static String Class_PREFIX = "examples.";
    public static final int BufferSize = 8196;
    public static String REDUCE_FILE_SUFFIX = "_REDUCE";
    public static String divisor = "!#$";

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
            MainRoutingPort = Integer.parseInt(args.get("MainRoutingPort"));
            FileDownloadTimeout = Integer.parseInt(args
                    .get("FileDownloadTimeout"));
            if (args.get("IdealMapperNum") != null) {
                IdealMapperNum = Integer.parseInt(args.get("IdealMapperNum"));
            }
            if (args.get("ChunkSize") != null) {
                ChunkSize = Long.parseLong(args.get("ChunkSize"));
            }
            IdealReducerNum = Integer.parseInt(args.get("IdealReducerNum"));
            SlaveActivePort = Integer.parseInt(args.get("SlaveActivePort"));
            RegularTimout = Integer.parseInt(args.get("RegularTimout"));
            IdealReducerJobs = Integer.parseInt(args.get("IdealReducerJobs"));
            IdealMapperJobs = Integer.parseInt(args.get("IdealMapperJobs"));
            ReplFac = Integer.parseInt(args.get("ReplFac"));
            startPort = Integer.parseInt(args.get("startPort"));
            endPort = Integer.parseInt(args.get("endPort"));

            MasterIp = args.get("MasterIp");
            if (MasterIp == null) {
                throw new Exception();
            }

        } catch (Exception e) {
            System.out.println("Parameter missing or in wrong format");
        }
    }
}
