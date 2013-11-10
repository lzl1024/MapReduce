package mapreduce;

import io.Context;
import io.Writable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import node.SlaveCompute;
import socket.CompleteMsg;
import socket.Message;
import socket.Message.MSG_TYPE;
import socket.ReducerAckMsg;
import util.Constants;

public class ReducerPerform extends Thread {

    private String ReducerClass;
    private ArrayList<String> fileNames;
    private Integer jobID;
    private Integer index;
    private Class<?> reduceKey;
    private Class<?> reduceValue;

    public ReducerPerform(ReducerAckMsg reducerAck) {
        System.out.println("ReducerAckMSg" + reducerAck.toString());
        this.ReducerClass = reducerAck.getReducerClass();
        this.fileNames = reducerAck.getfileNames();
        this.jobID = reducerAck.getJobID();
        this.index = reducerAck.getIndex();
        this.reduceKey = reducerAck.getReduceKey();
        this.reduceValue = reducerAck.getReduceValue();
    }

    /**
     * 
     * Use this class to store key value pairs and sort
     * 
     */
    public class KVPair implements Comparable<KVPair> {
        public Writable<?> key;
        public Writable<?> value;
        public int index; // come from which file

        public KVPair(Writable<?> key, Writable<?> value, int index) {
            this.key = key;
            this.value = value;
            this.index = index;
        }

        @Override
        public int compareTo(KVPair kvp1) {
            if (this.key.hashCode() < kvp1.key.hashCode()) {
                return 1;
            } else if (this.key.hashCode() > kvp1.key.hashCode()) {
                return -1;
            }
            return 0;
        }
    }

    public void run() {
        // get the file Names and sort its record

        for (int i = 0; i < fileNames.size(); i++) {
            try {
                FileReader fd = new FileReader(fileNames.get(i)
                        + Constants.REFUCE_FILE_SUFFIX);
                BufferedReader reader = new BufferedReader(fd);
                ArrayList<KVPair> records = new ArrayList<KVPair>();
                String line;

                // reflect and add key-pair value into records
                while ((line = reader.readLine()) != null) {
                    records.add(readReord(line, i));
                }

                // sort based on key
                Collections.sort(records);
                reader.close();
                fd.close();
                // delete oldfile
                new File(fileNames.get(i)).delete();

                // write records into a file, reuse file names
                FileWriter fw = new FileWriter(fileNames.get(i));
                PrintWriter pw = new PrintWriter(fw);
                for (KVPair kvp : records) {
                    pw.println(kvp.key.get() + " " + Constants.divisor + " "
                            + kvp.value.get());
                }

                pw.close();
                fw.close();
            } catch (Exception e) {
                System.out.println("Internal Error : Reducer merge file!"
                        + fileNames.get(i));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        // external merge the record in files and perform reduce function
        String reduceFile = null;
        try {
            reduceFile = mergeAndPerform(fileNames);
        } catch (Exception e) {
            System.out.println("Internal Error: Reducer External sort!");
            e.printStackTrace();
            System.exit(-1);
        }

        // send success message back to master
        try {
            Socket tmpSock = new Socket(Constants.MasterIp,
                    Constants.SlaveActivePort);

            new Message(MSG_TYPE.REDUCER_COMPLETE, new CompleteMsg(reduceFile
                    + "_1", SlaveCompute.sockToMaster.getLocalSocketAddress(),
                    this.jobID)).send(tmpSock, null, -1);
            Message.receive(tmpSock, null, -1);
            tmpSock.close();
        } catch (Exception e) {
            System.out.println("Failed to connect with Master");
            System.exit(-1);
        }

        // delete all split files
        for (String file : fileNames) {
            new File(file).delete();
        }

    }

    /**
     * Merge the records in files and perform reduce function
     * 
     * @param mergeOutFile
     * @return
     * @throws Exception
     */
    private String mergeAndPerform(ArrayList<String> mergeOutFile)
            throws Exception {
        String fileName = Constants.FS_LOCATION + this.jobID.toString() + "##_"
                + this.index;
        Context context = new Context(1, fileName);
        PriorityQueue<KVPair> records = new PriorityQueue<KVPair>();

        // open list of files
        BufferedReader[] readerList = new BufferedReader[mergeOutFile.size()];
        for (int i = 0; i < mergeOutFile.size(); i++) {
            readerList[i] = new BufferedReader(new FileReader(fileNames.get(i)));
            String line;
            if ((line = readerList[i].readLine()) != null) {
                records.add(readReord(line, i));
            }
        }

        // external sort
        ArrayList<Writable<?>> sameRecord = new ArrayList<Writable<?>>();
        KVPair last = null;

        while (records.size() > 0) {
            KVPair tmp = records.poll();

            if (last != null && last.key.hashCode() != tmp.key.hashCode()) {
                perform(last.key, sameRecord, context);
                sameRecord = new ArrayList<Writable<?>>();
            }

            sameRecord.add(tmp.value);
            String line;
            if ((line = readerList[tmp.index].readLine()) != null) {
                records.add(readReord(line, tmp.index));
            }
            last = tmp;
        }
        // last part of external sort
        if (sameRecord.size() != 0) {
            perform(last.key, sameRecord, context);
        }

        if (!context.isClose()) {
            context.close();
        }
        for (BufferedReader reader : readerList) {
            reader.close();
        }

        return fileName;
    }

    /**
     * Perform reduce function
     * 
     * @param key
     * @param sameRecord
     * @param context
     * @throws Exception
     */
    private void perform(Writable<?> key, ArrayList<Writable<?>> sameRecord,
            Context context) throws Exception {
        // reflect the reduce class
        Class<?> obj = Class
                .forName(Constants.Class_PREFIX + this.ReducerClass);
        Constructor<?> objConstructor = obj.getConstructor();
        Reducer reducer = (Reducer) objConstructor.newInstance();
        reducer.reduce(key, sameRecord, context);
    }

    /**
     * Read a record from a line
     * 
     * @param line
     * @return
     * @throws Exception
     */
    private KVPair readReord(String line, int index) throws Exception {
        int pos = line.indexOf(Constants.divisor);
        Writable<?> key = (Writable<?>) reduceKey.getConstructor()
                .newInstance();
        key.parse(line.substring(0, pos).trim());
        Writable<?> value = (Writable<?>) reduceValue.getConstructor()
                .newInstance();

        value.parse(line.substring(pos + Constants.divisor.length()).trim());
        return new KVPair(key, value, index);
    }
}
