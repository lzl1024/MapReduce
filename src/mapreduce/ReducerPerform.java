package mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;

import socket.ReducerAckMsg;

public class ReducerPerform extends Thread {

    private String ReducerClass;
    private ArrayList<String> fileNames;

    public ReducerPerform(ReducerAckMsg reducerAck) {
        this.ReducerClass = reducerAck.getReducerClass();
        this.fileNames = reducerAck.getfileNames();
    }

    public void run() {
        // get the file Names
        BufferedReader[] readerList = new BufferedReader[fileNames.size()];
        for (int i = 0; i < fileNames.size(); i++) {
            try {
                readerList[i] = new BufferedReader(new FileReader(
                        fileNames.get(i)));
            } catch (FileNotFoundException e) {
                System.out.println("Internal Error : Reducer file not found!"
                        + fileNames.get(i));
                e.printStackTrace();
            }
        }

        // merge the record in fileNames and perform reduce function 
        // transmit file to master
    }
}
