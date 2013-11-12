package io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import util.Constants;

/**
 * 
 * Write record class of the mapreduce and dfs
 *
 */
public class Context {

    private ArrayList<PrintWriter> writerList = new ArrayList<PrintWriter>();
    private int size;
    private boolean close;

    public Context(int size, String splitName) {
        for (int i = 1; i <= size; i++) {
            try {
                writerList.add(new PrintWriter(new FileWriter(splitName + "_"
                        + i), true));
            } catch (IOException e) {
                System.out.println("Internal Error");
                e.printStackTrace();
            }
        }
        this.size = size;
        close = false;
    }

    public void write(Writable<?> key, Writable<?> value) {
        // get proper writer
        PrintWriter out = writerList.get(Math.abs(key.hashCode()) % size);
        out.println(key.get() + " " + Constants.divisor + " " + value.get());
    }

    public void close() {
        for (PrintWriter out : writerList) {
            out.close();
        }
        close = true;
    }

    public boolean isClose() {
        return close;
    }
}
