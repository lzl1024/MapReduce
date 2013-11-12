package examples;

import util.Constants;
import dfs.DFSApi;

/**
 * 
 * Example 3: Send two files on the dfs and read a line.
 * To do the experiment, we can disconnect a slave after the file has
 * already be put onto the dfs and see whether the file will be
 * send to those slaves who does not have the file splits 
 *
 */
public class Exp3DFSfail {
    public static void main(String[] args) throws Exception {
        new Constants(args[0]);
        DFSApi.put(args[1]);
        DFSApi.put("harrypotter.txt");
        System.out.println(DFSApi.readRecord(10L, args[1]));
    }
}
