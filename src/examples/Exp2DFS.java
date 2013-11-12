package examples;

import util.Constants;
import dfs.DFSApi;

/**
 * 
 * Example 2: send a file onto dfs, and download it again with another name
 * then read the first line of the file and finally delete the file on dfs
 *
 */
public class Exp2DFS {
    public static void main(String[] args) throws Exception {
        new Constants(args[0]);
        DFSApi.put(args[1]);
        DFSApi.get(args[1], args[2], false);
        System.out.println(DFSApi.readRecord(1L, args[1]));
        DFSApi.delete(args[1]);
    }
}
