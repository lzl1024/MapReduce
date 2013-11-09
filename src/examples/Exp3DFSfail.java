package examples;

import util.Constants;
import dfs.DFSApi;

public class Exp3DFSfail {
    public static void main(String[] args) throws Exception {
        new Constants(args[0]);
        DFSApi.put(args[1]);
        DFSApi.put("harrypotter.txt");
        System.out.println(DFSApi.readRecord(10L, args[1]));
    }
}
